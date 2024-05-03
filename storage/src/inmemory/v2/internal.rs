#[cfg(loom)]
use loom::sync::{atomic, Arc};
#[cfg(not(loom))]
use std::sync::{atomic, Arc};

const BLOCK_SIZE: usize = 512;

pub struct RelationList {
    head: atomic::AtomicPtr<RelationBlock<BLOCK_SIZE>>,
    tail: atomic::AtomicPtr<RelationBlock<BLOCK_SIZE>>,
    locked: atomic::AtomicBool,
    active_handles: atomic::AtomicUsize,
    newest_row_id: atomic::AtomicU64,
}

unsafe impl Send for RelationList {}
unsafe impl Sync for RelationList {}

pub struct RelationBlock<const N: usize> {
    next: atomic::AtomicPtr<Self>,
    block_idx: usize,
    position: atomic::AtomicUsize,
    slots: Vec<RelationSlot>,
}

impl<const N: usize> core::fmt::Debug for RelationBlock<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationBlock").finish()
    }
}

pub struct RelationSlot {
    /// Indicate the state of this slot
    /// Meaning of Bits:
    /// 00.000: Empty slot, nothing written yet
    /// 0..001: Picked slot, data being written
    /// 0...10: Used slot, data already written (can be read)
    flags: atomic::AtomicU64,
    pub created: atomic::AtomicU64,
    pub expired: atomic::AtomicU64,
    pub row_id: atomic::AtomicU64,
    pub data: core::cell::UnsafeCell<Vec<crate::Data>>,
}

pub struct RelationListHandle {
    list: Arc<RelationList>,
}

pub struct RelationListExclusiveHandle {
    list: Arc<RelationList>,
}

pub struct RelationListIter<'h> {
    _handle: &'h RelationListHandle,
    current_block: *mut RelationBlock<BLOCK_SIZE>,
}

pub struct RelationBlockIter<'b, const N: usize> {
    block: &'b RelationBlock<N>,
    idx: usize,
}

pub struct HandleRowIter {
    _handle: RelationListHandle,
    block: *mut RelationBlock<BLOCK_SIZE>,
    idx: usize,
}

impl RelationList {
    pub fn new() -> Self {
        let head_block = RelationBlock::alloced();
        let head_ptr = Box::into_raw(Box::new(head_block));

        Self {
            head: atomic::AtomicPtr::new(head_ptr),
            tail: atomic::AtomicPtr::new(head_ptr),
            locked: atomic::AtomicBool::new(false),
            active_handles: atomic::AtomicUsize::new(0),
            newest_row_id: atomic::AtomicU64::new(0),
        }
    }

    pub fn try_get_handle(list: Arc<Self>) -> Option<RelationListHandle> {
        if list.locked.load(atomic::Ordering::SeqCst) {
            return None;
        }

        list.active_handles.fetch_add(1, atomic::Ordering::SeqCst);

        Some(RelationListHandle { list })
    }

    pub fn try_get_exclusive_handle(list: Arc<Self>) -> Option<RelationListExclusiveHandle> {
        match list.locked.compare_exchange(
            false,
            true,
            atomic::Ordering::SeqCst,
            atomic::Ordering::SeqCst,
        ) {
            Ok(_) => {}
            Err(_) => return None,
        };

        let active_handles = list.active_handles.load(atomic::Ordering::SeqCst);
        if active_handles != 0 {
            list.locked.store(false, atomic::Ordering::SeqCst);
            return None;
        }

        Some(RelationListExclusiveHandle { list })
    }
}

impl RelationListHandle {
    pub fn iter<'h>(&'h self) -> RelationListIter<'h> {
        RelationListIter {
            _handle: self,
            current_block: self.list.head.load(atomic::Ordering::SeqCst),
        }
    }

    pub fn into_iter(self) -> HandleRowIter {
        let block = self.list.head.load(atomic::Ordering::SeqCst);

        HandleRowIter {
            _handle: self,
            block,
            idx: 0,
        }
    }

    pub fn insert_row(&self, data: Vec<crate::Data>, created: u64) {
        let mut block_ptr = self.list.tail.load(atomic::Ordering::SeqCst);
        let mut last_block_ptr = block_ptr;

        let (block_ref, slot_idx) = loop {
            if block_ptr.is_null() {
                assert!(!last_block_ptr.is_null());

                let previous_block = unsafe { &*last_block_ptr };

                let mut n_block: RelationBlock<BLOCK_SIZE> = RelationBlock::alloced();
                n_block.block_idx = previous_block.block_idx + 1;

                let n_block_ptr = Box::into_raw(Box::new(n_block));
                match previous_block.next.compare_exchange(
                    core::ptr::null_mut(),
                    n_block_ptr,
                    atomic::Ordering::SeqCst,
                    atomic::Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        block_ptr = n_block_ptr;
                        let _ = self.list.tail.compare_exchange(
                            last_block_ptr,
                            n_block_ptr,
                            atomic::Ordering::SeqCst,
                            atomic::Ordering::SeqCst,
                        );

                        continue;
                    }
                    Err(other_new) => {
                        let _ = unsafe { Box::from_raw(n_block_ptr) };
                        block_ptr = other_new;
                    }
                };
            } else {
                let block_ref = unsafe { &*block_ptr };

                let slot_idx = block_ref.position.fetch_add(1, atomic::Ordering::SeqCst);
                if slot_idx < BLOCK_SIZE {
                    break (block_ref, slot_idx);
                }

                block_ref.position.fetch_sub(1, atomic::Ordering::SeqCst);
                last_block_ptr = block_ptr;

                block_ptr = block_ref.next.load(atomic::Ordering::SeqCst);
            }
        };

        let slot = block_ref.slots.get(slot_idx).expect("");

        slot.flags.store(1, atomic::Ordering::SeqCst);
        slot.created.store(created, atomic::Ordering::SeqCst);
        slot.expired.store(0, atomic::Ordering::SeqCst);
        slot.row_id.store(
            self.list
                .newest_row_id
                .fetch_add(1, atomic::Ordering::SeqCst),
            atomic::Ordering::SeqCst,
        );

        let slot_data = unsafe { &mut *slot.data.get() };
        *slot_data = data;

        slot.flags.store(2, atomic::Ordering::SeqCst);
    }
}

impl RelationListExclusiveHandle {
    pub fn update_rows<F>(&mut self, mut func: F)
    where
        F: FnMut(&mut Vec<crate::Data>),
    {
        let mut block_ptr = self.list.head.load(atomic::Ordering::SeqCst);

        while let Some(block_ptr_nn) = core::ptr::NonNull::new(block_ptr) {
            let block = unsafe { block_ptr_nn.as_ref() };

            for slot in block.slots.iter() {
                if slot.flags.load(atomic::Ordering::SeqCst) != 2 {
                    continue;
                }

                let data = unsafe { &mut *slot.data.get() };
                func(data);
            }

            block_ptr = block.next.load(atomic::Ordering::SeqCst);
        }
    }
}

impl<const N: usize> RelationBlock<N> {
    pub fn alloced() -> Self {
        let slots: Vec<_> = (0..N)
            .map(|_| RelationSlot {
                flags: atomic::AtomicU64::new(0),
                created: atomic::AtomicU64::new(0),
                expired: atomic::AtomicU64::new(0),
                row_id: atomic::AtomicU64::new(0),
                data: core::cell::UnsafeCell::new(Vec::new()),
            })
            .collect();

        Self {
            next: atomic::AtomicPtr::new(core::ptr::null_mut()),
            block_idx: 0,
            position: atomic::AtomicUsize::new(0),
            slots,
        }
    }

    pub fn iter(&self) -> RelationBlockIter<'_, N> {
        RelationBlockIter {
            block: self,
            idx: 0,
        }
    }
}

impl<'h, 'rl> Iterator for RelationListIter<'h> {
    type Item = &'h RelationBlock<BLOCK_SIZE>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_block.is_null() {
            return None;
        }

        // SAFETY:
        // We know this is safe, because:
        // 1. The Pointer is not null, that was checked above
        // 2. we currently never deallocate blocks so the data is not freed
        // 3. we hold a handle, which means that it blocks any mutating operations
        let block = unsafe { &*self.current_block };
        self.current_block = block.next.load(atomic::Ordering::SeqCst);

        Some(block)
    }
}

impl<'b, const N: usize> Iterator for RelationBlockIter<'b, N> {
    type Item = &'b RelationSlot;

    fn next(&mut self) -> Option<Self::Item> {
        while self.idx < self.block.position.load(atomic::Ordering::SeqCst) {
            let slot = self.block.slots.get(self.idx)?;
            self.idx += 1;

            if slot.flags.load(atomic::Ordering::SeqCst) != 2 {
                continue;
            }

            return Some(slot);
        }
        None
    }
}

impl Iterator for HandleRowIter {
    type Item = RelationSlot;

    fn next(&mut self) -> Option<Self::Item> {
        'block_loop: while let Some(ptr) = core::ptr::NonNull::new(self.block) {
            // SAFETY:
            // TODO
            let block = unsafe { ptr.as_ref() };

            loop {
                if self.idx >= block.position.load(atomic::Ordering::SeqCst) {
                    self.idx = 0;
                    self.block = block.next.load(atomic::Ordering::SeqCst);
                    continue 'block_loop;
                }

                let slot = block.slots.get(self.idx)?;
                self.idx += 1;

                let flags = slot.flags.load(atomic::Ordering::SeqCst);
                if flags != 2 {
                    continue;
                }

                let slot_data = unsafe { &*slot.data.get() };

                return Some(RelationSlot {
                    flags: atomic::AtomicU64::new(2),
                    expired: atomic::AtomicU64::new(slot.expired.load(atomic::Ordering::SeqCst)),
                    created: atomic::AtomicU64::new(slot.created.load(atomic::Ordering::SeqCst)),
                    row_id: atomic::AtomicU64::new(slot.row_id.load(atomic::Ordering::SeqCst)),
                    data: core::cell::UnsafeCell::new(slot_data.clone()),
                });
            }
        }

        None
    }
}

impl RelationSlot {
    pub fn into_row(self) -> crate::Row {
        crate::Row {
            rid: self.row_id.load(atomic::Ordering::SeqCst),
            data: self.data.into_inner(),
        }
    }
}

impl Drop for RelationList {
    fn drop(&mut self) {
        assert_eq!(0, self.active_handles.load(atomic::Ordering::SeqCst));
        assert_eq!(false, self.locked.load(atomic::Ordering::SeqCst));

        let mut block_ptr = self.head.load(atomic::Ordering::SeqCst);
        while !block_ptr.is_null() {
            let block = unsafe { Box::from_raw(block_ptr) };

            block_ptr = block.next.load(atomic::Ordering::SeqCst);

            let _ = block;
        }
    }
}

impl Drop for RelationListHandle {
    fn drop(&mut self) {
        self.list
            .active_handles
            .fetch_sub(1, atomic::Ordering::SeqCst);
    }
}

impl Drop for RelationListExclusiveHandle {
    fn drop(&mut self) {
        self.list.locked.store(false, atomic::Ordering::SeqCst);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn create_relation_list() {
        let _ = RelationList::new();
    }

    #[test]
    fn relation_list_empty_iter() {
        let list = Arc::new(RelationList::new());

        let listhandle = RelationList::try_get_handle(list).unwrap();
        let mut iter = listhandle.iter();
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }

    #[test]
    fn relation_list_empty_iter_block() {
        let list = Arc::new(RelationList::new());

        let listhandle = RelationList::try_get_handle(list).unwrap();
        let mut iter = listhandle.iter();

        let first_block = iter.next().unwrap();
        assert!(iter.next().is_none());

        let mut block_iter = first_block.iter();
        assert!(block_iter.next().is_none());
    }

    #[test]
    fn relation_list_empty_handle_iter() {
        let list = Arc::new(RelationList::new());

        let listhandle = RelationList::try_get_handle(list).unwrap();
        let mut iter = listhandle.into_iter();

        assert!(iter.next().is_none());
    }

    #[test]
    fn insert() {
        let list = Arc::new(RelationList::new());

        let handle = RelationList::try_get_handle(list).unwrap();
        handle.insert_row(vec![crate::Data::Integer(123)], 1);

        let mut iter = handle.into_iter();

        let first_entry = iter.next().unwrap();
        assert_eq!(1, first_entry.created.load(atomic::Ordering::SeqCst));
    }

    #[test]
    fn insert_with_new_block() {
        let list = Arc::new(RelationList::new());

        let handle = RelationList::try_get_handle(list).unwrap();
        for i in 0..BLOCK_SIZE + 1 {
            handle.insert_row(vec![crate::Data::Integer(i as i32)], i as u64);
        }

        let iter = handle.into_iter();
        assert_eq!(BLOCK_SIZE + 1, iter.count());
    }

    #[test]
    fn getting_exclusive_handle() {
        let list = Arc::new(RelationList::new());

        let _ehandle = RelationList::try_get_exclusive_handle(list).unwrap();
    }

    #[test]
    fn getting_two_exclusive_handles() {
        let list = Arc::new(RelationList::new());

        let first = RelationList::try_get_exclusive_handle(list.clone());
        let second = RelationList::try_get_exclusive_handle(list.clone());

        assert!(first.is_some());
        assert!(second.is_none());
    }

    #[test]
    fn handle_then_exclusive() {
        let list = Arc::new(RelationList::new());

        let handle = RelationList::try_get_handle(list.clone()).unwrap();
        assert!(RelationList::try_get_exclusive_handle(list.clone()).is_none());
        assert!(RelationList::try_get_handle(list.clone()).is_some());

        drop(handle);
        assert!(RelationList::try_get_exclusive_handle(list.clone()).is_some());
    }

    #[test]
    fn update_rows() {
        let list = Arc::new(RelationList::new());

        {
            let handle = RelationList::try_get_handle(list.clone()).unwrap();
            for idx in 0..5 {
                handle.insert_row(vec![crate::Data::Integer(idx)], 0);
            }
        }

        let mut handle = RelationList::try_get_exclusive_handle(list.clone()).unwrap();
        handle.update_rows(|row| {
            row.push(crate::Data::Null);
        });

        drop(handle);

        let handle = RelationList::try_get_handle(list.clone()).unwrap();
        let mut iter = handle.into_iter();
        assert_eq!(
            Some(vec![crate::Data::Integer(0), crate::Data::Null]),
            iter.next().map(|s| s.data.into_inner())
        );
        assert_eq!(
            Some(vec![crate::Data::Integer(1), crate::Data::Null]),
            iter.next().map(|s| s.data.into_inner())
        );
        assert_eq!(
            Some(vec![crate::Data::Integer(2), crate::Data::Null]),
            iter.next().map(|s| s.data.into_inner())
        );
        assert_eq!(
            Some(vec![crate::Data::Integer(3), crate::Data::Null]),
            iter.next().map(|s| s.data.into_inner())
        );
        assert_eq!(
            Some(vec![crate::Data::Integer(4), crate::Data::Null]),
            iter.next().map(|s| s.data.into_inner())
        );
        assert_eq!(None, iter.next().map(|s| s.data.into_inner()));
    }
}

#[cfg(loom)]
mod loom_tests {
    use super::*;

    #[test]
    fn insert() {
        loom::model(|| {
            let list = Arc::new(RelationList::new());

            let ths: Vec<_> = (0..2)
                .map(|idx| {
                    let list = list.clone();
                    loom::thread::spawn(move || {
                        let handle = RelationList::try_get_handle(list).unwrap();

                        handle.insert_row(vec![crate::Data::Integer(idx)], 1);
                    })
                })
                .collect();

            for h in ths {
                h.join().unwrap();
            }

            let handle = RelationList::try_get_handle(list).unwrap();
            let mut iter = handle.into_iter();
            assert!(iter.next().is_some());
            assert!(iter.next().is_some());
        });
    }

    #[test]
    fn get_two_exclusive() {
        loom::model(|| {
            let list = Arc::new(RelationList::new());

            let ths: Vec<_> = (0..2)
                .map(|idx| {
                    let list = list.clone();
                    loom::thread::spawn(move || RelationList::try_get_exclusive_handle(list))
                })
                .collect();

            let results: Vec<_> = ths.into_iter().map(|t| t.join().unwrap()).collect();
            assert!(!results.iter().all(|h| h.is_some()));
            assert!(!results.iter().all(|h| h.is_none()));
        });
    }
}
