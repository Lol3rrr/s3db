use core::sync::atomic;
use std::rc::Rc;

const BLOCK_SIZE: usize = 512;

pub struct RelationList {
    head: atomic::AtomicPtr<RelationBlock<BLOCK_SIZE>>,
    tail: atomic::AtomicPtr<RelationBlock<BLOCK_SIZE>>,
    locked: atomic::AtomicBool,
    active_handles: atomic::AtomicUsize,
    newest_row_id: atomic::AtomicU64,
}

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
    list: Rc<RelationList>,
}

pub struct RelationListIter<'h> {
    handle: &'h RelationListHandle,
    current_block: *mut RelationBlock<BLOCK_SIZE>,
}

pub struct RelationBlockIter<'b, const N: usize> {
    block: &'b RelationBlock<N>,
    idx: usize,
}

pub struct HandleRowIter {
    handle: RelationListHandle,
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

    pub fn try_get_handle(self: Rc<Self>) -> Option<RelationListHandle> {
        if self.locked.load(atomic::Ordering::SeqCst) {
            return None;
        }

        self.active_handles.fetch_add(1, atomic::Ordering::SeqCst);

        Some(RelationListHandle { list: self })
    }
}

impl RelationListHandle {
    pub fn iter<'h>(&'h self) -> RelationListIter<'h> {
        RelationListIter {
            handle: self,
            current_block: self.list.head.load(atomic::Ordering::SeqCst),
        }
    }

    pub fn into_iter(self) -> HandleRowIter {
        let block = self.list.head.load(atomic::Ordering::SeqCst);

        HandleRowIter {
            handle: self,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_relation_list() {
        let _ = RelationList::new();
    }

    #[test]
    fn relation_list_empty_iter() {
        let list = Rc::new(RelationList::new());

        let listhandle = list.try_get_handle().unwrap();
        let mut iter = listhandle.iter();
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }

    #[test]
    fn relation_list_empty_iter_block() {
        let list = Rc::new(RelationList::new());

        let listhandle = list.try_get_handle().unwrap();
        let mut iter = listhandle.iter();

        let first_block = iter.next().unwrap();
        assert!(iter.next().is_none());

        let mut block_iter = first_block.iter();
        assert!(block_iter.next().is_none());
    }

    #[test]
    fn relation_list_empty_handle_iter() {
        let list = Rc::new(RelationList::new());

        let listhandle = list.try_get_handle().unwrap();
        let mut iter = listhandle.into_iter();

        assert!(iter.next().is_none());
    }

    #[test]
    fn insert() {
        let list = Rc::new(RelationList::new());

        let handle = list.try_get_handle().unwrap();
        handle.insert_row(vec![crate::Data::Integer(123)], 1);

        let mut iter = handle.into_iter();

        let first_entry = iter.next().unwrap();
        assert_eq!(1, first_entry.created.load(atomic::Ordering::SeqCst));
    }

    #[test]
    fn insert_with_new_block() {
        let list = Rc::new(RelationList::new());

        let handle = list.try_get_handle().unwrap();
        for i in 0..BLOCK_SIZE + 1 {
            handle.insert_row(vec![crate::Data::Integer(i as i32)], i as u64);
        }

        let iter = handle.into_iter();
        assert_eq!(BLOCK_SIZE + 1, iter.count());
    }
}
