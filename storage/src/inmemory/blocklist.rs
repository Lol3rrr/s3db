#[cfg(loom)]
use loom::sync::atomic::{self, AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize};
#[cfg(not(loom))]
use std::sync::atomic::{self, AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize};
use std::{fmt::Debug, future::Future, marker::PhantomData};

use crate::Data;

mod lock;

mod iter;
pub use iter::BlockIterator;

#[cfg(not(loom))]
type DefaultYield = tokio_yield::TokioYield;
#[cfg(loom)]
type DefaultYield = ();

pub struct BlockList<const N: usize, Y = DefaultYield> {
    head: AtomicPtr<Block<N>>,
    count: AtomicIsize,
    locked: AtomicBool,
    fields: usize,
    _marker: PhantomData<Y>,
}

pub struct Block<const N: usize> {
    next: AtomicPtr<Self>,
    fields: usize,
    position: AtomicUsize,
    data: *mut u8,
}

pub struct BlockSlot<'list, 'block, const N: usize, Y> {
    fields: usize,
    head: BlockListHead<'list, N, Y>,
    block: &'block Block<N>,
    pos: usize,
}

struct BlockListHead<'list, const N: usize, Y> {
    list: &'list BlockList<N, Y>,
    head: *mut Block<N>,
}

pub trait Yielding {
    fn yielding() -> impl Future<Output = ()> + 'static;
}
impl Yielding for () {
    async fn yielding() {}
}

#[cfg(not(loom))]
mod tokio_yield {
    pub struct TokioYield {}

    impl super::Yielding for TokioYield {
        fn yielding() -> impl std::future::Future<Output = ()> {
            tokio::task::yield_now()
        }
    }
}

impl<const N: usize, Y> BlockList<N, Y>
where
    Y: Yielding,
{
    pub fn new(fields: usize) -> Self {
        Self {
            head: AtomicPtr::new(Block::<N>::new(fields)),
            count: AtomicIsize::new(0),
            locked: AtomicBool::new(false),
            fields,
            _marker: PhantomData {},
        }
    }

    pub fn lock(&self) -> impl Future<Output = lock::BlockLockGuard<'_, N, Y>> {
        lock::LockFuture::new(self)
    }

    async fn get_head(&self) -> BlockListHead<'_, N, Y> {
        let mut count = self.count.load(atomic::Ordering::SeqCst);
        loop {
            while self.locked.load(atomic::Ordering::SeqCst) {
                Y::yielding().await;
            }

            match self.count.compare_exchange(
                count,
                count + 1,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return BlockListHead {
                        list: self,
                        head: self.head.load(atomic::Ordering::SeqCst),
                    }
                }
                Err(val) => {
                    count = val;
                }
            };
        }
    }

    pub async fn iter(&self) -> BlockIterator<'_, N, Y> {
        let head = self.get_head().await;
        let head_ptr = head.head;

        BlockIterator {
            list: head,
            head: head_ptr,
        }
    }

    pub async fn get_slot<'s, 'list, 'block>(&'s self) -> BlockSlot<'list, 'block, N, Y>
    where
        's: 'list,
        's: 'block,
    {
        let head = self.get_head().await;
        let mut prev = head.head;
        let mut head_ptr = prev;

        loop {
            if head_ptr.is_null() {
                let block = Block::<N>::new(self.fields);
                dbg!(block);

                let prev_block = unsafe { &*prev };
                match prev_block.next.compare_exchange(
                    core::ptr::null_mut(),
                    block,
                    atomic::Ordering::SeqCst,
                    atomic::Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        head_ptr = block;
                    }
                    Err(n) => {
                        // Free the allocated block
                        unsafe { Block::<N>::free(block) };

                        head_ptr = n;
                    }
                }
            }

            let head_ref = unsafe { &*head_ptr };
            let pos = head_ref.position.fetch_add(1, atomic::Ordering::SeqCst);
            if pos >= N {
                head_ref.position.store(N, atomic::Ordering::Relaxed);
                prev = head_ptr;
                head_ptr = head_ref.next.load(atomic::Ordering::SeqCst);
                continue;
            }

            return BlockSlot {
                fields: self.fields,
                head,
                block: head_ref,
                pos,
            };
        }
    }
}

const _: () = assert!(
    32 == std::alloc::Layout::new::<Data>().size(),
    "Size of storage::Data has changed"
);
const _: () = assert!(
    8 == std::alloc::Layout::new::<Data>().align(),
    "Alignment of storage::Data has changed"
);
const _: () = assert!(8 == std::alloc::Layout::new::<u64>().size());
const _: () = assert!(8 == std::alloc::Layout::new::<u64>().align());

impl<const N: usize> Block<N> {
    pub fn new(fields: usize) -> *mut Block<N> {
        let entry_layout = std::alloc::Layout::from_size_align(32, 8).unwrap();
        let (target_layout, offsets) = entry_layout.repeat((1 + fields) * N).unwrap();

        assert_eq!(32, offsets);

        let data = unsafe { std::alloc::alloc_zeroed(target_layout) };

        for pos in 0..N {
            let addr = unsafe { data.add(pos * offsets) };
            unsafe { core::ptr::write(addr as *mut AtomicU64, AtomicU64::new(0)) };
            unsafe { core::ptr::write(addr.byte_add(8) as *mut AtomicU64, AtomicU64::new(0)) };
            unsafe { core::ptr::write(addr.byte_add(16) as *mut AtomicU64, AtomicU64::new(0)) };
        }

        let tmp = Box::new(Block {
            next: AtomicPtr::new(core::ptr::null_mut()),
            fields,
            position: AtomicUsize::new(0),
            data,
        });

        Box::leak(tmp)
    }

    unsafe fn free(ptr: *mut Self) {
        todo!("Freing Block")
    }

    pub fn rows(&self) -> iter::RowIter<'_, N> {
        iter::RowIter {
            block: self,
            pos: 0,
        }
    }

    unsafe fn try_get_entry(
        &self,
        entry: usize,
    ) -> Option<(&'_ AtomicU64, &'_ AtomicU64, &'_ [Data])> {
        let row_addr = unsafe { self.data.byte_add(entry * ((self.fields + 1) * 32)) };
        dbg!(row_addr);

        let lock_addr = row_addr as *mut AtomicU64;
        let lock_field = unsafe { &*lock_addr };

        if lock_field.load(atomic::Ordering::SeqCst) == 0 {
            return None;
        }

        atomic::fence(atomic::Ordering::SeqCst);

        let created_field_addr = unsafe { row_addr.byte_offset(8) as *mut AtomicU64 };
        let expired_field_addr = unsafe { row_addr.byte_offset(16) as *mut AtomicU64 };

        let created_field = unsafe { &*created_field_addr };
        let expired_field = unsafe { &*expired_field_addr };

        let data = unsafe {
            core::slice::from_raw_parts(row_addr.byte_offset(32) as *mut Data, self.fields)
        };

        Some((created_field, expired_field, data))
    }
}

impl<'list, 'block, const N: usize, Y> Debug for BlockSlot<'list, 'block, N, Y> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockSlot").field("pos", &self.pos).finish()
    }
}
impl<'list, 'block, const N: usize, Y> BlockSlot<'list, 'block, N, Y> {
    pub fn insert_data(self, data: Vec<Data>, created_at: u64, expired_at: u64) {
        assert_eq!(self.fields, data.len());

        let slot_data_start = unsafe { self.block.data.byte_offset(self.pos as isize * 32) };

        let slot_fields_start = unsafe { slot_data_start.byte_offset(32) } as *mut Data;
        for (idx, entry) in data.into_iter().enumerate() {
            let field_addr = unsafe { slot_fields_start.offset(idx as isize) };
            unsafe {
                core::ptr::write(field_addr, entry);
            }
        }

        let lock_field_addr = slot_data_start as *mut AtomicU64;
        let created_field_addr = unsafe { slot_data_start.byte_offset(8) as *mut AtomicU64 };
        let expired_field_addr = unsafe { slot_data_start.byte_offset(16) as *mut AtomicU64 };

        unsafe {
            (&*created_field_addr).store(created_at, atomic::Ordering::SeqCst);
            (&*expired_field_addr).store(expired_at, atomic::Ordering::SeqCst);
        }

        atomic::fence(atomic::Ordering::SeqCst);

        let lock_field = unsafe { &*lock_field_addr };
        lock_field.store(1, atomic::Ordering::SeqCst);
    }
}

impl<'list, const N: usize, Y> Drop for BlockListHead<'list, N, Y> {
    fn drop(&mut self) {
        self.list.count.fetch_sub(1, atomic::Ordering::SeqCst);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn create() {
        let list = BlockList::<4>::new(1);
        let _ = list;
    }

    #[tokio::test]
    async fn lock() {
        let list = BlockList::<4>::new(1);

        let first_lock = tokio::time::timeout(Duration::from_millis(100), list.lock())
            .await
            .unwrap();

        let second_lock = tokio::time::timeout(Duration::from_millis(100), list.lock()).await;
        assert_eq!(true, second_lock.is_err());

        drop(first_lock);

        let second_lock = tokio::time::timeout(Duration::from_millis(100), list.lock())
            .await
            .unwrap();

        drop(second_lock);
    }

    #[tokio::test]
    async fn lock_attempt_insert() {
        let list = BlockList::<4>::new(1);

        let lock = tokio::time::timeout(Duration::from_millis(100), list.lock())
            .await
            .unwrap();

        let attempted_insert =
            tokio::time::timeout(Duration::from_millis(100), list.get_slot()).await;
        assert_eq!(true, attempted_insert.is_err());

        drop(lock);

        let attempted_insert =
            tokio::time::timeout(Duration::from_millis(100), list.get_slot()).await;
        assert_eq!(true, attempted_insert.is_ok());
    }

    #[tokio::test]
    async fn get_slot_attempt_lock() {
        let list = BlockList::<4>::new(1);

        let attempted_insert =
            tokio::time::timeout(Duration::from_millis(100), list.get_slot()).await;
        assert_eq!(true, attempted_insert.is_ok());

        let attempted_lock = tokio::time::timeout(Duration::from_millis(100), list.lock()).await;
        assert_eq!(true, attempted_lock.is_err());

        drop(attempted_insert);

        let attempted_lock = tokio::time::timeout(Duration::from_millis(100), list.lock()).await;
        assert_eq!(true, attempted_lock.is_ok());
    }

    #[tokio::test]
    async fn insert() {
        let tmp = BlockList::<4>::new(1);

        let slot = tmp.get_slot().await;
        slot.insert_data(vec![Data::Integer(1)], 2, 0);
    }

    #[tokio::test]
    async fn single_insert_iter() {
        let tmp = BlockList::<4>::new(1);

        let slot = tmp.get_slot().await;
        slot.insert_data(vec![Data::Integer(1)], 2, 0);

        let mut block_iter = tmp.iter().await;
        assert_eq!(1, (&mut block_iter).count());
    }

    #[tokio::test]
    async fn insert_iter_lock() {
        let tmp = BlockList::<4>::new(1);

        let slot = tmp.get_slot().await;
        slot.insert_data(vec![Data::Integer(1)], 2, 0);

        let mut block_iter = tmp.iter().await;
        assert_eq!(1, (&mut block_iter).count());
        drop(block_iter);

        let lock_attempt = tokio::time::timeout(Duration::from_millis(100), tmp.lock()).await;
        assert_eq!(true, lock_attempt.is_ok());
    }

    #[tokio::test]
    async fn single_insert_iter_rows() {
        let tmp = BlockList::<4>::new(1);

        let slot = tmp.get_slot().await;
        slot.insert_data(vec![Data::Integer(1)], 2, 0);

        let mut block_iter = tmp.iter().await;
        let rows: Vec<_> = (&mut block_iter).flat_map(|block| block.rows()).collect();

        let expected = [(2, 0, &[Data::Integer(1)])];
        for (elem, (expected, actual)) in expected.iter().zip(rows.iter()).enumerate() {
            assert_eq!(expected.0, actual.0.load(atomic::Ordering::SeqCst));
            assert_eq!(expected.1, actual.1.load(atomic::Ordering::SeqCst));
            assert_eq!(expected.2, actual.2);
        }
    }

    #[tokio::test]
    async fn fill_more_than_two_blocks() {
        let list = BlockList::<2>::new(1);

        for idx in 0..5 {
            let slot = list.get_slot().await;
            slot.insert_data(vec![Data::Integer(idx)], 2, 0);
        }
    }
}
