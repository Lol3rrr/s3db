#[cfg(loom)]
use loom::sync::atomic::{self, AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize};
#[cfg(not(loom))]
use std::sync::atomic::{self, AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize};

use crate::Data;

use super::{Block, BlockListHead};

pub struct BlockIterator<'list, const N: usize, Y> {
    pub(super) list: BlockListHead<'list, N, Y>,
    pub(super) head: *mut Block<N>,
}

impl<'iter, 'list, const N: usize, Y> Iterator for &'iter mut BlockIterator<'list, N, Y>
where
    'list: 'iter,
{
    type Item = &'iter Block<N>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.head.is_null() {
            return None;
        }

        let block = unsafe { &*self.head };
        self.head = block.next.load(atomic::Ordering::SeqCst);

        Some(block)
    }
}

pub struct RowIter<'block, const N: usize> {
    pub(super) block: &'block Block<N>,
    pub(super) pos: usize,
}

impl<'block, const N: usize> Iterator for RowIter<'block, N> {
    type Item = (&'block AtomicU64, &'block AtomicU64, &'block [Data]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos >= N {
                return None;
            }

            let attempted_load = unsafe { self.block.try_get_entry(self.pos) };

            self.pos += 1;
            match attempted_load {
                Some(val) => return Some(val),
                None => {
                    continue;
                }
            }
        }
    }
}
