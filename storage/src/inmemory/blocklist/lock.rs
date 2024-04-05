#[cfg(loom)]
use loom::sync::atomic::{self, AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize};
#[cfg(not(loom))]
use std::sync::atomic::{self, AtomicBool, AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize};
use std::{future::Future, pin::Pin};

use super::BlockList;

pub struct LockFuture<'list, const N: usize, Y> {
    list: &'list BlockList<N, Y>,
    state: LockState,
}

enum LockState {
    Nothing(Option<Pin<Box<dyn Future<Output = ()>>>>),
    Locked,
    Waiting(Option<Pin<Box<dyn Future<Output = ()>>>>),
    Resolved,
}

pub struct BlockLockGuard<'list, const N: usize, Y> {
    pub(super) list: &'list BlockList<N, Y>,
}

impl<'list, const N: usize, Y> LockFuture<'list, N, Y> {
    pub fn new(list: &'list BlockList<N, Y>) -> Self {
        Self {
            list,
            state: LockState::Nothing(None),
        }
    }
}

impl<'list, const N: usize, Y> Future for LockFuture<'list, N, Y>
where
    Y: super::Yielding,
{
    type Output = BlockLockGuard<'list, N, Y>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {
            let list = self.list;
            let state = &mut self.state;
            match state {
                LockState::Nothing(fut) => {
                    match list.locked.compare_exchange(
                        false,
                        true,
                        atomic::Ordering::SeqCst,
                        atomic::Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            self.state = LockState::Locked;
                        }
                        Err(_) => {
                            match fut {
                                Some(fut) => {
                                    match fut.as_mut().poll(cx) {
                                        core::task::Poll::Ready(_) => {}
                                        core::task::Poll::Pending => {
                                            return core::task::Poll::Pending
                                        }
                                    };
                                }
                                None => {}
                            };

                            *fut = Some(Box::pin(Y::yielding()));
                        }
                    };
                }
                LockState::Locked => {
                    list.count.fetch_sub(1, atomic::Ordering::SeqCst);
                    self.state = LockState::Waiting(None);
                }
                LockState::Waiting(fut) => {
                    let count = list.count.load(atomic::Ordering::SeqCst);

                    if count < 0 {
                        self.state = LockState::Resolved;
                        return core::task::Poll::Ready(BlockLockGuard { list: &self.list });
                    } else {
                        match fut {
                            Some(fut) => {
                                match fut.as_mut().poll(cx) {
                                    core::task::Poll::Ready(_) => {}
                                    core::task::Poll::Pending => return core::task::Poll::Pending,
                                };
                            }
                            None => {}
                        };

                        *fut = Some(Box::pin(Y::yielding()));
                    }
                }
                LockState::Resolved => return core::task::Poll::Pending,
            }
        }
    }
}

impl<'list, const N: usize, Y> Drop for LockFuture<'list, N, Y> {
    fn drop(&mut self) {
        match self.state {
            LockState::Nothing(_) => {}
            LockState::Locked => {
                self.list.locked.store(false, atomic::Ordering::SeqCst);
            }
            LockState::Waiting(_) => {
                self.list.locked.store(false, atomic::Ordering::SeqCst);
                self.list.count.fetch_add(1, atomic::Ordering::SeqCst);
            }
            LockState::Resolved => {}
        };
    }
}

impl<'list, const N: usize, Y> Drop for BlockLockGuard<'list, N, Y> {
    fn drop(&mut self) {
        self.list.count.fetch_add(1, atomic::Ordering::SeqCst);
        self.list.locked.store(false, atomic::Ordering::SeqCst);
    }
}
