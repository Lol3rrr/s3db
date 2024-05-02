use core::{cell::RefCell, sync::atomic};
use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

use crate::{mvcc, RelationStorage};

use futures::stream::StreamExt;

mod internal;

pub struct InMemoryStorage {
    relations: RefCell<HashMap<String, Rc<internal::RelationList>>>,
    current_tid: atomic::AtomicU64,
    active_tids: RefCell<HashSet<u64>>,
    aborted_tids: RefCell<HashSet<u64>>,
    latest_commit: atomic::AtomicU64,
}

#[derive(Debug, Clone)]
pub struct InMemoryTransactionGuard {
    id: u64,
    active: HashSet<u64>,
    aborted: HashSet<u64>,
    latest_commit: u64,
}

impl RelationStorage for InMemoryStorage {
    type LoadingError = ();
    type TransactionGuard = InMemoryTransactionGuard;

    async fn schemas(&self) -> Result<crate::Schemas, Self::LoadingError> {
        todo!()
    }

    async fn start_transaction(&self) -> Result<Self::TransactionGuard, Self::LoadingError> {
        let id = self.current_tid.fetch_add(1, atomic::Ordering::AcqRel);
        let mut active_ids = self.active_tids.try_borrow_mut().unwrap();
        active_ids.insert(id);

        let aborted_ids = self.aborted_tids.try_borrow().unwrap();

        Ok(InMemoryTransactionGuard {
            id,
            latest_commit: self.latest_commit.load(atomic::Ordering::SeqCst),
            active: active_ids.clone(),
            aborted: aborted_ids.clone(),
        })
    }

    async fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut active_ids = self.active_tids.try_borrow_mut().unwrap();
        active_ids.remove(&guard.id);

        loop {
            let value = self.latest_commit.load(atomic::Ordering::Acquire);
            let n_value = core::cmp::max(value, guard.id);

            match self.latest_commit.compare_exchange(
                value,
                n_value,
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            };
        }

        Ok(())
    }
    async fn abort_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut active_ids = self.active_tids.try_borrow_mut().unwrap();
        active_ids.remove(&guard.id);

        let mut aborted_ids = self.aborted_tids.try_borrow_mut().unwrap();
        aborted_ids.insert(guard.id);

        Ok(())
    }

    async fn stream_relation<'own, 'name, 'transaction, 'stream, 'rowdata>(
        &'own self,
        name: &'name str,
        transaction: &'transaction Self::TransactionGuard,
    ) -> Result<
        (
            crate::TableSchema,
            futures::stream::LocalBoxStream<'stream, crate::RowCow<'rowdata>>,
        ),
        Self::LoadingError,
    >
    where
        'own: 'stream,
        'name: 'stream,
        'transaction: 'stream,
        'own: 'rowdata,
    {
        let tmp: Rc<internal::RelationList> =
            self.relations.borrow().get(name).cloned().ok_or(())?;

        let handle = loop {
            match tmp.clone().try_get_handle() {
                Some(h) => break h,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        };

        let schema = crate::TableSchema { rows: Vec::new() };
        let stream = futures::stream::iter(
            handle
                .into_iter()
                .filter(|r| {
                    mvcc::check(
                        transaction.id,
                        &transaction.active,
                        &transaction.aborted,
                        transaction.latest_commit,
                        r.created.load(atomic::Ordering::SeqCst),
                        r.expired.load(atomic::Ordering::SeqCst),
                    ) == mvcc::Visibility::Visible
                })
                .map(|r| crate::RowCow::Owned(r.into_row())),
        );

        Ok((schema, stream.boxed_local()))
    }

    async fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<crate::Data>>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let tmp: Rc<internal::RelationList> =
            self.relations.borrow().get(name).cloned().ok_or(())?;

        let handle = loop {
            match tmp.clone().try_get_handle() {
                Some(h) => break h,
                None => {
                    tokio::task::yield_now().await;
                }
            }
        };

        for row in rows {
            handle.insert_row(row, transaction.id);
        }

        Ok(())
    }
    async fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<crate::Data>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
    }
    async fn delete_rows(
        &self,
        name: &str,
        rids: &mut dyn Iterator<Item = u64>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
    }

    async fn relation_exists(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<bool, Self::LoadingError> {
        todo!()
    }
    async fn create_relation(
        &self,
        name: &str,
        fields: std::vec::Vec<(String, sql::DataType, Vec<sql::TypeModifier>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
    }

    async fn rename_relation(
        &self,
        name: &str,
        target: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
    }
    async fn remove_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
    }
    async fn modify_relation(
        &self,
        name: &str,
        modification: crate::ModifyRelation,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
    }
}
