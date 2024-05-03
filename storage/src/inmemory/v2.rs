use core::{cell::RefCell, sync::atomic};
use std::collections::{HashMap, HashSet};
#[cfg(not(loom))]
use std::sync::Arc;

#[cfg(loom)]
use loom::sync::Arc;

use crate::{mvcc, RelationStorage};

use futures::stream::StreamExt;

mod internal;

async fn _yield() {
    #[cfg(not(loom))]
    tokio::task::yield_now().await;
}

pub struct InMemoryStorage {
    schemas: RefCell<crate::Schemas>,
    relations: RefCell<HashMap<String, Arc<internal::RelationList>>>,
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

#[derive(Debug)]
pub enum LoadingError {
    Other(&'static str),
}

impl RelationStorage for InMemoryStorage {
    type LoadingError = LoadingError;
    type TransactionGuard = InMemoryTransactionGuard;

    async fn schemas(&self) -> Result<crate::Schemas, Self::LoadingError> {
        let schemas = self
            .schemas
            .try_borrow()
            .map_err(|_| LoadingError::Other("Borrowing Schemas"))?;

        Ok(schemas.clone())
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
        let tmp: Arc<internal::RelationList> = self
            .relations
            .borrow()
            .get(name)
            .cloned()
            .ok_or(LoadingError::Other("Borrow Relations"))?;
        let schemas = self
            .schemas
            .try_borrow()
            .map_err(|e| LoadingError::Other("Borrow Schemas"))?;

        let handle = loop {
            match internal::RelationList::try_get_handle(tmp.clone()) {
                Some(h) => break h,
                None => {
                    _yield().await;
                }
            }
        };

        let schema = schemas
            .get_table(name)
            .ok_or(LoadingError::Other("Get Schema for Table"))?;

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

        Ok((schema.clone(), stream.boxed_local()))
    }

    async fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<crate::Data>>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let tmp: Arc<internal::RelationList> = self
            .relations
            .borrow()
            .get(name)
            .cloned()
            .ok_or(LoadingError::Other("Borrow Relations"))?;

        let handle = loop {
            match internal::RelationList::try_get_handle(tmp.clone()) {
                Some(h) => break h,
                None => {
                    _yield().await;
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
        let tmp: Arc<internal::RelationList> = self
            .relations
            .borrow()
            .get(name)
            .cloned()
            .ok_or(LoadingError::Other("Borrow Relations"))?;

        let handle = loop {
            match internal::RelationList::try_get_handle(tmp.clone()) {
                Some(h) => break h,
                None => {
                    _yield().await;
                }
            }
        };

        for (row_id, nvalues) in rows {
            let slot_iter = handle.iter().flat_map(|b| b.iter()).filter(|r| {
                mvcc::check(
                    transaction.id,
                    &transaction.active,
                    &transaction.aborted,
                    transaction.latest_commit,
                    r.created.load(atomic::Ordering::SeqCst),
                    r.expired.load(atomic::Ordering::SeqCst),
                ) == mvcc::Visibility::Visible
            });
            for slot in slot_iter {
                if slot.row_id.load(atomic::Ordering::SeqCst) == row_id {
                    match slot.expired.compare_exchange(
                        0,
                        transaction.id,
                        atomic::Ordering::SeqCst,
                        atomic::Ordering::SeqCst,
                    ) {
                        Ok(_) => {}
                        Err(_) => {
                            // TODO
                            // Someone else also updated this row
                        }
                    };
                    break;
                }
            }

            handle.insert_row(nvalues, transaction.id);
        }

        Ok(())
    }
    async fn delete_rows(
        &self,
        name: &str,
        rids: &mut dyn Iterator<Item = u64>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let tmp: Arc<internal::RelationList> = self
            .relations
            .borrow()
            .get(name)
            .cloned()
            .ok_or(LoadingError::Other("Borrow Relations"))?;

        let handle = loop {
            match internal::RelationList::try_get_handle(tmp.clone()) {
                Some(h) => break h,
                None => {
                    _yield().await;
                }
            }
        };

        for row_id in rids {
            let slot_iter = handle.iter().flat_map(|b| b.iter()).filter(|r| {
                mvcc::check(
                    transaction.id,
                    &transaction.active,
                    &transaction.aborted,
                    transaction.latest_commit,
                    r.created.load(atomic::Ordering::SeqCst),
                    r.expired.load(atomic::Ordering::SeqCst),
                ) == mvcc::Visibility::Visible
            });
            for slot in slot_iter {
                if slot.row_id.load(atomic::Ordering::SeqCst) == row_id {
                    match slot.expired.compare_exchange(
                        0,
                        transaction.id,
                        atomic::Ordering::SeqCst,
                        atomic::Ordering::SeqCst,
                    ) {
                        Ok(_) => {}
                        Err(_) => {
                            // TODO
                            // Someone else also updated this row
                        }
                    };
                    break;
                }
            }
        }

        Ok(())
    }

    async fn relation_exists(
        &self,
        name: &str,
        _transaction: &Self::TransactionGuard,
    ) -> Result<bool, Self::LoadingError> {
        let schemas = self
            .schemas
            .try_borrow()
            .map_err(|e| LoadingError::Other("Borrow Schemas"))?;

        Ok(schemas.tables.contains_key(name))
    }
    async fn create_relation(
        &self,
        name: &str,
        fields: std::vec::Vec<(String, sql::DataType, Vec<sql::TypeModifier>)>,
        _transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut tables_mut = self
            .relations
            .try_borrow_mut()
            .map_err(|_| LoadingError::Other("Relations"))?;

        tables_mut.insert(name.to_owned(), Arc::new(internal::RelationList::new()));

        let mut schemas_mut = self
            .schemas
            .try_borrow_mut()
            .map_err(|_| LoadingError::Other("Schemas"))?;
        schemas_mut.tables.insert(
            name.to_owned(),
            crate::TableSchema {
                rows: fields
                    .into_iter()
                    .map(|(name, ty, mods)| crate::ColumnSchema { name, ty, mods })
                    .collect(),
            },
        );

        Ok(())
    }

    async fn rename_relation(
        &self,
        name: &str,
        target: &str,
        _transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut schemas = self
            .schemas
            .try_borrow_mut()
            .map_err(|e| LoadingError::Other("Schemas"))?;
        let mut relations = self
            .relations
            .try_borrow_mut()
            .map_err(|e| LoadingError::Other("Relations"))?;

        let schema = schemas
            .tables
            .remove(name)
            .ok_or(LoadingError::Other("Removing Schema"))?;
        schemas.tables.insert(target.to_owned(), schema);

        let list = relations
            .remove(name)
            .ok_or(LoadingError::Other("Removing Relation"))?;
        relations.insert(target.to_owned(), list);

        Ok(())
    }
    async fn remove_relation(
        &self,
        name: &str,
        _transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut schemas = self
            .schemas
            .try_borrow_mut()
            .map_err(|e| LoadingError::Other("Borrowing Schemas"))?;
        let mut relations = self
            .relations
            .try_borrow_mut()
            .map_err(|e| LoadingError::Other("Borrowing Relations"))?;

        let _schema = schemas
            .tables
            .remove(name)
            .ok_or(LoadingError::Other("Removing Schema"))?;
        let _list = relations
            .remove(name)
            .ok_or(LoadingError::Other("Remove Relation"))?;

        Ok(())
    }
    async fn modify_relation(
        &self,
        name: &str,
        modification: crate::ModifyRelation,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut schemas = self
            .schemas
            .try_borrow_mut()
            .map_err(|e| LoadingError::Other("Borrow Schemas"))?;
        let relations = self
            .relations
            .try_borrow()
            .map_err(|e| LoadingError::Other("Borrow Relations"))?;

        let schema = schemas
            .tables
            .get_mut(name)
            .ok_or(LoadingError::Other("Getting TableSchema"))?;
        let list = relations
            .get(name)
            .ok_or(LoadingError::Other("Getting Relation"))?;

        let mut list_handle = loop {
            match internal::RelationList::try_get_exclusive_handle(list.clone()) {
                Some(h) => break h,
                None => {
                    _yield().await;
                }
            }
        };

        for modification in modification.modifications {
            schema.apply_mod(&modification);

            match modification {
                crate::RelationModification::AddColumn { modifiers, .. } => {
                    let mut value = crate::Data::Null;
                    for modification in modifiers {
                        if let crate::TypeModifier::DefaultValue { value: Some(v) } = modification {
                            value = crate::Data::from_literal(&v);
                        }
                    }

                    list_handle.update_rows(|row| {
                        row.push(value.clone());
                    });
                }
                crate::RelationModification::ChangeType { name, ty } => {
                    let idx = match schema.rows.iter().enumerate().find_map(|(idx, column)| {
                        if column.name == name {
                            Some(idx)
                        } else {
                            None
                        }
                    }) {
                        Some(i) => i,
                        None => {
                            // TODO
                            // How to handle this?
                            continue;
                        }
                    };

                    list_handle.update_rows(|row| {
                        let slot = &mut row[idx];

                        let prev = core::mem::replace(slot, crate::Data::Null);
                        let nvalue = match prev.try_cast(&ty) {
                            Ok(v) => v,
                            _ => crate::Data::Null,
                        };

                        *slot = nvalue;
                    });
                }
                _ => {}
            };
        }

        Ok(())
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let mut relations = HashMap::new();
        let mut table_schemas = HashMap::new();

        for (name, schema, rows) in super::postgres_tables() {
            let list = Arc::new(internal::RelationList::new());
            let handle = internal::RelationList::try_get_handle(list.clone()).unwrap();
            for r in rows {
                handle.insert_row(r.data, 0);
            }

            relations.insert(name.clone(), list);
            table_schemas.insert(name, schema);
        }

        Self {
            schemas: RefCell::new(crate::Schemas {
                tables: table_schemas,
            }),
            relations: RefCell::new(relations),
            latest_commit: atomic::AtomicU64::new(0),
            aborted_tids: RefCell::new(HashSet::new()),
            active_tids: RefCell::new(HashSet::new()),
            current_tid: atomic::AtomicU64::new(1),
        }
    }
}
