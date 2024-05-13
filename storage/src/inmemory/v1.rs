use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use sql::{DataType, TypeModifier};

use crate::{
    mvcc, schema::ColumnSchema, Data, RelationModification, RelationStorage, Row, RowCow, Schemas,
    Sequence, SequenceStorage, Storage, TableSchema,
};

/// # InMemoryStorage
/// Stores all it's content in memory without any persistent storage. This is ideal for testing
/// or validating ideas but should never be used in production
pub struct InMemoryStorage {
    tables: RefCell<HashMap<String, Table>>,
    current_tid: AtomicU64,
    active_tids: RefCell<HashSet<u64>>,
    aborted_tids: RefCell<HashSet<u64>>,
    latest_commit: AtomicU64,

    // Sequence stuff
    sequences: RefCell<HashMap<String, Arc<AtomicU64>>>,
}

/// The TransactionGuard for the [`InMemoryStorage`] storage engine
#[derive(Debug, Clone)]
pub struct InMemoryTransactionGuard {
    id: u64,
    active: HashSet<u64>,
    aborted: HashSet<u64>,
    latest_commit: u64,
}

pub struct InMemorySequence<'seq> {
    counter: Arc<AtomicU64>,
    _marker: core::marker::PhantomData<&'seq ()>,
}

#[derive(Debug)]
pub enum LoadingError {
    BorrowingTables,
    UnknownRelation,
    Other(&'static str),
}

struct Table {
    columns: Vec<(String, DataType, Vec<TypeModifier>)>,
    rows: Vec<Vec<InternalRow>>,
    cid: AtomicU64,
}
#[derive(Debug)]
struct InternalRow {
    data: Row,
    created: u64,
    expired: u64,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        let mut tables = HashMap::new();

        for (name, schema, rows) in super::postgres_tables() {
            tables.insert(
                name,
                Table {
                    columns: schema
                        .rows
                        .into_iter()
                        .map(|c| (c.name, c.ty, c.mods))
                        .collect(),
                    rows: vec![rows
                        .into_iter()
                        .map(|r| InternalRow {
                            created: 0,
                            expired: 0,
                            data: r,
                        })
                        .collect()],
                    cid: AtomicU64::new(0),
                },
            );
        }

        Self {
            tables: RefCell::new(tables),
            current_tid: AtomicU64::new(1),
            active_tids: RefCell::new(HashSet::new()),
            aborted_tids: RefCell::new(HashSet::new()),
            latest_commit: AtomicU64::new(0),
            sequences: RefCell::new(HashMap::new()),
        }
    }
}

impl<'s> Sequence for InMemorySequence<'s> {
    async fn set_value(&self, value: u64) -> () {
        self.counter
            .store(value, core::sync::atomic::Ordering::SeqCst);
    }

    async fn get_next(&self) -> u64 {
        self.counter
            .fetch_add(1, core::sync::atomic::Ordering::SeqCst)
    }
}

impl SequenceStorage for InMemoryStorage {
    type SequenceHandle<'s> = InMemorySequence<'s> where Self: 's;

    async fn create_sequence(&self, name: &str) -> Result<(), ()> {
        <&InMemoryStorage as SequenceStorage>::create_sequence(&self, name).await
    }
    async fn remove_sequence(&self, name: &str) -> Result<(), ()> {
        <&InMemoryStorage as SequenceStorage>::remove_sequence(&self, name).await
    }

    async fn get_sequence<'se, 'seq>(
        &'se self,
        name: &str,
    ) -> Result<Option<Self::SequenceHandle<'seq>>, ()>
    where
        'se: 'seq,
    {
        let tmp = <&InMemoryStorage as SequenceStorage>::get_sequence(&self, name).await?;
        Ok(tmp.map(|tmp| InMemorySequence {
            counter: tmp.counter,
            _marker: core::marker::PhantomData {},
        }))
    }
}

impl RelationStorage for InMemoryStorage {
    type LoadingError = LoadingError;
    type TransactionGuard = InMemoryTransactionGuard;

    async fn start_transaction(&self) -> Result<Self::TransactionGuard, Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::start_transaction(&self).await
    }
    async fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::commit_transaction(&self, guard).await
    }
    async fn abort_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::abort_transaction(&self, guard).await
    }

    async fn stream_relation<'own, 'name, 'transaction, 'stream, 'rowdata>(
        &'own self,
        name: &'name str,
        transaction: &'transaction Self::TransactionGuard,
    ) -> Result<
        (
            TableSchema,
            futures::stream::LocalBoxStream<'stream, RowCow<'rowdata>>,
        ),
        Self::LoadingError,
    >
    where
        'own: 'stream,
        'name: 'stream,
        'transaction: 'stream,
        'own: 'rowdata,
    {
        use futures::stream::StreamExt;

        let tables = self
            .tables
            .try_borrow()
            .map_err(|_| LoadingError::BorrowingTables)?;
        let table = tables.get(name).ok_or(LoadingError::UnknownRelation)?;

        let columns: Vec<ColumnSchema> = table
            .columns
            .iter()
            .map(|(name, ty, modifiers)| ColumnSchema {
                name: name.to_string(),
                ty: ty.clone(),
                mods: modifiers.to_vec(),
            })
            .collect();
        let table_schema = TableSchema { rows: columns };

        let rows: Vec<Row> = table
            .rows
            .iter()
            .flat_map(|p| p)
            .filter(|row| {
                let vis = mvcc::check(
                    transaction.id,
                    &transaction.active,
                    &transaction.aborted,
                    transaction.latest_commit,
                    row.created,
                    row.expired,
                );
                mvcc::Visibility::Visible == vis
            })
            .map(|r| r.data.clone())
            .collect();

        let stream = futures::stream::iter(rows)
            .map(|r| RowCow::Owned(r))
            .boxed_local();

        Ok((table_schema, stream))
    }

    async fn relation_exists(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<bool, Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::relation_exists(&self, name, transaction).await
    }

    async fn create_relation(
        &self,
        name: &str,
        fields: Vec<(String, DataType, Vec<TypeModifier>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::create_relation(&self, name, fields, transaction)
            .await
    }

    async fn rename_relation(
        &self,
        name: &str,
        target: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::rename_relation(&self, name, target, transaction)
            .await
    }

    async fn remove_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::remove_relation(&self, name, transaction).await
    }

    async fn modify_relation(
        &self,
        name: &str,
        modification: crate::ModifyRelation,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::modify_relation(
            &self,
            name,
            modification,
            transaction,
        )
        .await
    }

    async fn schemas(&self) -> Result<Schemas, Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::schemas(&self).await
    }

    async fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<Data>>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::insert_rows(&self, name, rows, transaction).await
    }

    async fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<Data>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), crate::RelationError<Self::LoadingError>> {
        <&InMemoryStorage as RelationStorage>::update_rows(&self, name, rows, transaction).await
    }

    async fn delete_rows(
        &self,
        name: &str,
        rids: &mut dyn Iterator<Item = u64>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as RelationStorage>::delete_rows(&self, name, rids, transaction).await
    }
}

impl Storage for InMemoryStorage {}

impl SequenceStorage for &InMemoryStorage {
    type SequenceHandle<'s> = InMemorySequence<'s> where Self: 's;

    async fn create_sequence(&self, name: &str) -> Result<(), ()> {
        let mut seqs = self.sequences.try_borrow_mut().map_err(|e| ())?;
        if seqs.contains_key(name) {
            return Err(());
        }

        seqs.insert(name.to_string(), Arc::new(AtomicU64::new(1)));

        Ok(())
    }
    async fn remove_sequence(&self, name: &str) -> Result<(), ()> {
        let mut seqs = self.sequences.try_borrow_mut().map_err(|e| ())?;
        if !seqs.contains_key(name) {
            return Err(());
        }

        seqs.remove(name);

        Ok(())
    }

    async fn get_sequence<'se, 'seq>(
        &'se self,
        name: &str,
    ) -> Result<Option<Self::SequenceHandle<'seq>>, ()>
    where
        'se: 'seq,
    {
        let seqs = self.sequences.try_borrow().map_err(|e| ())?;

        match seqs.get(name) {
            Some(seq) => Ok(Some(InMemorySequence {
                counter: seq.clone(),
                _marker: core::marker::PhantomData {},
            })),
            None => Ok(None),
        }
    }
}

impl RelationStorage for &InMemoryStorage {
    type LoadingError = LoadingError;
    type TransactionGuard = InMemoryTransactionGuard;

    async fn start_transaction(&self) -> Result<Self::TransactionGuard, Self::LoadingError> {
        let id = self.current_tid.fetch_add(1, Ordering::AcqRel);
        let mut active_ids = self.active_tids.try_borrow_mut().unwrap();
        active_ids.insert(id);

        let aborted_ids = self.aborted_tids.try_borrow().unwrap();

        Ok(InMemoryTransactionGuard {
            id,
            active: active_ids.clone(),
            aborted: aborted_ids.clone(),
            latest_commit: self.latest_commit.load(Ordering::Acquire),
        })
    }

    async fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut active_ids = self.active_tids.try_borrow_mut().unwrap();
        active_ids.remove(&guard.id);

        loop {
            let value = self.latest_commit.load(Ordering::Acquire);
            let n_value = core::cmp::max(value, guard.id);

            match self.latest_commit.compare_exchange(
                value,
                n_value,
                Ordering::SeqCst,
                Ordering::SeqCst,
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
            TableSchema,
            futures::stream::LocalBoxStream<'stream, RowCow<'rowdata>>,
        ),
        Self::LoadingError,
    >
    where
        'own: 'stream,
        'name: 'stream,
        'transaction: 'stream,
        'own: 'rowdata,
    {
        use futures::stream::StreamExt;

        let tables = self
            .tables
            .try_borrow()
            .map_err(|_| LoadingError::BorrowingTables)?;
        let table = tables.get(name).ok_or(LoadingError::UnknownRelation)?;

        let columns: Vec<ColumnSchema> = table
            .columns
            .iter()
            .map(|(name, ty, modifiers)| ColumnSchema {
                name: name.to_string(),
                ty: ty.clone(),
                mods: modifiers.to_vec(),
            })
            .collect();
        let table_schema = TableSchema { rows: columns };

        let rows: Vec<Row> = table
            .rows
            .iter()
            .flat_map(|p| p)
            .filter(|row| {
                if (transaction.active.contains(&row.created)
                    || row.created > transaction.latest_commit
                    || transaction.aborted.contains(&row.created))
                    && row.created != transaction.id
                {
                    return false;
                }
                if row.expired != 0
                    && (!transaction.active.contains(&row.expired)
                        || row.expired == transaction.id
                        || row.expired < transaction.latest_commit)
                    && !transaction.aborted.contains(&row.expired)
                {
                    return false;
                }

                true
            })
            .map(|r| r.data.clone())
            .collect();

        let stream = futures::stream::iter(rows)
            .map(|r| RowCow::Owned(r))
            .boxed_local();

        Ok((table_schema, stream))
    }

    async fn relation_exists(
        &self,
        name: &str,
        _transaction: &Self::TransactionGuard,
    ) -> Result<bool, LoadingError> {
        Ok(self
            .tables
            .try_borrow()
            .map_err(|_| LoadingError::BorrowingTables)?
            .contains_key(name))
    }

    async fn create_relation(
        &self,
        name: &str,
        fields: Vec<(String, DataType, Vec<TypeModifier>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), LoadingError> {
        // TODO

        let mut tables_mut = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        tables_mut.insert(
            name.to_owned(),
            Table {
                columns: fields,
                rows: Vec::new(),
                cid: AtomicU64::new(0),
            },
        );

        Ok(())
    }

    async fn rename_relation(
        &self,
        name: &str,
        target: &str,
        _transaction: &Self::TransactionGuard,
    ) -> Result<(), LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        if tables.contains_key(target) {
            return Err(LoadingError::Other("Target Relation already exists"));
        }

        let data = match tables.remove(name) {
            Some(d) => d,
            None => return Err(LoadingError::Other("Source Relation does not exist")),
        };

        tables.insert(target.to_string(), data);

        Ok(())
    }

    async fn remove_relation(
        &self,
        name: &str,
        _transaction: &Self::TransactionGuard,
    ) -> Result<(), LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        tables.remove(name);

        Ok(())
    }

    async fn modify_relation(
        &self,
        name: &str,
        modification: crate::ModifyRelation,
        _transaction: &Self::TransactionGuard,
    ) -> Result<(), LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;
        let table = tables
            .get_mut(name)
            .ok_or(LoadingError::Other("Table does not exist"))?;

        for change in modification.modifications {
            match change {
                RelationModification::AddColumn {
                    name,
                    ty,
                    modifiers,
                } => {
                    table.columns.push((name, ty.clone(), modifiers.clone()));

                    let value = modifiers
                        .iter()
                        .find_map(|modifier| match modifier {
                            TypeModifier::DefaultValue { value } => {
                                value.as_ref().map(|v| Data::from_literal(v))
                            }
                            _ => None,
                        })
                        .unwrap_or(Data::Null);

                    for part in table.rows.iter_mut() {
                        for row in part.iter_mut() {
                            row.data.data.push(value.clone());
                        }
                    }
                }
                RelationModification::RenameColumn { from, to } => {
                    let column = table
                        .columns
                        .iter_mut()
                        .find(|(cn, _, _)| cn == &from)
                        .ok_or(LoadingError::Other("Column does not exist"))?;

                    column.0 = to;
                }
                RelationModification::ChangeType { name, ty } => {
                    let column = table
                        .columns
                        .iter_mut()
                        .find(|(cn, _, _)| cn == &name)
                        .ok_or(LoadingError::Other("Column does not exist"))?;

                    column.1 = ty;

                    // TODO
                    // Validate/change stored values
                }
                RelationModification::AddModifier { column, modifier } => {
                    let column = table
                        .columns
                        .iter_mut()
                        .find(|(cn, _, _)| cn == &column)
                        .ok_or(LoadingError::Other("Column does not exist"))?;

                    column.2.push(modifier);
                }
                RelationModification::RemoveModifier { column, modifier } => {
                    let column = table
                        .columns
                        .iter_mut()
                        .find(|(cn, _, _)| cn == &column)
                        .ok_or(LoadingError::Other("Column does not exist"))?;

                    column.2.retain(|m| m != &modifier);
                }
                RelationModification::SetColumnDefault { column, value } => {
                    // TODO
                    dbg!(&column, &value);
                }
            };
        }

        Ok(())
    }

    async fn schemas(&self) -> Result<Schemas, LoadingError> {
        let tables = self
            .tables
            .try_borrow()
            .map_err(|_| LoadingError::BorrowingTables)?;

        let res: HashMap<_, _> = tables
            .iter()
            .map(|(name, table)| {
                (
                    name.to_owned(),
                    TableSchema {
                        rows: table
                            .columns
                            .iter()
                            .map(|(cname, dtype, mods)| ColumnSchema {
                                name: cname.clone(),
                                ty: dtype.clone(),
                                mods: mods.clone(),
                            })
                            .collect::<Vec<_>>(),
                    },
                )
            })
            .collect();

        Ok(Schemas { tables: res })
    }

    async fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<Data>>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        let table = tables.get_mut(name).ok_or(LoadingError::UnknownRelation)?;

        for row in rows {
            let part = match table.rows.last_mut() {
                Some(part) if part.len() < 4092 => part,
                _ => {
                    table.rows.push(Vec::with_capacity(4092));
                    table.rows.last_mut().expect("")
                }
            };

            part.push(InternalRow {
                data: Row::new(table.cid.fetch_add(1, Ordering::SeqCst), row),
                created: transaction.id,
                expired: 0,
            });
        }

        Ok(())
    }

    async fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<Data>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), crate::RelationError<Self::LoadingError>> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| crate::RelationError::Inner(LoadingError::BorrowingTables))?;

        let table = tables
            .get_mut(name)
            .ok_or(crate::RelationError::Inner(LoadingError::UnknownRelation))?;

        tracing::info!("Rows in DB: {:?}", table.rows.len());

        for (row_id, new_values) in rows {
            let row = table
                .rows
                .iter_mut()
                .rev()
                .flat_map(|p| p)
                .filter(|r| r.expired == 0)
                .find(|row| row.data.id() == row_id)
                .ok_or(crate::RelationError::Inner(LoadingError::Other(
                    "Could not find Row",
                )))?;

            row.expired = transaction.id;

            let mut n_row = InternalRow {
                data: row.data.clone(),
                created: transaction.id,
                expired: 0,
            };
            n_row.data.rid = table.cid.fetch_add(1, Ordering::SeqCst);
            n_row.data.data = new_values;

            let part = match table.rows.last_mut() {
                Some(part) if part.len() < 4092 => part,
                _ => {
                    table.rows.push(Vec::with_capacity(4092));
                    table.rows.last_mut().expect("")
                }
            };
            part.push(n_row);
        }

        Ok(())
    }

    async fn delete_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = u64>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        let table = tables.get_mut(name).ok_or(LoadingError::UnknownRelation)?;

        for cid in rows {
            for row in table
                .rows
                .iter_mut()
                .flat_map(|p| p)
                .filter(|row| row.data.rid == cid)
            {
                assert_eq!(0, row.expired);
                row.expired = transaction.id;
            }
        }

        Ok(())
    }
}

impl Storage for &InMemoryStorage {}
