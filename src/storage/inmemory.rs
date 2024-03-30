use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    sql::{DataType, TypeModifier},
    storage::{Data, EntireRelation, PartialRelation},
};

use super::{schema::ColumnSchema, RelationModification, Row, Schemas, Storage, TableSchema};

pub struct InMemoryStorage {
    tables: RefCell<HashMap<String, Table>>,
    current_tid: AtomicU64,
    active_tids: RefCell<HashSet<u64>>,
}

#[derive(Debug)]
pub struct InMemoryTransactionGuard {
    id: u64,
}

#[derive(Debug)]
pub enum LoadingError {
    BorrowingTables,
    UnknownRelation,
    Other(&'static str),
}

struct Table {
    columns: Vec<(String, DataType, Vec<TypeModifier>)>,
    rows: Vec<(Row, u64, u64)>,
    cid: AtomicU64,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            tables: RefCell::new(
                [
                    (
                        "pg_tables".to_string(),
                        Table {
                            columns: vec![
                                ("schemaname".to_string(), DataType::Name, vec![]),
                                ("tablename".to_string(), DataType::Name, vec![]),
                                ("tableowner".to_string(), DataType::Name, vec![]),
                                ("tablespace".to_string(), DataType::Name, vec![]),
                                ("hasindexes".to_string(), DataType::Bool, vec![]),
                                ("hasrules".to_string(), DataType::Bool, vec![]),
                                ("hastriggers".to_string(), DataType::Bool, vec![]),
                                ("rowsecurity".to_string(), DataType::Bool, vec![]),
                            ],
                            rows: Vec::new(),
                            cid: AtomicU64::new(0),
                        },
                    ),
                    (
                        "pg_indexes".to_string(),
                        Table {
                            columns: vec![
                                ("schemaname".to_string(), DataType::Name, vec![]),
                                ("tablename".to_string(), DataType::Name, vec![]),
                                ("indexname".to_string(), DataType::Name, vec![]),
                                ("tablespace".to_string(), DataType::Name, vec![]),
                                ("indexdef".to_string(), DataType::Text, vec![]),
                            ],
                            rows: Vec::new(),
                            cid: AtomicU64::new(0),
                        },
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            current_tid: AtomicU64::new(1),
            active_tids: RefCell::new(HashSet::new()),
        }
    }
}

impl Storage for InMemoryStorage {
    type LoadingError = LoadingError;
    type TransactionGuard = InMemoryTransactionGuard;

    async fn start_transaction(&self) -> Result<Self::TransactionGuard, Self::LoadingError> {
        <&InMemoryStorage as Storage>::start_transaction(&self).await
    }
    async fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::commit_transaction(&self, guard).await
    }

    async fn get_entire_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<EntireRelation, Self::LoadingError> {
        <&InMemoryStorage as Storage>::get_entire_relation(&self, name, transaction).await
    }

    async fn relation_exists(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<bool, Self::LoadingError> {
        <&InMemoryStorage as Storage>::relation_exists(&self, name, transaction).await
    }

    async fn create_relation(
        &self,
        name: &str,
        fields: Vec<(String, DataType, Vec<TypeModifier>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::create_relation(&self, name, fields, transaction).await
    }

    async fn rename_relation(
        &self,
        name: &str,
        target: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::rename_relation(&self, name, target, transaction).await
    }

    async fn remove_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::remove_relation(&self, name, transaction).await
    }

    async fn modify_relation(
        &self,
        name: &str,
        modification: super::ModifyRelation,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::modify_relation(&self, name, modification, transaction).await
    }

    async fn schemas(&self) -> Result<Schemas, Self::LoadingError> {
        <&InMemoryStorage as Storage>::schemas(&self).await
    }

    async fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<Data>>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::insert_rows(&self, name, rows, transaction).await
    }

    async fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<Data>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::update_rows(&self, name, rows, transaction).await
    }

    async fn delete_rows(
        &self,
        name: &str,
        rids: &mut dyn Iterator<Item = u64>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::delete_rows(&self, name, rids, transaction).await
    }
}

impl Storage for &InMemoryStorage {
    type LoadingError = LoadingError;
    type TransactionGuard = InMemoryTransactionGuard;

    async fn start_transaction(&self) -> Result<Self::TransactionGuard, Self::LoadingError> {
        let id = self.current_tid.fetch_add(1, Ordering::AcqRel);
        let mut active_ids = self.active_tids.try_borrow_mut().unwrap();
        active_ids.insert(id);

        Ok(InMemoryTransactionGuard { id })
    }

    async fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut active_ids = self.active_tids.try_borrow_mut().unwrap();
        active_ids.remove(&guard.id);

        Ok(())
    }

    async fn get_entire_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> Result<super::EntireRelation, LoadingError> {
        tracing::debug!("Getting Relation {:?}", name);

        let active_transactions = self.active_tids.try_borrow().unwrap();
        self.tables
            .try_borrow()
            .map_err(|_| LoadingError::BorrowingTables)?
            .get(name)
            .map(|table| EntireRelation {
                columns: table.columns.clone(),
                parts: vec![PartialRelation {
                    rows: table
                        .rows
                        .iter()
                        .filter(|(_, created, deleted)| {
                            if active_transactions.contains(created) && created != &transaction.id {
                                return false;
                            }
                            if *deleted != 0
                                && (!active_transactions.contains(deleted)
                                    || deleted == &transaction.id)
                            {
                                return false;
                            }

                            true
                        })
                        .map(|(r, _, _)| r.clone())
                        .collect(),
                }],
            })
            .ok_or(LoadingError::UnknownRelation)
    }

    async fn relation_exists(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
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

        let pg_tables = tables_mut.get_mut("pg_tables").unwrap();
        pg_tables.rows.push((
            Row::new(
                pg_tables.cid.fetch_add(1, Ordering::SeqCst),
                vec![
                    Data::Name("".to_string()),
                    Data::Name(name.to_string()),
                    Data::Name("".to_string()),
                    Data::Name("".to_string()),
                    Data::Boolean(false),
                    Data::Boolean(false),
                    Data::Boolean(false),
                    Data::Boolean(false),
                ],
            ),
            transaction.id,
            0,
        ));

        Ok(())
    }

    async fn rename_relation(
        &self,
        name: &str,
        target: &str,
        transaction: &Self::TransactionGuard,
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
        transaction: &Self::TransactionGuard,
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
        modification: super::ModifyRelation,
        transaction: &Self::TransactionGuard,
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
                            TypeModifier::DefaultValue { value } => match value {
                                Some(v) => Some(Data::from_literal(v)),
                                None => None,
                            },
                            _ => None,
                        })
                        .unwrap_or(Data::Null);

                    for row in table.rows.iter_mut() {
                        row.0.data.push(value.clone());
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
            table.rows.push((
                Row::new(table.cid.fetch_add(1, Ordering::SeqCst), row),
                transaction.id,
                0,
            ));
        }

        Ok(())
    }

    async fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<Data>)>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        let table = tables.get_mut(name).ok_or(LoadingError::UnknownRelation)?;

        for (row_id, new_values) in rows {
            let row = table
                .rows
                .iter_mut()
                .find(|(row, _, _)| row.id() == row_id)
                .ok_or(LoadingError::Other("Could not find Row"))?;

            row.2 = transaction.id;

            row.0.data = new_values;
            let n_row = row.0.clone();
            table.rows.push((n_row, transaction.id, 0));
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
            for (_, _, exp_id) in table.rows.iter_mut().filter(|(row, _, _)| row.rid == cid) {
                assert_eq!(0, *exp_id);
                *exp_id = transaction.id;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic() {
        let storage = InMemoryStorage::new();

        let transaction = storage.start_transaction().await.unwrap();

        let relation = storage
            .get_entire_relation("pg_tables", &transaction)
            .await
            .unwrap();
        assert_eq!(1, relation.parts.len());
        assert_eq!(0, relation.parts[0].rows.len());

        storage
            .create_relation("testing", Vec::new(), &transaction)
            .await
            .unwrap();

        let relation = storage
            .get_entire_relation("pg_tables", &transaction)
            .await
            .unwrap();
        assert_eq!(1, relation.parts.len());
        assert_eq!(1, relation.parts[0].rows.len());

        let entry = &relation.parts[0].rows[0];
        assert_eq!(
            &[
                Data::Name("".to_string()),
                Data::Name("testing".to_string()),
                Data::Name("".to_string()),
                Data::Name("".to_string()),
                Data::Boolean(false),
                Data::Boolean(false),
                Data::Boolean(false),
                Data::Boolean(false),
            ],
            entry.data.as_slice()
        );
    }
}
