use std::{
    cell::RefCell,
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    sql::{DataType, TypeModifier},
    storage::{Data, EntireRelation, PartialRelation},
};

use super::{schema::ColumnSchema, RelationModification, Row, Schemas, Storage, TableSchema};

pub struct InMemoryStorage {
    tables: RefCell<HashMap<String, Table>>,
}

#[derive(Debug)]
pub enum LoadingError {
    BorrowingTables,
    UnknownRelation,
    Other(&'static str),
}

struct Table {
    columns: Vec<(String, DataType, Vec<TypeModifier>)>,
    rows: Vec<Row>,
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
        }
    }
}

impl Storage for InMemoryStorage {
    type LoadingError = LoadingError;

    async fn get_entire_relation(&self, name: &str) -> Result<EntireRelation, Self::LoadingError> {
        <&InMemoryStorage as Storage>::get_entire_relation(&self, name).await
    }

    async fn relation_exists(&self, name: &str) -> Result<bool, Self::LoadingError> {
        <&InMemoryStorage as Storage>::relation_exists(&self, name).await
    }

    async fn create_relation(
        &self,
        name: &str,
        fields: Vec<(String, DataType, Vec<TypeModifier>)>,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::create_relation(&self, name, fields).await
    }

    async fn rename_relation(&self, name: &str, target: &str) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::rename_relation(&self, name, target).await
    }

    async fn remove_relation(&self, name: &str) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::remove_relation(&self, name).await
    }

    async fn modify_relation(
        &self,
        name: &str,
        modification: super::ModifyRelation,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::modify_relation(&self, name, modification).await
    }

    async fn schemas(&self) -> Result<Schemas, Self::LoadingError> {
        <&InMemoryStorage as Storage>::schemas(&self).await
    }

    async fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<Data>>,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::insert_rows(&self, name, rows).await
    }

    async fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<Data>)>,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::update_rows(&self, name, rows).await
    }

    async fn delete_rows(
        &self,
        name: &str,
        rids: &mut dyn Iterator<Item = u64>,
    ) -> Result<(), Self::LoadingError> {
        <&InMemoryStorage as Storage>::delete_rows(&self, name, rids).await
    }
}

impl Storage for &InMemoryStorage {
    type LoadingError = LoadingError;

    async fn get_entire_relation(&self, name: &str) -> Result<super::EntireRelation, LoadingError> {
        tracing::debug!("Getting Relation {:?}", name);

        self.tables
            .try_borrow()
            .map_err(|_| LoadingError::BorrowingTables)?
            .get(name)
            .map(|table| EntireRelation {
                columns: table.columns.clone(),
                parts: vec![PartialRelation {
                    rows: table.rows.clone(),
                }],
            })
            .ok_or(LoadingError::UnknownRelation)
    }

    async fn relation_exists(&self, name: &str) -> Result<bool, LoadingError> {
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
        pg_tables.rows.push(Row::new(
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
        ));

        Ok(())
    }

    async fn rename_relation(&self, name: &str, target: &str) -> Result<(), LoadingError> {
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

    async fn remove_relation(&self, name: &str) -> Result<(), LoadingError> {
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
                        row.data.push(value.clone());
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
    ) -> Result<(), LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        let table = tables.get_mut(name).ok_or(LoadingError::UnknownRelation)?;

        for row in rows {
            table
                .rows
                .push(Row::new(table.cid.fetch_add(1, Ordering::SeqCst), row));
        }

        Ok(())
    }

    async fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<Data>)>,
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
                .find(|row| row.id() == row_id)
                .ok_or(LoadingError::Other("Could not find Row"))?;

            row.data = new_values;
        }

        Ok(())
    }

    async fn delete_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = u64>,
    ) -> Result<(), LoadingError> {
        let mut tables = self
            .tables
            .try_borrow_mut()
            .map_err(|_| LoadingError::BorrowingTables)?;

        let table = tables.get_mut(name).ok_or(LoadingError::UnknownRelation)?;

        for cid in rows {
            table.rows.retain(|row| row.rid != cid);
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

        let relation = storage.get_entire_relation("pg_tables").await.unwrap();
        assert_eq!(1, relation.parts.len());
        assert_eq!(0, relation.parts[0].rows.len());

        storage
            .create_relation("testing", Vec::new())
            .await
            .unwrap();

        let relation = storage.get_entire_relation("pg_tables").await.unwrap();
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
