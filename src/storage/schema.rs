use std::collections::HashMap;

use crate::sql::{DataType, TypeModifier};

#[derive(Debug, PartialEq, Clone)]
pub struct Schemas {
    pub tables: HashMap<String, TableSchema>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableSchema {
    pub rows: Vec<ColumnSchema>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub ty: DataType,
    pub mods: Vec<TypeModifier>,
}

impl From<HashMap<String, Vec<(String, DataType)>>> for Schemas {
    fn from(value: HashMap<String, Vec<(String, DataType)>>) -> Self {
        Self {
            tables: value
                .into_iter()
                .map(|(name, rows)| {
                    (
                        name,
                        TableSchema {
                            rows: rows
                                .into_iter()
                                .map(|(n, ty)| ColumnSchema {
                                    name: n,
                                    ty,
                                    mods: Vec::new(),
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl FromIterator<(String, Vec<(String, DataType)>)> for Schemas {
    fn from_iter<T: IntoIterator<Item = (String, Vec<(String, DataType)>)>>(iter: T) -> Self {
        Schemas::from_iter(iter.into_iter().map(
            |(name, rows): (String, Vec<(String, DataType)>)| {
                (
                    name,
                    rows.into_iter()
                        .map(|(n, ty)| (n, ty, Vec::new()))
                        .collect::<Vec<(_, _, _)>>(),
                )
            },
        ))
    }
}

impl FromIterator<(String, Vec<(String, DataType, Vec<TypeModifier>)>)> for Schemas {
    fn from_iter<T: IntoIterator<Item = (String, Vec<(String, DataType, Vec<TypeModifier>)>)>>(
        iter: T,
    ) -> Self {
        Self {
            tables: iter
                .into_iter()
                .map(|(name, rows)| {
                    (
                        name,
                        TableSchema {
                            rows: rows
                                .into_iter()
                                .map(|(n, ty, mods)| ColumnSchema { name: n, ty, mods })
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl Schemas {
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }
}

impl TableSchema {
    pub fn get_column_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.rows.iter().find(|c| c.name == name)
    }
}
