use std::collections::HashMap;

use sql::{DataType, TypeModifier};

/// Stores the Schemas for all relation stored
#[derive(Debug, PartialEq, Clone)]
pub struct Schemas {
    pub tables: HashMap<String, TableSchema>,
}

/// The Schema for a single Relation/Table
#[derive(Debug, PartialEq, Clone)]
pub struct TableSchema {
    pub rows: Vec<ColumnSchema>,
}

/// The Schema for a single column in a relation
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
    fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.rows.iter().find(|c| c.name == name)
    }

    pub fn apply_mod(&mut self, modification: &crate::RelationModification) {
        match modification {
            crate::RelationModification::AddColumn {
                name,
                ty,
                modifiers,
            } => {
                self.rows.push(ColumnSchema {
                    name: name.clone(),
                    ty: ty.clone(),
                    mods: modifiers.clone(),
                });
            }
            crate::RelationModification::RenameColumn { from, to } => {
                match self.rows.iter_mut().find(|c| &c.name == from) {
                    Some(c) => {
                        c.name = to.clone();
                    }
                    None => {
                        // TODO
                        // What to do?
                    }
                };
            }
            crate::RelationModification::ChangeType { name, ty } => {
                match self.rows.iter_mut().find(|c| &c.name == name) {
                    Some(c) => {
                        c.ty = ty.clone();
                    }
                    None => {
                        // TODO
                        // What to do?
                    }
                };
            }
            crate::RelationModification::SetColumnDefault { column, value } => {
                match self.rows.iter_mut().find(|c| &c.name == column) {
                    Some(c) => {
                        match c.mods.iter_mut().find_map(|m| match m {
                            TypeModifier::DefaultValue { value } => Some(value),
                            _ => None,
                        }) {
                            Some(prev) => {
                                *prev = Some(value.clone());
                            }
                            None => {
                                c.mods.push(TypeModifier::DefaultValue {
                                    value: Some(value.clone()),
                                });
                            }
                        };
                    }
                    None => {
                        // TODO
                        // What to do?
                    }
                };
            }
            crate::RelationModification::AddModifier { column, modifier } => {
                match self.rows.iter_mut().find(|c| &c.name == column) {
                    Some(c) => {
                        c.mods.push(modifier.clone());
                    }
                    None => {
                        // TODO
                        // What to do?
                    }
                };
            }
            crate::RelationModification::RemoveModifier { column, modifier } => {
                match self.rows.iter_mut().find(|c| &c.name == column) {
                    Some(c) => {
                        c.mods.retain(|m| m != modifier);
                    }
                    None => {
                        // TODO
                        // What to do?
                    }
                };
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::RelationModification;

    #[test]
    fn add_column() {
        let mut schema = TableSchema::new();
        assert_eq!(&[] as &[ColumnSchema], &schema.rows);

        schema.apply_mod(&RelationModification::AddColumn {
            name: "testing".to_owned(),
            ty: DataType::Integer,
            modifiers: Vec::new(),
        });

        assert_eq!(
            &[ColumnSchema {
                name: "testing".to_owned(),
                ty: DataType::Integer,
                mods: Vec::new(),
            }] as &[ColumnSchema],
            &schema.rows
        );
    }

    #[test]
    fn rename_column() {
        let mut schema = TableSchema::new();
        assert_eq!(&[] as &[ColumnSchema], &schema.rows);

        schema.rows.push(ColumnSchema {
            name: "test".into(),
            ty: DataType::Integer,
            mods: Vec::new(),
        });

        schema.apply_mod(&RelationModification::RenameColumn {
            from: "test".into(),
            to: "something".into(),
        });

        assert_eq!(
            &[ColumnSchema {
                name: "something".to_owned(),
                ty: DataType::Integer,
                mods: Vec::new(),
            }] as &[ColumnSchema],
            &schema.rows
        );
    }

    #[test]
    fn change_type() {
        let mut schema = TableSchema::new();
        assert_eq!(&[] as &[ColumnSchema], &schema.rows);

        schema.rows.push(ColumnSchema {
            name: "test".into(),
            ty: DataType::Integer,
            mods: Vec::new(),
        });

        schema.apply_mod(&RelationModification::ChangeType {
            name: "test".into(),
            ty: DataType::BigInteger,
        });

        assert_eq!(
            &[ColumnSchema {
                name: "test".to_owned(),
                ty: DataType::BigInteger,
                mods: Vec::new(),
            }] as &[ColumnSchema],
            &schema.rows
        );
    }

    #[test]
    fn set_column_default() {
        let mut schema = TableSchema::new();
        assert_eq!(&[] as &[ColumnSchema], &schema.rows);

        schema.rows.push(ColumnSchema {
            name: "test".into(),
            ty: DataType::Integer,
            mods: Vec::new(),
        });

        schema.apply_mod(&RelationModification::SetColumnDefault {
            column: "test".into(),
            value: sql::Literal::Integer(2),
        });

        assert_eq!(
            &[ColumnSchema {
                name: "test".to_owned(),
                ty: DataType::Integer,
                mods: vec![TypeModifier::DefaultValue {
                    value: Some(sql::Literal::Integer(2)),
                }],
            }] as &[ColumnSchema],
            &schema.rows
        );
    }

    #[test]
    fn set_column_default_overwrite() {
        let mut schema = TableSchema::new();
        assert_eq!(&[] as &[ColumnSchema], &schema.rows);

        schema.rows.push(ColumnSchema {
            name: "test".into(),
            ty: DataType::Integer,
            mods: vec![TypeModifier::DefaultValue {
                value: Some(sql::Literal::Integer(1)),
            }],
        });

        schema.apply_mod(&RelationModification::SetColumnDefault {
            column: "test".into(),
            value: sql::Literal::Integer(2),
        });

        assert_eq!(
            &[ColumnSchema {
                name: "test".to_owned(),
                ty: DataType::Integer,
                mods: vec![TypeModifier::DefaultValue {
                    value: Some(sql::Literal::Integer(2)),
                }],
            }] as &[ColumnSchema],
            &schema.rows
        );
    }

    #[test]
    fn add_modifier() {
        let mut schema = TableSchema::new();
        assert_eq!(&[] as &[ColumnSchema], &schema.rows);

        schema.rows.push(ColumnSchema {
            name: "test".into(),
            ty: DataType::Integer,
            mods: Vec::new(),
        });

        schema.apply_mod(&RelationModification::AddModifier {
            column: "test".into(),
            modifier: TypeModifier::PrimaryKey,
        });

        assert_eq!(
            &[ColumnSchema {
                name: "test".to_owned(),
                ty: DataType::Integer,
                mods: vec![TypeModifier::PrimaryKey],
            }] as &[ColumnSchema],
            &schema.rows
        );
    }

    #[test]
    fn remove_modifier() {
        let mut schema = TableSchema::new();
        assert_eq!(&[] as &[ColumnSchema], &schema.rows);

        schema.rows.push(ColumnSchema {
            name: "test".into(),
            ty: DataType::Integer,
            mods: vec![TypeModifier::PrimaryKey],
        });

        schema.apply_mod(&RelationModification::RemoveModifier {
            column: "test".into(),
            modifier: TypeModifier::PrimaryKey,
        });

        assert_eq!(
            &[ColumnSchema {
                name: "test".to_owned(),
                ty: DataType::Integer,
                mods: vec![],
            }] as &[ColumnSchema],
            &schema.rows
        );
    }
}
