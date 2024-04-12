use std::{fmt::Debug, future::Future};

use sql::{DataType, Literal, TypeModifier};

mod data;
pub use data::{Data, RealizeError};

mod schema;
pub use schema::{ColumnSchema, Schemas, TableSchema};

pub mod inmemory;

#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    rid: u64,
    pub data: Vec<Data>,
}

#[derive(Debug, PartialEq)]
pub struct PartialRelation {
    pub rows: Vec<Row>,
}

#[derive(Debug, PartialEq)]
pub struct EntireRelation {
    pub columns: Vec<(String, DataType, Vec<TypeModifier>)>,
    pub parts: Vec<PartialRelation>,
}

pub struct ModifyRelation {
    modifications: Vec<RelationModification>,
}

#[derive(Debug, PartialEq)]
pub enum RelationModification {
    AddColumn {
        name: String,
        ty: DataType,
        modifiers: Vec<TypeModifier>,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    ChangeType {
        name: String,
        ty: DataType,
    },
    RemoveModifier {
        column: String,
        modifier: TypeModifier,
    },
    SetColumnDefault {
        column: String,
        value: Literal<'static>,
    },
}

pub trait Storage {
    type LoadingError: Debug;
    type TransactionGuard: Debug;

    fn start_transaction(
        &self,
    ) -> impl Future<Output = Result<Self::TransactionGuard, Self::LoadingError>>;

    fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn abort_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn get_entire_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<EntireRelation, Self::LoadingError>>;

    fn relation_exists(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<bool, Self::LoadingError>>;

    fn create_relation(
        &self,
        name: &str,
        fields: Vec<(String, DataType, Vec<TypeModifier>)>,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn rename_relation(
        &self,
        name: &str,
        target: &str,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn remove_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn modify_relation(
        &self,
        name: &str,
        modification: ModifyRelation,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn schemas(&self) -> impl Future<Output = Result<Schemas, Self::LoadingError>>;

    fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<Data>>,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<Data>)>,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;

    fn delete_rows(
        &self,
        name: &str,
        rids: &mut dyn Iterator<Item = u64>,
        transaction: &Self::TransactionGuard,
    ) -> impl Future<Output = Result<(), Self::LoadingError>>;
}

impl Row {
    pub fn new(rid: u64, data: Vec<Data>) -> Self {
        Self { rid, data }
    }

    pub fn id(&self) -> u64 {
        self.rid
    }
}

impl Default for ModifyRelation {
    fn default() -> Self {
        Self::new()
    }
}

impl ModifyRelation {
    pub fn new() -> Self {
        Self {
            modifications: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column: &str, ty: DataType, modifiers: Vec<TypeModifier>) {
        self.modifications.push(RelationModification::AddColumn {
            name: column.to_string(),
            ty,
            modifiers,
        });
    }

    pub fn change_type(&mut self, column: &str, ty: DataType) {
        self.modifications.push(RelationModification::ChangeType {
            name: column.to_string(),
            ty,
        });
    }

    pub fn remove_modifier(&mut self, column: &str, modifier: TypeModifier) {
        self.modifications
            .push(RelationModification::RemoveModifier {
                column: column.to_string(),
                modifier,
            });
    }

    pub fn rename_column(&mut self, from: &str, to: &str) {
        self.modifications.push(RelationModification::RenameColumn {
            from: from.into(),
            to: to.into(),
        });
    }

    pub fn set_default(&mut self, column: &str, value: Literal<'static>) {
        self.modifications
            .push(RelationModification::SetColumnDefault {
                column: column.into(),
                value,
            });
    }
}

impl EntireRelation {
    pub fn into_rows(self) -> impl Iterator<Item = Row> {
        self.parts.into_iter().flat_map(|p| p.rows.into_iter())
    }
}
