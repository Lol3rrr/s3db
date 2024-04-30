use core::{sync::atomic, cell::RefCell};
use std::{collections::HashMap, rc::Rc};

use crate::RelationStorage;

struct RelationList {
    head: atomic::AtomicPtr<RelationBlock>,
}

struct RelationBlock {
    next: atomic::AtomicPtr<Self>,
    position: atomic::AtomicUsize,
    slots: Vec<RelationSlot>,
}

struct RelationSlot {
    row_id: u64,
}

pub struct InMemoryStorage {
    relations: RefCell<HashMap<String, Rc<RelationList>>>
}

impl RelationStorage for InMemoryStorage {
    type LoadingError = ();
    type TransactionGuard = ();

    async fn schemas(&self) -> Result<crate::Schemas, Self::LoadingError> {
        todo!()
    }

    async fn start_transaction(
        &self,
    ) -> Result<Self::TransactionGuard, Self::LoadingError> {
        todo!()
    }

    async fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
    }
    async fn abort_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
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
        'own: 'rowdata {
        todo!()
    }

    async fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<crate::Data>>,
        transaction: &Self::TransactionGuard,
    ) -> Result<(), Self::LoadingError> {
        todo!()
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
