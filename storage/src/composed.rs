use crate::{RelationStorage, SequenceStorage, Storage};

pub struct ComposedStorage<RS, SS> {
    relations: RS,
    sequences: SS,
}

impl<RS, SS> ComposedStorage<RS, SS> {
    pub fn new(relations: RS, sequences: SS) -> Self {
        Self {
            relations,
            sequences,
        }
    }
}

impl<RS, SS> SequenceStorage for ComposedStorage<RS, SS>
where
    SS: SequenceStorage,
{
    type SequenceHandle<'s> = SS::SequenceHandle<'s> where Self: 's;

    fn get_sequence<'se, 'seq>(
        &'se self,
        name: &str,
    ) -> impl futures::prelude::Future<Output = Result<Option<Self::SequenceHandle<'seq>>, ()>>
    where
        'se: 'seq,
    {
        self.sequences.get_sequence(name)
    }

    fn create_sequence(
        &self,
        name: &str,
    ) -> impl futures::prelude::Future<Output = Result<(), ()>> {
        self.sequences.create_sequence(name)
    }

    fn remove_sequence(
        &self,
        name: &str,
    ) -> impl futures::prelude::Future<Output = Result<(), ()>> {
        self.sequences.remove_sequence(name)
    }
}

impl<RS, SS> RelationStorage for ComposedStorage<RS, SS>
where
    RS: RelationStorage,
{
    type LoadingError = RS::LoadingError;
    type TransactionGuard = RS::TransactionGuard;

    fn schemas(
        &self,
    ) -> impl futures::prelude::Future<Output = Result<crate::Schemas, Self::LoadingError>> {
        RS::schemas(&self.relations)
    }

    fn insert_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = Vec<crate::Data>>,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::insert_rows(&self.relations, name, rows, transaction)
    }
    fn update_rows(
        &self,
        name: &str,
        rows: &mut dyn Iterator<Item = (u64, Vec<crate::Data>)>,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::update_rows(&self.relations, name, rows, transaction)
    }
    fn delete_rows(
        &self,
        name: &str,
        rids: &mut dyn Iterator<Item = u64>,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::delete_rows(&self.relations, name, rids, transaction)
    }

    fn stream_relation<'own, 'name, 'transaction, 'stream, 'rowdata>(
        &'own self,
        name: &'name str,
        transaction: &'transaction Self::TransactionGuard,
    ) -> impl futures::prelude::Future<
        Output = Result<
            (
                crate::TableSchema,
                futures::stream::LocalBoxStream<'stream, crate::RowCow<'rowdata>>,
            ),
            Self::LoadingError,
        >,
    >
    where
        'own: 'stream,
        'name: 'stream,
        'transaction: 'stream,
        'own: 'rowdata,
    {
        RS::stream_relation(&self.relations, name, transaction)
    }

    fn relation_exists(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<bool, Self::LoadingError>> {
        RS::relation_exists(&self.relations, name, transaction)
    }
    fn create_relation(
        &self,
        name: &str,
        fields: std::vec::Vec<(String, sql::DataType, Vec<sql::TypeModifier>)>,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::create_relation(&self.relations, name, fields, transaction)
    }
    fn rename_relation(
        &self,
        name: &str,
        target: &str,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::rename_relation(&self.relations, name, target, transaction)
    }
    fn remove_relation(
        &self,
        name: &str,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::remove_relation(&self.relations, name, transaction)
    }
    fn modify_relation(
        &self,
        name: &str,
        modification: crate::ModifyRelation,
        transaction: &Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::modify_relation(&self.relations, name, modification, transaction)
    }

    fn start_transaction(
        &self,
    ) -> impl futures::prelude::Future<Output = Result<Self::TransactionGuard, Self::LoadingError>>
    {
        RS::start_transaction(&self.relations)
    }
    fn commit_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::commit_transaction(&self.relations, guard)
    }
    fn abort_transaction(
        &self,
        guard: Self::TransactionGuard,
    ) -> impl futures::prelude::Future<Output = Result<(), Self::LoadingError>> {
        RS::abort_transaction(&self.relations, guard)
    }
}

impl<RS, SS> Storage for ComposedStorage<RS, SS>
where
    RS: RelationStorage,
    SS: SequenceStorage,
{
}
