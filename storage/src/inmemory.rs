pub mod v1;

mod v2;

pub type LoadingError = v1::LoadingError;
pub type InMemoryStorage = v1::InMemoryStorage;
pub type InMemorySequence<'a> = v1::InMemorySequence<'a>;
pub type InMemoryTransactionGuard = v1::InMemoryTransactionGuard;
