mod v1;
pub use v1::{InMemoryStorage as InMemoryStorageV1, InMemorySequence as InMemorySequenceV1, InMemoryTransactionGuard as InMemoryTransactionGuardV1, LoadingError as LoadingErrorV1};

pub type LoadingError = LoadingErrorV1;
pub type InMemoryStorage = InMemoryStorageV1;
pub type InMemorySequence<'a> = InMemorySequenceV1<'a>;
pub type InMemoryTransactionGuard = InMemoryTransactionGuardV1;
