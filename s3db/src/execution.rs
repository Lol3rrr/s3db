//! All the execution related code

use std::{fmt::Debug, future::Future};

use crate::{
    postgres::FormatCode,
    storage::{self, EntireRelation},
};
use sql::DataType;

pub mod algorithms;

pub mod naive;

pub struct Context<T> {
    pub transaction: Option<T>,
}

#[derive(Debug, PartialEq)]
pub enum ExecuteResult {
    Set,
    Select {
        content: EntireRelation,
        formats: Vec<FormatCode>,
    },
    Insert {
        inserted_rows: usize,
        returning: Vec<Vec<storage::Data>>,
        formats: Vec<FormatCode>,
    },
    Update {
        updated_rows: usize,
    },
    Delete {
        deleted_rows: usize,
    },
    Create,
    Alter,
    Begin,
    Commit,
    Rollback,
    Drop_,
    Truncate,
    Vacuum,
}

#[derive(Debug)]
pub enum ExecuteError<PE, BE, EE> {
    Prepare(PE),
    Bind(BE),
    Execute(EE),
}

pub trait Execute<T> {
    type Prepared: PreparedStatement + 'static;
    type PrepareError: Debug;
    type ExecuteBoundError: Debug;

    type CopyState<'e>: CopyState
    where
        Self: 'e,
        T: 'e;

    fn prepare<'q, 'a>(
        &self,
        query: &sql::Query<'q, 'a>,
        ctx: &mut Context<T>,
    ) -> impl Future<Output = Result<Self::Prepared, Self::PrepareError>>;

    fn execute_bound(
        &self,
        query: &<Self::Prepared as PreparedStatement>::Bound,
        ctx: &mut Context<T>,
    ) -> impl Future<Output = Result<ExecuteResult, Self::ExecuteBoundError>>;

    fn start_copy<'s, 'e, 'c>(
        &'s self,
        table: &str,
        ctx: &'c mut Context<T>,
    ) -> impl Future<Output = Result<Self::CopyState<'e>, Self::ExecuteBoundError>>
    where
        's: 'e,
        'c: 'e;

    fn execute<'q, 'a>(
        &self,
        query: &sql::Query<'q, 'a>,
        ctx: &mut Context<T>,
    ) -> impl Future<
        Output = Result<
            ExecuteResult,
            ExecuteError<
                Self::PrepareError,
                <Self::Prepared as PreparedStatement>::BindError,
                Self::ExecuteBoundError,
            >,
        >,
    > {
        async {
            let prepared = self
                .prepare(query, ctx)
                .await
                .map_err(ExecuteError::Prepare)?;

            // TODO
            // Can we just leave the result_formats empty or should we populate them
            let bound = prepared
                .bind(Vec::new(), Vec::new())
                .map_err(ExecuteError::Bind)?;

            self.execute_bound(&bound, ctx)
                .await
                .map_err(ExecuteError::Execute)
        }
    }
}

pub trait PreparedStatement {
    type Bound;
    type BindError: Debug;

    fn bind(
        &self,
        values: Vec<Vec<u8>>,
        result_formats: Vec<FormatCode>,
    ) -> Result<Self::Bound, Self::BindError>;

    fn parameters(&self) -> Vec<DataType>;

    fn row_columns(&self) -> Vec<(String, DataType)>;
}

pub trait CopyState {
    fn columns(&self) -> Vec<()>;

    fn insert(&mut self, raw_column: &[u8]) -> impl Future<Output = Result<(), ()>>;
}

impl<T> Default for Context<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Context<T> {
    pub fn new() -> Self {
        Self { transaction: None }
    }

    pub fn transaction_state(&self) -> u8 {
        match self.transaction.as_ref() {
            Some(_) => b'T',
            None => b'I',
        }
    }
}
