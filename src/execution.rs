use std::{fmt::Debug, future::Future};

use crate::{
    postgres::FormatCode,
    sql::{self, DataType},
    storage::{self, EntireRelation},
};

pub mod naive;

pub struct Context {
    pub transaction: Option<()>,
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
    Drop_,
}

#[derive(Debug)]
pub enum ExecuteError<PE, BE, EE> {
    Prepare(PE),
    Bind(BE),
    Execute(EE),
}

pub trait Execute {
    type Prepared: PreparedStatement;
    type PrepareError: Debug;
    type ExecuteBoundError: Debug;

    fn prepare<'q>(
        &self,
        query: &sql::Query<'q>,
        ctx: &mut Context,
    ) -> impl Future<Output = Result<Self::Prepared, Self::PrepareError>>;

    fn execute_bound(
        &self,
        query: &<Self::Prepared as PreparedStatement>::Bound,
        ctx: &mut Context,
    ) -> impl Future<Output = Result<ExecuteResult, Self::ExecuteBoundError>>;

    fn execute<'q>(
        &self,
        query: &sql::Query<'q>,
        ctx: &mut Context,
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
                .map_err(|e| ExecuteError::Prepare(e))?;

            // TODO
            // Can we just leave the result_formats empty or should we populate them
            let bound = prepared
                .bind(Vec::new(), Vec::new())
                .map_err(|e| ExecuteError::Bind(e))?;

            self.execute_bound(&bound, ctx)
                .await
                .map_err(|e| ExecuteError::Execute(e))
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

impl Context {
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
