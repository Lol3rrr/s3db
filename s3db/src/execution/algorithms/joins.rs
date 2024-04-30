use futures::Future;

use crate::{execution::naive::EvaulateRaError, ra::RaCondition};

mod naive;
pub use naive::Naive;

mod fallback;
pub use fallback::Fallback;

pub trait EvaluateConditions<SE> {
    fn evaluate(
        &self,
        condition: &RaCondition,
        row: &storage::RowCow<'_>,
    ) -> impl Future<Output = Result<bool, EvaulateRaError<SE>>>;
}

pub struct JoinArguments<'condition> {
    pub kind: sql::JoinKind,
    pub conditon: &'condition RaCondition,
}

pub struct JoinContext {}

pub trait Join<CE, SE>
where
    CE: EvaluateConditions<SE>,
{
    fn compatible(&self, args: &JoinArguments<'_>, ctx: &JoinContext) -> bool;

    fn execute<'lr, 'rr>(
        &self,
        args: JoinArguments<'_>,
        ctx: JoinContext,
        result_columns: Vec<storage::ColumnSchema>,
        left_rows: futures::stream::LocalBoxStream<'lr, storage::RowCow<'_>>,
        right_rows: futures::stream::LocalBoxStream<'rr, storage::RowCow<'_>>,
        condition_eval: &CE,
    ) -> impl Future<
        Output = Result<
            (
                storage::TableSchema,
                futures::stream::BoxStream<storage::Row>,
            ),
            EvaulateRaError<SE>,
        >,
    >;
}
