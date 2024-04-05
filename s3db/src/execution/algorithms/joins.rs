use futures::Future;

use crate::{execution::naive::EvaulateRaError, ra::RaCondition, storage};

mod naive;
pub use naive::Naive;

mod fallback;
pub use fallback::Fallback;

pub trait EvaluateConditions<SE> {
    fn evaluate(
        &self,
        condition: &RaCondition,
        row: &storage::Row,
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

    fn execute(
        &self,
        args: JoinArguments<'_>,
        ctx: JoinContext,
        result_columns: Vec<(String, sql::DataType, Vec<sql::TypeModifier>)>,
        left_result: storage::EntireRelation,
        right_result: storage::EntireRelation,
        condition_eval: &CE,
    ) -> impl Future<Output = Result<storage::EntireRelation, EvaulateRaError<SE>>>;
}
