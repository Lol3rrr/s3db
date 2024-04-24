use super::{EvaluateConditions, Join};

pub struct Fallback<P, F> {
    primary: P,
    fallback: F,
}

impl<P, F> Fallback<P, F> {
    pub fn new(primary: P, fallback: F) -> Self {
        Self { primary, fallback }
    }
}

impl<P, F, CE, SE> Join<CE, SE> for Fallback<P, F>
where
    CE: EvaluateConditions<SE>,
    P: Join<CE, SE>,
    F: Join<CE, SE>,
{
    fn compatible(&self, args: &super::JoinArguments<'_>, ctx: &super::JoinContext) -> bool {
        self.primary.compatible(args, ctx) || self.fallback.compatible(args, ctx)
    }

    async fn execute<'lr, 'rr>(
        &self,
        args: super::JoinArguments<'_>,
        ctx: super::JoinContext,
        result_columns: Vec<storage::ColumnSchema>,
        left_result: futures::stream::LocalBoxStream<'lr, storage::Row>,
        right_result: futures::stream::LocalBoxStream<'rr, storage::Row>,
        condition_eval: &CE,
    ) -> Result<
        (
            storage::TableSchema,
            futures::stream::BoxStream<storage::Row>,
        ),
        crate::execution::naive::EvaulateRaError<SE>,
    > {
        assert!(
            <Self as Join<CE, SE>>::compatible(self, &args, &ctx),
            "Not compatible"
        );

        if self.primary.compatible(&args, &ctx) {
            self.primary
                .execute(
                    args,
                    ctx,
                    result_columns,
                    left_result,
                    right_result,
                    condition_eval,
                )
                .await
        } else {
            self.fallback
                .execute(
                    args,
                    ctx,
                    result_columns,
                    left_result,
                    right_result,
                    condition_eval,
                )
                .await
        }
    }
}
