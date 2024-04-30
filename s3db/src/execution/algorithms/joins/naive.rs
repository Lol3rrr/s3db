use futures::stream::StreamExt;

use crate::execution::naive::EvaulateRaError;

use super::{EvaluateConditions, Join, JoinArguments, JoinContext};

pub struct Naive {}

impl<CE, SE> Join<CE, SE> for Naive
where
    CE: EvaluateConditions<SE>,
{
    fn compatible(&self, _args: &JoinArguments<'_>, _ctx: &JoinContext) -> bool {
        true
    }

    async fn execute<'lr, 'rr>(
        &self,
        args: JoinArguments<'_>,
        _ctx: JoinContext,
        result_columns: Vec<storage::ColumnSchema>,
        mut left_result: futures::stream::LocalBoxStream<'lr, storage::RowCow<'_>>,
        right_result: futures::stream::LocalBoxStream<'rr, storage::RowCow<'_>>,
        condition_eval: &CE,
    ) -> Result<
        (
            storage::TableSchema,
            futures::stream::BoxStream<storage::Row>,
        ),
        EvaulateRaError<SE>,
    > {
        assert!(
            <Self as Join<CE, SE>>::compatible(self, &args, &_ctx),
            "Not compatible"
        );

        let right_rows: Vec<_> = right_result.collect().await;

        let result_schema = storage::TableSchema {
            rows: result_columns,
        };

        match args.kind {
            sql::JoinKind::Inner => {
                let mut result_rows = Vec::new();
                while let Some(left_row) = left_result.next().await {
                    for right_row in right_rows.iter() {
                        let joined_row_data = {
                            let mut tmp: Vec<storage::Data> = left_row.as_ref().to_vec();
                            tmp.extend(right_row.as_ref().iter().cloned());
                            tmp
                        };

                        if condition_eval.evaluate(args.conditon, &storage::RowCow::Borrowed(storage::BorrowedRow {
                            rid: result_rows.len() as u64,
                            data: &joined_row_data
                        })).await? {
                            result_rows.push(storage::Row::new(result_rows.len() as u64, joined_row_data));
                        }
                    }
                }

                Ok((result_schema, futures::stream::iter(result_rows).boxed()))
            }
            sql::JoinKind::LeftOuter => {
                let mut result_rows = Vec::new();
                while let Some(left_row) = left_result.next().await {
                    let mut included = false;

                    for right_row in right_rows.iter() {
                        let joined_row_data = {
                            let mut tmp: Vec<storage::Data> = left_row.as_ref().to_vec();
                            tmp.extend(right_row.as_ref().iter().cloned());
                            tmp
                        };


                        if condition_eval.evaluate(args.conditon, &storage::RowCow::Borrowed(storage::BorrowedRow {
                            rid: result_rows.len() as u64,
                            data: &joined_row_data
                        })).await? {
                            result_rows.push(storage::Row::new(result_rows.len() as u64, joined_row_data));
                            included = true;
                        }
                    }

                    if !included {
                        let joined_row_data = {
                            let mut tmp = left_row.as_ref().to_vec();
                            tmp.extend(
                                (0..(result_schema.rows.len() - tmp.len()))
                                    .map(|_| storage::Data::Null),
                            );
                            tmp
                        };

                        let row = storage::Row::new(result_rows.len() as u64, joined_row_data);

                        result_rows.push(row);
                    }
                }

                Ok((result_schema, futures::stream::iter(result_rows).boxed()))
            }
            other => {
                dbg!(other);

                Err(EvaulateRaError::Other("Unsupported Join Kind"))
            }
        }
    }
}
