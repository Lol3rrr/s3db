use crate::{execution::naive::EvaulateRaError, storage};

use super::{EvaluateConditions, Join, JoinArguments, JoinContext};

pub struct Naive {}

impl<CE, SE> Join<CE, SE> for Naive
where
    CE: EvaluateConditions<SE>,
{
    fn compatible(&self, _args: &JoinArguments<'_>, _ctx: &JoinContext) -> bool {
        true
    }

    async fn execute(
        &self,
        args: JoinArguments<'_>,
        _ctx: JoinContext,
        result_columns: Vec<(String, sql::DataType, Vec<sql::TypeModifier>)>,
        left_result: storage::EntireRelation,
        right_result: storage::EntireRelation,
        condition_eval: &CE,
    ) -> Result<storage::EntireRelation, EvaulateRaError<SE>> {
        assert!(
            <Self as Join<CE, SE>>::compatible(self, &args, &_ctx),
            "Not compatible"
        );

        match args.kind {
            sql::JoinKind::Inner => {
                let mut result_rows = Vec::new();
                for left_row in left_result
                    .parts
                    .into_iter()
                    .flat_map(|p| p.rows.into_iter())
                {
                    for right_row in right_result.parts.iter().flat_map(|p| p.rows.iter()) {
                        let joined_row_data = {
                            let mut tmp = left_row.data.clone();
                            tmp.extend(right_row.data.clone());
                            tmp
                        };

                        let row = storage::Row::new(result_rows.len() as u64, joined_row_data);
                        if condition_eval.evaluate(&args.conditon, &row).await? {
                            result_rows.push(row);
                        }
                    }
                }

                Ok(storage::EntireRelation {
                    columns: result_columns,
                    parts: vec![storage::PartialRelation { rows: result_rows }],
                })
            }
            sql::JoinKind::LeftOuter => {
                let mut result_rows = Vec::new();
                for left_row in left_result
                    .parts
                    .into_iter()
                    .flat_map(|p| p.rows.into_iter())
                {
                    let mut included = false;

                    for right_row in right_result.parts.iter().flat_map(|p| p.rows.iter()) {
                        let joined_row_data = {
                            let mut tmp = left_row.data.clone();
                            tmp.extend(right_row.data.clone());
                            tmp
                        };

                        let row = storage::Row::new(result_rows.len() as u64, joined_row_data);

                        if condition_eval.evaluate(&args.conditon, &row).await? {
                            result_rows.push(row);
                            included = true;
                        }
                    }

                    if !included {
                        let joined_row_data = {
                            let mut tmp = left_row.data.clone();
                            tmp.extend(
                                (0..(result_columns.len() - tmp.len()))
                                    .map(|_| storage::Data::Null),
                            );
                            tmp
                        };

                        let row = storage::Row::new(result_rows.len() as u64, joined_row_data);

                        result_rows.push(row);
                    }
                }

                Ok(storage::EntireRelation {
                    columns: result_columns,
                    parts: vec![storage::PartialRelation { rows: result_rows }],
                })
            }
            other => {
                dbg!(other);

                return Err(EvaulateRaError::Other("Unsupported Join Kind"));
            }
        }
    }
}
