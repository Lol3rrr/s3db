use super::{pattern, value, EvaulateRaError, NaiveEngine};

use futures::{
    future::{FutureExt, LocalBoxFuture},
    stream::StreamExt,
};
use std::collections::HashMap;

use crate::{
    ra::{self, AttributeId},
    storage::{self, Data},
};
use sql::DataType;

#[derive(Debug)]
enum ConditionInstruction {
    And {
        count: usize,
        source: usize,
        idx: usize,
    },
    Or {
        count: usize,
        source: usize,
        idx: usize,
    },
    Value {
        source: usize,
        idx: usize,
    },
}

pub async fn evaluate_ra_cond<'s, 'cond, 'r, 'col, 'p, 'c, 'o, 't, 'f, 'arena, S>(
    engine: &'s NaiveEngine<S>,
    condition: &'cond ra::RaCondition,
    row: &'r storage::Row,
    columns: &'col [(String, DataType, AttributeId)],
    placeholders: &'p HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
    transaction: &'t S::TransactionGuard,
    arena: &'arena bumpalo::Bump,
) -> Result<bool, EvaulateRaError<S::LoadingError>>
where
    's: 'f,
    'cond: 'f,
    'r: 'f,
    'col: 'f,
    'p: 'f,
    'c: 'f,
    'o: 'f,
    't: 'f,
    'arena: 'f,
    S: crate::storage::Storage,
{
    let mut pending = vec![(0, 0, condition)];
    let mut instructions = Vec::new();
    let mut expressions = Vec::new();
    while let Some((src, idx, tmp)) = pending.pop() {
        match tmp {
            ra::RaCondition::Or(parts) => {
                instructions.push(ConditionInstruction::Or {
                    count: parts.len(),
                    source: src,
                    idx,
                });
                pending.extend(
                    parts
                        .iter()
                        .enumerate()
                        .rev()
                        .map(|(idx, p)| (instructions.len(), idx, p)),
                );
            }
            ra::RaCondition::And(parts) => {
                instructions.push(ConditionInstruction::And {
                    count: parts.len(),
                    source: src,
                    idx,
                });
                pending.extend(
                    parts
                        .iter()
                        .enumerate()
                        .rev()
                        .map(|(idx, p)| (instructions.len(), idx, p)),
                );
            }
            ra::RaCondition::Value(val) => {
                expressions.push(val);
                instructions.push(ConditionInstruction::Value { source: src, idx });
            }
        };
    }

    let mut results = Vec::new();
    while let Some(instruction) = instructions.pop() {
        let (src, instr_idx, outcome) = match instruction {
            ConditionInstruction::Value { source, idx } => {
                let value = expressions.pop().unwrap();
                let res = evaluate_ra_cond_val(
                    engine,
                    value,
                    row,
                    columns,
                    placeholders,
                    ctes,
                    outer,
                    transaction,
                    &arena,
                )
                .await?;

                (source, idx, res)
            }
            ConditionInstruction::And { count, source, idx } => {
                let mut outcome = true;
                for _ in 0..count {
                    outcome = outcome && results.pop().unwrap();
                }

                (source, idx, outcome)
            }
            ConditionInstruction::Or { count, source, idx } => {
                let mut outcome = false;
                for _ in 0..count {
                    outcome = outcome || results.pop().unwrap();
                }

                (source, idx, outcome)
            }
        };

        let src_index = match src.checked_sub(1) {
            Some(idx) => idx,
            None => {
                return Ok(outcome);
            }
        };
        match instructions.get(src_index).unwrap() {
            ConditionInstruction::And { count, .. } => {
                if outcome {
                    results.push(true);
                } else {
                    let count = *count;

                    let removed_instructions = instructions.drain(src_index + 1..);
                    for instr in removed_instructions {
                        match instr {
                            ConditionInstruction::Value { .. } => {
                                expressions.pop();
                            }
                            _ => {}
                        };
                    }

                    results.push(false);
                    results.extend(core::iter::repeat(false).take(count - instr_idx - 1));
                }
            }
            ConditionInstruction::Or { count, .. } => {
                if !outcome {
                    results.push(false);
                } else {
                    let count = *count;

                    let removed_instructions = instructions.drain(src_index + 1..);
                    for instr in removed_instructions {
                        match instr {
                            ConditionInstruction::Value { .. } => {
                                expressions.pop();
                            }
                            _ => {}
                        };
                    }

                    results.push(true);
                    results.extend(core::iter::repeat(true).take(count - instr_idx - 1));
                }
            }
            ConditionInstruction::Value { .. } => unreachable!(),
        };
    }

    Ok(false)
}

pub fn evaluate_ra_cond_val<'engine, 'cond, 'r, 'col, 'p, 'c, 'o, 't, 'f, 'arena, S>(
    engine: &'engine NaiveEngine<S>,
    condition: &'cond ra::RaConditionValue,
    row: &'r storage::Row,
    columns: &'col [(String, DataType, AttributeId)],
    placeholders: &'p HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
    transaction: &'t S::TransactionGuard,
    arena: &'arena bumpalo::Bump,
) -> LocalBoxFuture<'f, Result<bool, EvaulateRaError<S::LoadingError>>>
where
    'engine: 'f,
    'cond: 'f,
    'r: 'f,
    'col: 'f,
    'p: 'f,
    'c: 'f,
    'o: 'f,
    't: 'f,
    'arena: 'f,
    S: storage::Storage,
{
    async move {
        match condition {
            ra::RaConditionValue::Constant { value } => Ok(*value),
            ra::RaConditionValue::Attribute { name, a_id, .. } => {
                let data_result = row
                    .data
                    .iter()
                    .zip(columns.iter())
                    .find(|(_, column)| &column.2 == a_id)
                    .map(|(d, _)| d);

                match data_result {
                    Some(storage::Data::Boolean(v)) => Ok(*v),
                    Some(d) => {
                        dbg!(&d);
                        Err(EvaulateRaError::Other("Invalid Data Type"))
                    }
                    None => Err(EvaulateRaError::UnknownAttribute {
                        name: name.clone(),
                        id: *a_id,
                    }),
                }
            }
            ra::RaConditionValue::Comparison {
                first,
                second,
                comparison,
            } => {
                let first_value = value::evaluate_ra_value(
                    engine,
                    first,
                    row,
                    columns,
                    placeholders,
                    ctes,
                    outer,
                    transaction,
                    &arena,
                )
                .await?;
                let second_value = value::evaluate_ra_value(
                    engine,
                    second,
                    row,
                    columns,
                    placeholders,
                    ctes,
                    outer,
                    transaction,
                    &arena,
                )
                .await?;

                match comparison {
                    ra::RaComparisonOperator::Equals => Ok(first_value == second_value),
                    ra::RaComparisonOperator::NotEquals => Ok(first_value != second_value),
                    ra::RaComparisonOperator::Greater => Ok(first_value > second_value),
                    ra::RaComparisonOperator::GreaterEqual => Ok(first_value >= second_value),
                    ra::RaComparisonOperator::Less => Ok(first_value < second_value),
                    ra::RaComparisonOperator::LessEqual => Ok(first_value <= second_value),
                    ra::RaComparisonOperator::In => {
                        dbg!(&first_value, &second_value);

                        let parts = match second_value {
                            Data::List(ps) => ps,
                            other => {
                                dbg!(other);
                                return Err(EvaulateRaError::Other(
                                    "Attempting IN comparison on non List",
                                ));
                            }
                        };

                        Ok(parts.iter().any(|p| p == &first_value))
                    }
                    ra::RaComparisonOperator::NotIn => match second_value {
                        storage::Data::List(d) => Ok(d.iter().all(|v| v != &first_value)),
                        other => {
                            tracing::error!("Cannot perform NotIn Operation on not List Data");
                            dbg!(other);
                            Err(EvaulateRaError::Other("Incompatible Error"))
                        }
                    },
                    ra::RaComparisonOperator::Like => {
                        let haystack = match first_value {
                            Data::Text(t) => t,
                            other => {
                                tracing::warn!("Expected Text Data, got {:?}", other);
                                return Err(EvaulateRaError::Other("Wrong Type for Haystack"));
                            }
                        };
                        let raw_pattern = match second_value {
                            Data::Text(p) => p,
                            other => {
                                tracing::warn!("Expected Text Data, got {:?}", other);
                                return Err(EvaulateRaError::Other("Wrong Type for Pattern"));
                            }
                        };

                        let result = pattern::like_match(&haystack, &raw_pattern);

                        Ok(result)
                    }
                    ra::RaComparisonOperator::ILike => {
                        todo!("Perforing ILike Comparison")
                    }
                    ra::RaComparisonOperator::Is => match (first_value, second_value) {
                        (storage::Data::Boolean(val), storage::Data::Boolean(true)) => Ok(val),
                        (storage::Data::Boolean(val), storage::Data::Boolean(false)) => Ok(!val),
                        _ => Ok(false),
                    },
                    ra::RaComparisonOperator::IsNot => {
                        dbg!(&first_value, &second_value);

                        Err(EvaulateRaError::Other("Not implemented - IsNot Operator "))
                    }
                }
            }
            ra::RaConditionValue::Negation { inner } => {
                let inner_result = evaluate_ra_cond_val(
                    engine,
                    inner,
                    row,
                    columns,
                    placeholders,
                    ctes,
                    outer,
                    transaction,
                    &arena,
                )
                .await?;

                Ok(!inner_result)
            }
            ra::RaConditionValue::Exists { query } => {
                let n_tmp = {
                    let mut tmp = outer.clone();
                    tmp.extend(
                        columns
                            .iter()
                            .zip(row.data.iter())
                            .map(|(column, data)| (column.2, data.clone())),
                    );
                    tmp
                };

                let res = match engine
                    .evaluate_ra(query, placeholders, ctes, &n_tmp, transaction, &arena)
                    .await
                {
                    Ok((_, mut rows)) => Ok(rows.next().await.is_some()),
                    Err(e) => panic!("{:?}", e),
                };
                res
            }
        }
    }
    .boxed_local()
}
