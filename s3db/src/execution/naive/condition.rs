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

#[derive(Debug, PartialEq)]
pub enum ConditionMapping<'expr, 'outer, 'placeholders, 'ctes> {
    And {
        parts: Vec<Self>,
    },
    Or {
        parts: Vec<Self>,
    },
    Value {
        mapping: CondValueMapping<'expr, 'outer, 'placeholders, 'ctes>,
    },
}

#[derive(Debug, PartialEq)]
pub enum CondValueMapping<'expr, 'outer, 'placeholders, 'ctes> {
    Attribute {
        idx: usize,
    },
    Constant {
        value: bool,
    },
    Comparison {
        first: Box<value::ValueMapper<'expr, 'outer, 'placeholders, 'ctes>>,
        second: Box<value::ValueMapper<'expr, 'outer, 'placeholders, 'ctes>>,
        comparison: ra::RaComparisonOperator,
    },
    Negation {
        inner: Box<Self>,
    },
    Exists {
        query: &'expr ra::RaExpression,
        columns: Vec<(String, sql::DataType, AttributeId)>,
        outer: &'outer HashMap<AttributeId, storage::Data>,
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
    },
}

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

pub async fn construct_condition<'rve, 'placeholders, 'c, 'o, 'f, SE>(
    condition: &'rve ra::RaCondition,
    columns: Vec<(String, DataType, AttributeId)>,
    placeholders: &'placeholders HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
) -> Result<ConditionMapping<'rve, 'o, 'placeholders, 'c>, EvaulateRaError<SE>>
where
    'rve: 'f,
    'placeholders: 'f,
    'c: 'f,
    'o: 'f,
{
    let mut pending = vec![condition];
    let mut instructions = Vec::new();

    while let Some(tmp) = pending.pop() {
        instructions.push(tmp);

        match tmp {
            ra::RaCondition::And(parts) => {
                pending.extend(parts.iter().rev());
            }
            ra::RaCondition::Or(parts) => {
                pending.extend(parts.iter().rev());
            }
            ra::RaCondition::Value(_) => {}
        };
    }

    let mut mappings = Vec::new();
    while let Some(instruction) = instructions.pop() {
        let mapping = match instruction {
            ra::RaCondition::And(parts) => {
                let tmp: Vec<_> = mappings
                    .drain(mappings.len() - parts.len()..)
                    .rev()
                    .collect();
                ConditionMapping::And { parts: tmp }
            }
            ra::RaCondition::Or(parts) => {
                let tmp: Vec<_> = mappings
                    .drain(mappings.len() - parts.len()..)
                    .rev()
                    .collect();
                ConditionMapping::Or { parts: tmp }
            }
            ra::RaCondition::Value(p) => ConditionMapping::Value {
                mapping: construct_value(p, &columns, placeholders, ctes, outer).await?,
            },
        };

        mappings.push(mapping);
    }

    mappings.pop().ok_or_else(|| EvaulateRaError::Other(""))
}

pub async fn construct_value<'rve, 'columns, 'placeholders, 'c, 'o, 'f, SE>(
    expr: &'rve ra::RaConditionValue,
    columns: &'columns [(String, DataType, AttributeId)],
    placeholders: &'placeholders HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
) -> Result<CondValueMapping<'rve, 'o, 'placeholders, 'c>, EvaulateRaError<SE>>
where
    'rve: 'f,
    'columns: 'f,
    'placeholders: 'f,
    'c: 'f,
    'o: 'f,
{
    let mut pending = vec![expr];
    let mut instructions = Vec::new();

    while let Some(pend) = pending.pop() {
        instructions.push(pend);

        match pend {
            ra::RaConditionValue::Constant { .. } => {}
            ra::RaConditionValue::Attribute { .. } => {}
            ra::RaConditionValue::Exists { .. } => {}
            ra::RaConditionValue::Negation { inner } => {
                pending.push(inner);
            }
            ra::RaConditionValue::Comparison { .. } => {}
        };
    }

    let mut results: Vec<CondValueMapping<'_, '_, '_, '_>> = Vec::new();
    while let Some(instruction) = instructions.pop() {
        let mapper = match instruction {
            ra::RaConditionValue::Constant { value } => {
                CondValueMapping::Constant { value: *value }
            }
            ra::RaConditionValue::Attribute { a_id, .. } => {
                let idx = columns
                    .iter()
                    .enumerate()
                    .find(|(_, (_, _, cid))| cid == a_id)
                    .map(|(i, _)| i)
                    .unwrap();

                CondValueMapping::Attribute { idx }
            }
            ra::RaConditionValue::Negation { .. } => {
                let inner_mapper = results.pop().unwrap();
                CondValueMapping::Negation {
                    inner: Box::new(inner_mapper),
                }
            }
            ra::RaConditionValue::Comparison {
                first,
                second,
                comparison,
            } => {
                let first_mapper =
                    value::construct(first, columns, placeholders, ctes, outer).await?;
                let second_mapper =
                    value::construct(second, columns, placeholders, ctes, outer).await?;

                CondValueMapping::Comparison {
                    first: Box::new(first_mapper),
                    second: Box::new(second_mapper),
                    comparison: comparison.clone(),
                }
            }
            ra::RaConditionValue::Exists { query } => CondValueMapping::Exists {
                query,
                columns: columns.to_vec(),
                outer,
                ctes,
                placeholders,
            },
        };

        results.push(mapper);
    }

    results.pop().ok_or_else(|| EvaulateRaError::Other(""))
}

impl<'expr, 'outer, 'placeholders, 'ctes> ConditionMapping<'expr, 'outer, 'placeholders, 'ctes> {
    pub async fn evaluate<'s, 'row, 'engine, 'transaction, 'arena, 'f, S>(
        &'s self,
        row: &'row storage::Row,
        engine: &'engine NaiveEngine<S>,
        transaction: &'transaction S::TransactionGuard,
        arena: &'arena bumpalo::Bump,
    ) -> Option<bool>
    where
        's: 'f,
        'row: 'f,
        'engine: 'f,
        'transaction: 'f,
        'arena: 'f,
        S: storage::Storage,
    {
        let mut pending = vec![(0, 0, self)];
        let mut instructions = Vec::new();
        let mut expressions = Vec::new();
        while let Some((src, idx, tmp)) = pending.pop() {
            match tmp {
                ConditionMapping::Or { parts } => {
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
                ConditionMapping::And { parts } => {
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
                ConditionMapping::Value { mapping } => {
                    expressions.push(mapping);
                    instructions.push(ConditionInstruction::Value { source: src, idx });
                }
            };
        }

        let mut results = Vec::new();
        while let Some(instruction) = instructions.pop() {
            let (src, instr_idx, outcome) = match instruction {
                ConditionInstruction::Value { source, idx } => {
                    let value = expressions.pop().unwrap();
                    let res = value.evaluate(row, engine, transaction, arena).await?;

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
                    return Some(outcome);
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

        Some(false)
    }
}

impl<'expr, 'outer, 'placeholders, 'ctes> CondValueMapping<'expr, 'outer, 'placeholders, 'ctes> {
    pub async fn evaluate<'s, 'row, 'engine, 'transaction, 'arena, 'f, S>(
        &'s self,
        row: &'row storage::Row,
        engine: &'engine NaiveEngine<S>,
        transaction: &'transaction S::TransactionGuard,
        arena: &'arena bumpalo::Bump,
    ) -> Option<bool>
    where
        's: 'f,
        'row: 'f,
        'engine: 'f,
        'transaction: 'f,
        'arena: 'f,
        S: storage::Storage,
    {
        let mut pending = vec![self];
        let mut instructions = Vec::new();

        while let Some(pend) = pending.pop() {
            instructions.push(pend);

            match pend {
                Self::Constant { .. }
                | Self::Attribute { .. }
                | Self::Exists { .. }
                | Self::Comparison { .. } => {}
                Self::Negation { inner } => {
                    pending.push(&inner);
                }
            };
        }

        let mut results: Vec<bool> = Vec::new();

        while let Some(tmp) = instructions.pop() {
            let res = match tmp {
                Self::Constant { value } => Some(*value),
                Self::Attribute { idx } => {
                    let tmp = row.data.get(*idx)?;
                    match tmp {
                        storage::Data::Boolean(v) => Some(*v),
                        other => {
                            dbg!(other);
                            None
                        }
                    }
                }
                Self::Negation { inner } => {
                    let before = results.pop()?;
                    Some(!before)
                }
                Self::Comparison {
                    first,
                    second,
                    comparison,
                } => {
                    let first_value = first.evaluate(row, engine, transaction, arena).await?;
                    let second_value = second.evaluate(row, engine, transaction, arena).await?;

                    match comparison {
                        ra::RaComparisonOperator::Equals => Some(first_value == second_value),
                        ra::RaComparisonOperator::NotEquals => Some(first_value != second_value),
                        ra::RaComparisonOperator::Greater => Some(first_value > second_value),
                        ra::RaComparisonOperator::GreaterEqual => Some(first_value >= second_value),
                        ra::RaComparisonOperator::Less => Some(first_value < second_value),
                        ra::RaComparisonOperator::LessEqual => Some(first_value <= second_value),
                        ra::RaComparisonOperator::In => {
                            dbg!(&first_value, &second_value);

                            let parts = match second_value {
                                Data::List(ps) => ps,
                                other => {
                                    dbg!(other);
                                    // return Err(EvaulateRaError::Other(
                                    //    "Attempting IN comparison on non List",
                                    //));
                                    return None;
                                }
                            };

                            Some(parts.iter().any(|p| p == &first_value))
                        }
                        ra::RaComparisonOperator::NotIn => match second_value {
                            storage::Data::List(d) => Some(d.iter().all(|v| v != &first_value)),
                            other => {
                                tracing::error!("Cannot perform NotIn Operation on not List Data");
                                dbg!(other);
                                // Err(EvaulateRaError::Other("Incompatible Error"))
                                None
                            }
                        },
                        ra::RaComparisonOperator::Like => {
                            let haystack = match first_value {
                                Data::Text(t) => t,
                                other => {
                                    tracing::warn!("Expected Text Data, got {:?}", other);
                                    // return Err(EvaulateRaError::Other("Wrong Type for Haystack"));
                                    return None;
                                }
                            };
                            let raw_pattern = match second_value {
                                Data::Text(p) => p,
                                other => {
                                    tracing::warn!("Expected Text Data, got {:?}", other);
                                    // return Err(EvaulateRaError::Other("Wrong Type for Pattern"));
                                    return None;
                                }
                            };

                            let result = pattern::like_match(&haystack, &raw_pattern);

                            Some(result)
                        }
                        ra::RaComparisonOperator::ILike => {
                            todo!("Perforing ILike Comparison")
                        }
                        ra::RaComparisonOperator::Is => match (first_value, second_value) {
                            (storage::Data::Boolean(val), storage::Data::Boolean(true)) => {
                                Some(val)
                            }
                            (storage::Data::Boolean(val), storage::Data::Boolean(false)) => {
                                Some(!val)
                            }
                            _ => Some(false),
                        },
                        ra::RaComparisonOperator::IsNot => {
                            dbg!(&first_value, &second_value);

                            // Err(EvaulateRaError::Other("Not implemented - IsNot Operator "))
                            None
                        }
                    }
                }
                Self::Exists {
                    query,
                    columns,
                    outer,
                    placeholders,
                    ctes,
                } => {
                    let n_outer = {
                        let mut tmp: HashMap<_, _> = (*outer).clone();

                        tmp.extend(
                            columns
                                .iter()
                                .map(|(_, _, id)| *id)
                                .zip(row.data.iter().cloned()),
                        );

                        tmp
                    };

                    let (_, mut rows) = engine
                        .evaluate_ra(&query, placeholders, ctes, &n_outer, transaction, arena)
                        .await
                        .ok()?;
                    let exists = rows.next().await.is_some();
                    Some(exists)
                }
            }?;
            results.push(res);
        }

        results.pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ra::{RaCondition, RaConditionValue};

    #[tokio::test]
    async fn construct_empty_and() {
        let condition = RaCondition::And(vec![]);

        let p = HashMap::new();
        let ctes = HashMap::new();
        let outer = HashMap::new();
        let constructed = construct_condition::<()>(&condition, Vec::new(), &p, &ctes, &outer)
            .await
            .unwrap();

        assert_eq!(ConditionMapping::And { parts: Vec::new() }, constructed);
    }

    #[tokio::test]
    async fn construct_and_two_values() {
        let condition = RaCondition::And(vec![
            RaCondition::Value(Box::new(RaConditionValue::Constant { value: true })),
            RaCondition::Value(Box::new(RaConditionValue::Constant { value: false })),
        ]);

        let p = HashMap::new();
        let ctes = HashMap::new();
        let outer = HashMap::new();
        let constructed = construct_condition::<()>(&condition, Vec::new(), &p, &ctes, &outer)
            .await
            .unwrap();

        assert_eq!(
            ConditionMapping::And {
                parts: vec![
                    ConditionMapping::Value {
                        mapping: CondValueMapping::Constant { value: true },
                    },
                    ConditionMapping::Value {
                        mapping: CondValueMapping::Constant { value: false },
                    }
                ]
            },
            constructed
        );
    }

    #[tokio::test]
    async fn construct_nested() {
        let condition = RaCondition::And(vec![
            RaCondition::Value(Box::new(RaConditionValue::Constant { value: true })),
            RaCondition::Or(vec![]),
            RaCondition::Value(Box::new(RaConditionValue::Constant { value: false })),
        ]);

        let p = HashMap::new();
        let ctes = HashMap::new();
        let outer = HashMap::new();
        let constructed = construct_condition::<()>(&condition, Vec::new(), &p, &ctes, &outer)
            .await
            .unwrap();

        assert_eq!(
            ConditionMapping::And {
                parts: vec![
                    ConditionMapping::Value {
                        mapping: CondValueMapping::Constant { value: true },
                    },
                    ConditionMapping::Or { parts: vec![] },
                    ConditionMapping::Value {
                        mapping: CondValueMapping::Constant { value: false },
                    }
                ]
            },
            constructed
        );
    }
}
