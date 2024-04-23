use super::{pattern, value, EvaulateRaError, NaiveEngine};

use futures::{future::FutureExt, stream::StreamExt};
use std::collections::HashMap;

use crate::{
    ra::{self, AttributeId},
    storage::{self, Data},
};
use sql::DataType;

#[derive(Debug, PartialEq)]
pub enum CondExprInstruction<'expr, 'outer, 'placeholders, 'ctes> {
    Attribute {
        idx: usize,
    },
    Constant {
        value: bool,
    },
    Comparison {
        first: value::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        second: value::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        comparison: ra::RaComparisonOperator,
    },
    Negation {},
    Exists {
        query: &'expr ra::RaExpression,
        columns: Vec<(String, sql::DataType, AttributeId)>,
        outer: &'outer HashMap<AttributeId, storage::Data>,
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
    },
}

impl<'expr, 'outer, 'placeholders, 'ctes> super::mapping::MappingInstruction<'expr>
    for CondExprInstruction<'expr, 'outer, 'placeholders, 'ctes>
{
    type Input = ra::RaConditionValue;
    type Output = bool;
    type ConstructContext<'ctx> = (
        &'ctx [(String, DataType, AttributeId)],
        &'placeholders HashMap<usize, storage::Data>,
        &'ctes HashMap<String, storage::EntireRelation>,
        &'outer HashMap<AttributeId, storage::Data>,
    );

    fn push_nested(input: &'expr Self::Input, pending: &mut Vec<&'expr Self::Input>) {
        match input {
            ra::RaConditionValue::Negation { inner } => {
                pending.push(inner);
            }
            ra::RaConditionValue::Exists { .. } => {}
            ra::RaConditionValue::Comparison { .. } => {}
            ra::RaConditionValue::Attribute { .. } => {}
            ra::RaConditionValue::Constant { .. } => {}
        }
    }

    fn construct<'ctx, SE>(
        input: &'expr Self::Input,
        (columns, placeholders, ctes, outer): &Self::ConstructContext<'ctx>,
    ) -> Result<Self, EvaulateRaError<SE>> {
        match input {
            ra::RaConditionValue::Negation { .. } => Ok(Self::Negation {}),
            ra::RaConditionValue::Constant { value } => Ok(Self::Constant { value: *value }),
            ra::RaConditionValue::Attribute { a_id, .. } => {
                let idx = columns
                    .iter()
                    .enumerate()
                    .find(|(_, (_, _, cid))| cid == a_id)
                    .map(|(i, _)| i)
                    .unwrap();

                Ok(Self::Attribute { idx })
            }
            ra::RaConditionValue::Comparison {
                first,
                second,
                comparison,
            } => {
                let first_mapper =
                    value::Mapper::construct(first, (columns, placeholders, ctes, outer))?;
                let second_mapper =
                    value::Mapper::construct(second, (columns, placeholders, ctes, outer))?;

                Ok(Self::Comparison {
                    first: first_mapper,
                    second: second_mapper,
                    comparison: comparison.clone(),
                })
            }
            ra::RaConditionValue::Exists { query } => Ok(Self::Exists {
                query,
                outer,
                ctes,
                placeholders,
                columns: columns.to_vec(),
            }),
        }
    }

    async fn evaluate<S>(
        &self,
        result_stack: &mut Vec<Self::Output>,
        row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> Option<Self::Output>
    where
        S: storage::Storage,
    {
        match self {
            Self::Negation {} => {
                let before = result_stack.pop()?;
                Some(!before)
            }
            Self::Constant { value } => Some(*value),
            Self::Attribute { idx } => match row.data.get(*idx) {
                Some(storage::Data::Boolean(v)) => Some(*v),
                _ => None,
            },
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
                        (storage::Data::Boolean(val), storage::Data::Boolean(true)) => Some(val),
                        (storage::Data::Boolean(val), storage::Data::Boolean(false)) => Some(!val),
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

                let local_fut = async {
                    let (_, mut rows) = engine
                        .evaluate_ra(&query, placeholders, ctes, &n_outer, transaction, arena)
                        .await
                        .ok()?;
                    Some(rows.next().await.is_some())
                }
                .boxed_local();
                let exists = local_fut.await?;
                Some(exists)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum CondInstruction<'expr, 'outer, 'placeholders, 'ctes> {
    And {
        part_count: usize,
    },
    Or {
        part_count: usize,
    },
    Expression(
        super::mapping::Mapper<CondExprInstruction<'expr, 'outer, 'placeholders, 'ctes>, bool>,
    ),
}

impl<'expr, 'outer, 'placeholders, 'ctes> super::mapping::MappingInstruction<'expr>
    for CondInstruction<'expr, 'outer, 'placeholders, 'ctes>
{
    type Input = ra::RaCondition;
    type Output = bool;
    type ConstructContext<'ctx> = (
        &'ctx [(String, DataType, AttributeId)],
        &'placeholders HashMap<usize, storage::Data>,
        &'ctes HashMap<String, storage::EntireRelation>,
        &'outer HashMap<AttributeId, storage::Data>,
    );

    fn push_nested(input: &'expr Self::Input, pending: &mut Vec<&'expr Self::Input>) {
        match input {
            ra::RaCondition::And(parts) => {
                pending.extend(parts.iter().rev());
            }
            ra::RaCondition::Or(parts) => {
                pending.extend(parts.iter().rev());
            }
            ra::RaCondition::Value(_) => {}
        }
    }

    fn construct<'ctx, SE>(
        input: &'expr Self::Input,
        ctx: &Self::ConstructContext<'ctx>,
    ) -> Result<Self, EvaulateRaError<SE>> {
        match input {
            ra::RaCondition::And(parts) => Ok(Self::And {
                part_count: parts.len(),
            }),
            ra::RaCondition::Or(parts) => Ok(Self::Or {
                part_count: parts.len(),
            }),
            ra::RaCondition::Value(v) => {
                let value = super::mapping::Mapper::construct(v.as_ref(), *ctx)?;
                Ok(Self::Expression(value))
            }
        }
    }

    async fn evaluate<S>(
        &self,
        result_stack: &mut Vec<Self::Output>,
        row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> Option<Self::Output>
    where
        S: storage::Storage,
    {
        match self {
            Self::And { part_count } => {
                let stack_len = result_stack.len();
                let mut values = result_stack.drain(stack_len - part_count..);
                Some(values.all(|v| v))
            }
            Self::Or { part_count } => {
                let stack_len = result_stack.len();
                let mut values = result_stack.drain(stack_len - part_count..);
                Some(values.any(|v| v))
            }
            Self::Expression(expr) => expr.evaluate(row, engine, transaction, arena).await,
        }
    }

    // TODO
    // Add support for short-circuiting
}

pub type Mapper<'expr, 'outer, 'placeholders, 'ctes> =
    super::mapping::Mapper<CondInstruction<'expr, 'outer, 'placeholders, 'ctes>, bool>;

#[derive(Debug, PartialEq)]
pub struct ConditionMapper<'expr, 'outer, 'placeholders, 'ctes> {
    start: ConditionMapping<'expr, 'outer, 'placeholders, 'ctes>,
    len: usize,
}

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
        first: value::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        second: value::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
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

pub async fn construct_condition<'rve, 'placeholders, 'c, 'o, 'f, SE>(
    condition: &'rve ra::RaCondition,
    columns: Vec<(String, DataType, AttributeId)>,
    placeholders: &'placeholders HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
) -> Result<ConditionMapper<'rve, 'o, 'placeholders, 'c>, EvaulateRaError<SE>>
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

    let max_size = instructions.len();

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

    let root = mappings.pop().ok_or_else(|| EvaulateRaError::Other(""))?;

    Ok(ConditionMapper {
        start: root,
        len: max_size,
    })
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
                    value::Mapper::construct(first, (columns, placeholders, ctes, outer))?;
                let second_mapper =
                    value::Mapper::construct(second, (columns, placeholders, ctes, outer))?;

                CondValueMapping::Comparison {
                    first: first_mapper,
                    second: second_mapper,
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

impl<'expr, 'outer, 'placeholders, 'ctes> ConditionMapper<'expr, 'outer, 'placeholders, 'ctes> {
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
        let mut pending = Vec::with_capacity(self.len);
        pending.push((0, 0, &self.start));
        let mut instructions = Vec::with_capacity(self.len);
        let mut expressions = Vec::with_capacity(self.len);
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

        let mut results = Vec::with_capacity(instructions.len());
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
        let mut pending = Vec::with_capacity(16);
        pending.push(self);
        let mut instructions = Vec::with_capacity(16);

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

        let mut results: Vec<bool> = Vec::with_capacity(instructions.len());

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
                Self::Negation { .. } => {
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

                    let local_fut = async {
                        let (_, mut rows) = engine
                            .evaluate_ra(&query, placeholders, ctes, &n_outer, transaction, arena)
                            .await
                            .ok()?;
                        Some(rows.next().await.is_some())
                    }
                    .boxed_local();
                    let exists = local_fut.await?;
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

        assert_eq!(
            ConditionMapping::And { parts: Vec::new() },
            constructed.start
        );
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
            constructed.start
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
            constructed.start
        );
    }
}
