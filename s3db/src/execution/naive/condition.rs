use super::{pattern, value, EvaulateRaError};

use futures::{future::FutureExt, stream::StreamExt};
use std::collections::HashMap;

use crate::ra::{self, AttributeId};

use sql::DataType;
use storage::{self, Data};

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

    async fn evaluate_mut<S>(
        &mut self,
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
                let first_value = first.evaluate_mut(row, engine, transaction, arena).await?;
                let second_value = second.evaluate_mut(row, engine, transaction, arena).await?;

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

    async fn evaluate_mut<S>(
        &mut self,
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
                let mut values = result_stack.drain(stack_len - *part_count..);
                Some(values.all(|v| v))
            }
            Self::Or { part_count } => {
                let stack_len = result_stack.len();
                let mut values = result_stack.drain(stack_len - *part_count..);
                Some(values.any(|v| v))
            }
            Self::Expression(expr) => expr.evaluate_mut(row, engine, transaction, arena).await,
        }
    }

    // TODO
    // Add support for short-circuiting
}

pub type Mapper<'expr, 'outer, 'placeholders, 'ctes> =
    super::mapping::Mapper<CondInstruction<'expr, 'outer, 'placeholders, 'ctes>, bool>;
