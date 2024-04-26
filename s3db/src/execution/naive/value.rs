use futures::{future::FutureExt, stream::StreamExt};
use std::collections::HashMap;

use crate::ra::{self, AttributeId};

use sql::{BinaryOperator, DataType};
use storage::{self, Data, Sequence};

use super::EvaulateRaError;

impl<'expr, 'outer, 'placeholders, 'ctes> super::mapping::MappingInstruction<'expr>
    for ValueInstruction<'expr, 'outer, 'placeholders, 'ctes>
{
    type Input = ra::RaValueExpression;
    type Output = storage::Data;
    type ConstructContext<'ctx> = (
        &'ctx [(String, DataType, AttributeId)],
        &'placeholders HashMap<usize, storage::Data>,
        &'ctes HashMap<String, storage::EntireRelation>,
        &'outer HashMap<AttributeId, storage::Data>,
    );

    fn push_nested(input: &'expr Self::Input, pending: &mut Vec<&'expr Self::Input>) {
        match input {
            ra::RaValueExpression::Attribute { .. } => {}
            ra::RaValueExpression::OuterAttribute { .. } => {}
            ra::RaValueExpression::Placeholder(_) => {}
            ra::RaValueExpression::Literal(_) => {}
            ra::RaValueExpression::Renamed { value, .. } => {
                pending.push(&value);
            }
            ra::RaValueExpression::Cast { inner, .. } => {
                pending.push(&inner);
            }
            ra::RaValueExpression::List(parts) => {
                pending.extend(parts.iter().rev());
            }
            ra::RaValueExpression::BinaryOperation { first, second, .. } => {
                pending.push(second);
                pending.push(first);
            }
            ra::RaValueExpression::Function(func) => match func {
                ra::RaFunction::Lower(l) => {
                    pending.push(l);
                }
                ra::RaFunction::Substr {
                    str_value,
                    start,
                    count,
                } => {
                    if let Some(c) = count.as_ref() {
                        pending.push(c);
                    }
                    pending.push(&start);
                    pending.push(&str_value);
                }
                ra::RaFunction::LeftPad { base, .. } => {
                    pending.push(&base);
                }
                ra::RaFunction::Coalesce(v) => {
                    pending.extend(v.iter().rev());
                }
                ra::RaFunction::SetValue { value, .. } => {
                    pending.push(&value);
                }
                ra::RaFunction::ArrayPosition { array, target } => {
                    pending.push(&target);
                    pending.push(&array);
                }
                ra::RaFunction::CurrentSchemas { .. } => {}
            },
            ra::RaValueExpression::SubQuery { .. } => {}
        }
    }

    fn construct<'ctx, SE>(
        input: &'expr Self::Input,
        (columns, placeholders, ctes, outer): &Self::ConstructContext<'ctx>,
    ) -> Result<Self, EvaulateRaError<SE>> {
        match input {
            ra::RaValueExpression::Attribute { a_id, name, .. } => {
                // TODO

                let column_index = columns
                    .iter()
                    .enumerate()
                    .find(|(_, (_, _, id))| id == a_id)
                    .map(|(i, _)| i)
                    .ok_or_else(|| EvaulateRaError::UnknownAttribute {
                        name: name.clone(),
                        id: *a_id,
                    })?;

                Ok(ValueInstruction::Attribute { idx: column_index })
            }
            ra::RaValueExpression::OuterAttribute { a_id, name, .. } => {
                dbg!(&a_id, &outer);

                let value = outer
                    .get(a_id)
                    .ok_or_else(|| EvaulateRaError::UnknownAttribute {
                        name: name.clone(),
                        id: *a_id,
                    })?;

                Ok(ValueInstruction::Constant {
                    value: value.clone(),
                })
            }
            ra::RaValueExpression::Placeholder(placeholder) => {
                let value = placeholders.get(placeholder).cloned().ok_or_else(|| {
                    dbg!(&placeholders, &placeholder);
                    EvaulateRaError::Other("Getting Placeholder Value")
                })?;

                Ok(ValueInstruction::Constant { value })
            }
            ra::RaValueExpression::Literal(lit) => {
                let res = storage::Data::from_literal(lit);

                Ok(ValueInstruction::Constant { value: res })
            }
            ra::RaValueExpression::List(elems) => Ok(ValueInstruction::List { len: elems.len() }),
            ra::RaValueExpression::SubQuery { query } => Ok(ValueInstruction::SubQuery {
                query,
                outer,
                placeholders,
                ctes,
            }),
            ra::RaValueExpression::Cast { target, .. } => Ok(ValueInstruction::Cast {
                target: target.clone(),
            }),
            ra::RaValueExpression::BinaryOperation { operator, .. } => {
                Ok(ValueInstruction::BinaryOp {
                    operator: operator.clone(),
                })
            }
            ra::RaValueExpression::Function(fc) => {
                let func = match fc {
                    ra::RaFunction::LeftPad {
                        base,
                        length,
                        padding,
                    } => {
                        dbg!(&base, &length, &padding);

                        Err(EvaulateRaError::Other("Executing LeftPad not implemented"))
                    }
                    ra::RaFunction::Coalesce(parts) => {
                        dbg!(&parts);

                        Err(EvaulateRaError::Other("Executing Coalesce"))
                    }
                    ra::RaFunction::SetValue {
                        name,
                        is_called,
                        ..
                    } => Ok(FunctionInstruction::SetValue {
                        name: name.clone(),
                        is_called: *is_called,
                    }),
                    ra::RaFunction::Lower(val) => {
                        dbg!(&val);

                        Ok(FunctionInstruction::Lower {})
                    }
                    ra::RaFunction::Substr {
                        str_value,
                        start,
                        count,
                    } => {
                        dbg!(&str_value, &start, count);

                        Err(EvaulateRaError::Other("Executing Substr function"))
                    }
                    ra::RaFunction::CurrentSchemas { implicit } => {
                        dbg!(implicit);

                        Err(EvaulateRaError::Other("Executing Current Schemas"))
                    }
                    ra::RaFunction::ArrayPosition { array, target } => {
                        dbg!(&array, &target);

                        Err(EvaulateRaError::Other("Executing ArrayPosition"))
                    }
                }?;

                Ok(ValueInstruction::Function { func })
            }
            ra::RaValueExpression::Renamed { name, value } => {
                dbg!(&name, &value);

                Err(EvaulateRaError::Other("Renamed Value Expression"))
            }
        }
    }

    async fn evaluate<S>(
        &self,
        value_stack: &mut Vec<storage::Data>,
        row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> Option<storage::Data>
    where
        S: storage::Storage,
    {
        match self {
            ValueInstruction::Constant { value } => Some(value.clone()),
            ValueInstruction::Attribute { idx } => Some(row.data[*idx].clone()),
            ValueInstruction::Cast { target } => {
                let input = value_stack.pop()?;
                input.try_cast(target).ok()
            }
            ValueInstruction::List { len } => {
                let stack_len = value_stack.len();
                let values: Vec<_> = value_stack.drain(stack_len - len..).collect();
                Some(storage::Data::List(values))
            }
            ValueInstruction::BinaryOp { operator } => {
                let first_value = value_stack.pop()?;
                let second_value = value_stack.pop()?;

                match operator {
                    BinaryOperator::Add => match (first_value, second_value) {
                        (Data::SmallInt(f), Data::SmallInt(s)) => Some(Data::SmallInt(f + s)),
                        (Data::Integer(f), Data::SmallInt(s)) => Some(Data::Integer(f + s as i32)),
                        other => {
                            dbg!(other);
                            // Err(EvaulateRaError::Other("Addition"))
                            None
                        }
                    },
                    BinaryOperator::Subtract => match (first_value, second_value) {
                        (Data::Integer(f), Data::Integer(s)) => Some(Data::Integer(f - s)),
                        other => {
                            dbg!(other);
                            // Err(EvaulateRaError::Other("Subtracting"))
                            None
                        }
                    },
                    BinaryOperator::Divide => match (first_value, second_value) {
                        (Data::Integer(f), Data::Integer(s)) => Some(Data::Integer(f / s)),
                        other => {
                            dbg!(other);
                            // Err(EvaulateRaError::Other("Subtracting"))
                            None
                        }
                    },
                    other => {
                        dbg!(&other, first_value, second_value);

                        // Err(EvaulateRaError::Other("Evaluating Binary Operator"))
                        None
                    }
                }
            }
            ValueInstruction::SubQuery {
                query,
                outer,
                placeholders,
                ctes,
            } => {
                let n_outer = {
                    let tmp: HashMap<_, _> = (*outer).clone();

                    // TODO

                    tmp
                };

                let local_fut = async {
                    let (_, rows) = engine
                        .evaluate_ra(&query, placeholders, ctes, &n_outer, transaction, &arena)
                        .await
                        .ok()?;

                    let parts: Vec<_> = rows.map(|r| r.data[0].clone()).collect().await;
                    Some(parts)
                }
                .boxed_local();

                let parts = local_fut.await?;
                Some(storage::Data::List(parts))
            }
            ValueInstruction::Function { func } => {
                match func {
                    FunctionInstruction::SetValue { name, is_called } => {
                        let mut value = value_stack.pop()?;

                        let value = loop {
                            match value {
                                Data::BigInt(v) => break v,
                                Data::Integer(v) => break v as i64,
                                Data::SmallInt(v) => break v as i64,
                                Data::List(mut v) => {
                                    if v.is_empty() {
                                        // return Err(EvaulateRaError::Other(""));
                                        return None;
                                    }

                                    value = v.swap_remove(0);
                                    continue;
                                }
                                other => todo!("Other: {:?}", other),
                            };
                        };

                        let sequence = engine.storage.get_sequence(name).await.unwrap().unwrap();
                        if *is_called {
                            sequence.set_value(1 + value as u64).await;
                        } else {
                            sequence.set_value(value as u64).await;
                        }

                        Some(Data::BigInt(value))
                    }
                    FunctionInstruction::Lower {} => {
                        let value = value_stack.pop()?;

                        match value {
                            storage::Data::Text(d) => Some(storage::Data::Text(d.to_lowercase())),
                            other => {
                                dbg!(&other);
                                //Err(EvaulateRaError::Other("Unexpected Type"))
                                None
                            }
                        }
                    }
                }
            }
        }
    }
}

pub type Mapper<'expr, 'outer, 'placeholders, 'ctes> =
    super::mapping::Mapper<ValueInstruction<'expr, 'outer, 'placeholders, 'ctes>, storage::Data>;

#[derive(Debug, PartialEq)]
pub enum ValueInstruction<'expr, 'outer, 'placeholders, 'ctes> {
    Attribute {
        idx: usize,
    },
    Constant {
        value: storage::Data,
    },
    List {
        len: usize,
    },
    Cast {
        target: sql::DataType,
    },
    BinaryOp {
        operator: BinaryOperator,
    },
    Function {
        func: FunctionInstruction,
    },
    SubQuery {
        query: &'expr ra::RaExpression,
        outer: &'outer HashMap<AttributeId, storage::Data>,
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
    },
}

#[derive(Debug, PartialEq)]
pub enum FunctionInstruction {
    SetValue { name: String, is_called: bool },
    Lower {},
}

#[cfg(test)]
mod tests {
    use super::*;

    use ra::RaValueExpression;
    use sql::Literal;

    #[tokio::test]
    async fn literal_mapper() {
        let placeholders = HashMap::new();
        let ctes = HashMap::new();
        let outer = HashMap::new();
        let mapper = Mapper::construct::<()>(
            &RaValueExpression::Literal(Literal::BigInteger(123)),
            (&[], &placeholders, &ctes, &outer),
        )
        .unwrap();

        assert_eq!(
            Mapper {
                instruction_stack: vec![ValueInstruction::Constant {
                    value: storage::Data::BigInt(123),
                }],
                value_stack: Vec::new(),
            },
            mapper
        );
    }

    #[tokio::test]
    async fn cast_literal_mapper() {
        let placeholders = HashMap::new();
        let ctes = HashMap::new();
        let outer = HashMap::new();
        let expr = RaValueExpression::Cast {
            inner: Box::new(RaValueExpression::Literal(Literal::Integer(123))),
            target: sql::DataType::BigInteger,
        };
        let mapper = Mapper::construct::<()>(&expr, (&[], &placeholders, &ctes, &outer)).unwrap();

        assert_eq!(
            Mapper {
                instruction_stack: vec![
                    ValueInstruction::Cast {
                        target: sql::DataType::BigInteger,
                    },
                    ValueInstruction::Constant {
                        value: storage::Data::Integer(123),
                    }
                ],
                value_stack: Vec::new(),
            },
            mapper
        );
    }

    #[tokio::test]
    async fn binary_op_mapper() {
        let placeholders = HashMap::new();
        let ctes = HashMap::new();
        let outer = HashMap::new();
        let expr = RaValueExpression::BinaryOperation {
            first: Box::new(RaValueExpression::Literal(Literal::Integer(123))),
            second: Box::new(RaValueExpression::Literal(Literal::Integer(234))),
            operator: sql::BinaryOperator::Add,
        };
        let mapper = Mapper::construct::<()>(&expr, (&[], &placeholders, &ctes, &outer)).unwrap();

        assert_eq!(
            Mapper {
                instruction_stack: vec![
                    ValueInstruction::BinaryOp {
                        operator: sql::BinaryOperator::Add,
                    },
                    ValueInstruction::Constant {
                        value: storage::Data::Integer(123),
                    },
                    ValueInstruction::Constant {
                        value: storage::Data::Integer(234),
                    }
                ],
                value_stack: Vec::new(),
            },
            mapper
        );
    }
}
