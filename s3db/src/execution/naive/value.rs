use std::collections::HashMap;
use futures::{future::{LocalBoxFuture, FutureExt}, stream::StreamExt};

use sql::{DataType,BinaryOperator};
use crate::{execution::naive::NaiveEngine, storage::{self, Storage, Data}, ra::{self, AttributeId}};

use super::EvaulateRaError;

pub async fn evaluate_ra_value<'engine, 's, 'rve, 'row, 'columns, 'placeholders, 'c, 'o, 't, 'f, 'arena, S>(
    engine: &'engine NaiveEngine<S>,
    expr: &'rve ra::RaValueExpression,
    row: &'row storage::Row,
    columns: &'columns [(String, DataType, AttributeId)],
    placeholders: &'placeholders HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
    transaction: &'t S::TransactionGuard,
    arena: &'arena bumpalo::Bump,
) -> Result<storage::Data, EvaulateRaError<S::LoadingError>>
where
    's: 'f,
    'rve: 'f,
    'row: 'f,
    'columns: 'f,
    'placeholders: 'f,
    'c: 'f,
    'o: 'f,
    't: 'f,
    'arena: 'f,
    'engine: 'f,
    S: Storage,
{
        let mut pending = vec![expr];
        let mut instructions = Vec::new();
        while let Some(pend) = pending.pop() {
            instructions.push(pend);
            match pend {
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
                    ra::RaFunction::Substr { str_value, start, count } => {
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
                }
                ra::RaValueExpression::SubQuery { .. } => {}
            };
        }

        let mut results = Vec::with_capacity(instructions.len());

        while let Some(instruction) = instructions.pop() {
            let partial_res = match instruction {
                ra::RaValueExpression::Attribute { a_id, name, .. } => {
                    // TODO

                    let column_index = columns
                        .iter()
                        .enumerate()
                        .find(|(_, (_, _, id))| id == a_id)
                        .map(|(i, _)| i)
                        .ok_or_else(|| EvaulateRaError::UnknownAttribute { name: name.clone(), id: *a_id })?;

                    Ok(row.data[column_index].clone())
                }
                ra::RaValueExpression::OuterAttribute { a_id, name, .. } => {
                    dbg!(&a_id, &outer);

                    let value = outer.get(a_id).ok_or_else(|| EvaulateRaError::UnknownAttribute { name: name.clone(), id: *a_id })?;

                    Ok(value.clone())
                }
                ra::RaValueExpression::Placeholder(placeholder) => {
                    placeholders.get(placeholder).cloned().ok_or_else(|| {
                        dbg!(&placeholders, &placeholder);
                        EvaulateRaError::Other("Getting Placeholder Value")
                    })
                }
                ra::RaValueExpression::Literal(lit) => Ok(storage::Data::from_literal(lit)),
                ra::RaValueExpression::List(elems) => {
                    let results_len = results.len();
                    let result = results.drain(results_len-elems.len()..).collect();

                    Ok(Data::List(result))
                }
                ra::RaValueExpression::SubQuery { query } => {
                    let n_outer = {
                        let tmp = outer.clone();
                        // TODO
                        // dbg!(columns, row);
                        tmp
                    };
                    let (_, rows) = engine.evaluate_ra(&query, placeholders, ctes, &n_outer, transaction, &arena).await?;

                    let parts: Vec<_> = rows
                        .map(|r| r.data[0].clone())
                        .collect().await;

                    Ok(storage::Data::List(parts))
                }
                ra::RaValueExpression::Cast { inner, target } => {
                    let result = results.pop().unwrap();

                    result
                        .try_cast(target)
                        .map_err(|(data, ty)| EvaulateRaError::CastingType {
                            value: data,
                            target: ty.clone(),
                        })
                }
                ra::RaValueExpression::BinaryOperation {
                    first,
                    second,
                    operator,
                } => {
                    let first_value = results.pop().unwrap();
                    let second_value = results.pop().unwrap();

                    match operator {
                        BinaryOperator::Add => match (first_value, second_value) {
                            (Data::SmallInt(f), Data::SmallInt(s)) => Ok(Data::SmallInt(f + s)),
                            (Data::Integer(f), Data::SmallInt(s)) => Ok(Data::Integer(f + s as i32)),
                            other => {
                                dbg!(other);
                                Err(EvaulateRaError::Other("Addition"))
                            }
                        },
                        BinaryOperator::Subtract => match (first_value, second_value) {
                            (Data::Integer(f), Data::Integer(s)) => Ok(Data::Integer(f - s)),
                            other => {
                                dbg!(other);
                                Err(EvaulateRaError::Other("Subtracting"))
                            }
                        },
                        BinaryOperator::Divide => match (first_value, second_value) {
                            (Data::Integer(f), Data::Integer(s)) => Ok(Data::Integer(f / s)),
                            other => {
                                dbg!(other);
                                Err(EvaulateRaError::Other("Subtracting"))
                            }
                        },
                        other => {
                            dbg!(&other, first_value, second_value);

                            Err(EvaulateRaError::Other("Evaluating Binary Operator"))
                        }
                    }
                }
                ra::RaValueExpression::Function(fc) => match fc {
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
                        value,
                        is_called,
                    } => {
                        dbg!(&name, &value, &is_called);

                        let value_res = results.pop().unwrap();
                        dbg!(&value_res);

                        let value = match value_res {
                            Data::List(mut v) => {
                                if v.is_empty() {
                                    return Err(EvaulateRaError::Other(""));
                                }

                                v.swap_remove(0)
                            }
                            other => other,
                        };

                        dbg!(&value);

                        // TODO
                        // Actually update the Sequence Value

                        Ok(value)
                    }
                    ra::RaFunction::Lower(val) => {
                        dbg!(&val);

                        let data = results.pop().unwrap();

                        match data {
                            storage::Data::Text(d) => Ok(storage::Data::Text(d.to_lowercase())),
                            other => {
                                dbg!(&other);
                                Err(EvaulateRaError::Other("Unexpected Type"))
                            }
                        }
                    }
                    ra::RaFunction::Substr {
                        str_value,
                        start,
                        count,
                    } => {
                        dbg!(&str_value, &start);

                        let str_value = results.pop().unwrap();
                        let start_value = results.pop().unwrap();
                        let count_value = match count.as_ref() {
                            Some(_) => results.pop(),
                            None => None,
                        };

                        dbg!(&str_value, &start_value, &count_value);

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
                },
                ra::RaValueExpression::Renamed { name, value } => {
                    dbg!(&name, &value);

                    Err(EvaulateRaError::Other("Renamed Value Expression"))
                }
            }?;

            results.push(partial_res);
        }

        let value_res = results.pop().unwrap();
        Ok(value_res)
}
