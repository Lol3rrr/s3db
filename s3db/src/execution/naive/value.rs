use futures::{future::FutureExt, stream::StreamExt};
use std::collections::HashMap;

use crate::{
    execution::naive::NaiveEngine,
    ra::{self, AttributeId},
    storage::{self, Data, Storage},
};
use sql::{BinaryOperator, DataType};

use super::EvaulateRaError;

#[derive(Debug, PartialEq)]
pub enum ValueMapper<'expr, 'outer, 'placeholders, 'ctes> {
    Attribute {
        idx: usize,
    },
    Constant {
        value: storage::Data,
    },
    List {
        parts: Vec<Self>,
    },
    Cast {
        inner: Box<Self>,
        target: sql::DataType,
    },
    BinaryOp {
        first: Box<Self>,
        second: Box<Self>,
        operator: BinaryOperator,
    },
    Function {
        func: FunctionMapper<'expr, 'outer, 'placeholders, 'ctes>,
    },
    SubQuery {
        query: &'expr ra::RaExpression,
        outer: &'outer HashMap<AttributeId, storage::Data>,
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
    },
}

#[derive(Debug, PartialEq)]
pub enum FunctionMapper<'expr, 'outer, 'placeholders, 'ctes> {
    SetValue {
        name: String,
        value: Box<ValueMapper<'expr, 'outer, 'placeholders, 'ctes>>,
        is_called: bool,
    },
    Lower {
        value: Box<ValueMapper<'expr, 'outer, 'placeholders, 'ctes>>,
    },
}

impl<'expr, 'outer, 'placeholders, 'ctes> ValueMapper<'expr, 'outer, 'placeholders, 'ctes> {
    pub async fn evaluate<'s, 'row, 'engine, 'transaction, 'arena, 'f, S>(
        &'s self,
        row: &'row storage::Row,
        engine: &'engine NaiveEngine<S>,
        transaction: &'transaction S::TransactionGuard,
        arena: &'arena bumpalo::Bump,
    ) -> Option<storage::Data>
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
                Self::Attribute { .. } | Self::Constant { .. } | Self::SubQuery { .. } => {}
                Self::Cast { inner, .. } => {
                    pending.push(&inner);
                }
                Self::List { parts } => {
                    pending.extend(parts.iter().rev());
                }
                Self::BinaryOp { first, second, .. } => {
                    pending.push(second);
                    pending.push(first);
                }
                Self::Function { func } => match func {
                    FunctionMapper::SetValue { value, .. } => {
                        pending.push(value);
                    }
                    FunctionMapper::Lower { value } => {
                        pending.push(value);
                    }
                },
            };
        }

        let mut results: Vec<storage::Data> = Vec::with_capacity(instructions.len());

        while let Some(tmp) = instructions.pop() {
            let res = match tmp {
                Self::Attribute { idx } => {
                    let tmp = row.data.get(*idx)?;
                    Some(tmp.clone())
                }
                Self::Constant { value } => Some(value.clone()),
                Self::Cast { target, .. } => {
                    let inner_val = results.pop()?;
                    inner_val.try_cast(target).ok()
                }
                Self::Function { func } => match func {
                    FunctionMapper::Lower { .. } => {
                        let before = results.pop()?;
                        match before {
                            storage::Data::Text(d) => Some(storage::Data::Text(d.to_lowercase())),
                            other => {
                                dbg!(&other);
                                //Err(EvaulateRaError::Other("Unexpected Type"))
                                None
                            }
                        }
                    }
                    FunctionMapper::SetValue { .. } => {
                        let value_res = results.pop()?;

                        let value = match value_res {
                            Data::List(mut v) => {
                                if v.is_empty() {
                                    // return Err(EvaulateRaError::Other(""));
                                    return None;
                                }

                                v.swap_remove(0)
                            }
                            other => other,
                        };

                        dbg!(&value);

                        // TODO
                        // Actually update the Sequence Value

                        Some(value)
                    }
                },
                Self::BinaryOp { operator, .. } => {
                    let first_value = results.pop()?;
                    let second_value = results.pop()?;

                    match operator {
                        BinaryOperator::Add => match (first_value, second_value) {
                            (Data::SmallInt(f), Data::SmallInt(s)) => Some(Data::SmallInt(f + s)),
                            (Data::Integer(f), Data::SmallInt(s)) => {
                                Some(Data::Integer(f + s as i32))
                            }
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
                Self::SubQuery {
                    query,
                    outer,
                    placeholders,
                    ctes,
                } => {
                    let n_outer = {
                        let mut tmp: HashMap<_, _> = (*outer).clone();

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
                Self::List { parts } => {
                    let mut values = Vec::new();
                    for _ in parts {
                        let v = results.pop()?;
                        values.push(v);
                    }
                    Some(storage::Data::List(values))
                }
            }?;
            results.push(res);
        }

        results.pop()
    }
}

pub async fn construct<'rve, 'columns, 'placeholders, 'c, 'o, 'f, SE>(
    expr: &'rve ra::RaValueExpression,
    columns: &'columns [(String, DataType, AttributeId)],
    placeholders: &'placeholders HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
) -> Result<ValueMapper<'rve, 'o, 'placeholders, 'c>, EvaulateRaError<SE>>
where
    'rve: 'f,
    'columns: 'f,
    'placeholders: 'f,
    'c: 'f,
    'o: 'f,
{
    let mut pending = Vec::with_capacity(16);
    pending.push(expr);
    let mut instructions = Vec::with_capacity(16);
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
        };
    }

    let mut results: Vec<ValueMapper> = Vec::with_capacity(instructions.len());

    while let Some(instruction) = instructions.pop() {
        let partial_res: ValueMapper = match instruction {
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

                Ok(ValueMapper::Attribute { idx: column_index })
            }
            ra::RaValueExpression::OuterAttribute { a_id, name, .. } => {
                dbg!(&a_id, &outer);

                let value = outer
                    .get(a_id)
                    .ok_or_else(|| EvaulateRaError::UnknownAttribute {
                        name: name.clone(),
                        id: *a_id,
                    })?;

                Ok(ValueMapper::Constant {
                    value: value.clone(),
                })
            }
            ra::RaValueExpression::Placeholder(placeholder) => {
                let value = placeholders.get(placeholder).cloned().ok_or_else(|| {
                    dbg!(&placeholders, &placeholder);
                    EvaulateRaError::Other("Getting Placeholder Value")
                })?;

                Ok(ValueMapper::Constant { value })
            }
            ra::RaValueExpression::Literal(lit) => {
                let res = storage::Data::from_literal(lit);

                Ok(ValueMapper::Constant { value: res })
            }
            ra::RaValueExpression::List(elems) => {
                let results_len = results.len();
                let result: Vec<_> = results.drain(results_len - elems.len()..).collect();

                Ok(ValueMapper::List { parts: result })
            }
            ra::RaValueExpression::SubQuery { query } => Ok(ValueMapper::SubQuery {
                query,
                outer,
                placeholders,
                ctes,
            }),
            ra::RaValueExpression::Cast { target, .. } => {
                let result = results.pop().unwrap();

                Ok(ValueMapper::Cast {
                    inner: Box::new(result),
                    target: target.clone(),
                })
            }
            ra::RaValueExpression::BinaryOperation { operator, .. } => {
                let first_mapping = results.pop().unwrap();
                let second_mapping = results.pop().unwrap();

                Ok(ValueMapper::BinaryOp {
                    first: Box::new(first_mapping),
                    second: Box::new(second_mapping),
                    operator: operator.clone(),
                })
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

                    let value_res_mapping = results.pop().unwrap();

                    Ok(ValueMapper::Function {
                        func: FunctionMapper::SetValue {
                            name: name.clone(),
                            value: Box::new(value_res_mapping),
                            is_called: *is_called,
                        },
                    })
                }
                ra::RaFunction::Lower(val) => {
                    dbg!(&val);

                    let data_mapper = results.pop().unwrap();

                    Ok(ValueMapper::Function {
                        func: FunctionMapper::Lower {
                            value: Box::new(data_mapper),
                        },
                    })
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

pub async fn evaluate_ra_value<
    'engine,
    's,
    'rve,
    'row,
    'columns,
    'placeholders,
    'c,
    'o,
    't,
    'f,
    'arena,
    S,
>(
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
                    .ok_or_else(|| EvaulateRaError::UnknownAttribute {
                        name: name.clone(),
                        id: *a_id,
                    })?;

                Ok(row.data[column_index].clone())
            }
            ra::RaValueExpression::OuterAttribute { a_id, name, .. } => {
                dbg!(&a_id, &outer);

                let value = outer
                    .get(a_id)
                    .ok_or_else(|| EvaulateRaError::UnknownAttribute {
                        name: name.clone(),
                        id: *a_id,
                    })?;

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
                let result = results.drain(results_len - elems.len()..).collect();

                Ok(Data::List(result))
            }
            ra::RaValueExpression::SubQuery { query } => {
                let n_outer = {
                    let tmp = outer.clone();
                    // TODO
                    // dbg!(columns, row);
                    tmp
                };
                let (_, rows) = engine
                    .evaluate_ra(&query, placeholders, ctes, &n_outer, transaction, &arena)
                    .await?;

                let parts: Vec<_> = rows.map(|r| r.data[0].clone()).collect().await;

                Ok(storage::Data::List(parts))
            }
            ra::RaValueExpression::Cast { target, .. } => {
                let result = results.pop().unwrap();

                result
                    .try_cast(target)
                    .map_err(|(data, ty)| EvaulateRaError::CastingType {
                        value: data,
                        target: ty.clone(),
                    })
            }
            ra::RaValueExpression::BinaryOperation { operator, .. } => {
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
