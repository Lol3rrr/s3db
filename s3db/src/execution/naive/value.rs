use futures::{future::FutureExt, stream::StreamExt};
use std::collections::HashMap;

use crate::{
    execution::naive::NaiveEngine,
    ra::{self, AttributeId},
    storage::{self, Data},
};
use sql::{BinaryOperator, DataType};

use super::EvaulateRaError;

#[derive(Debug, PartialEq)]
pub struct Mapper<'expr, 'outer, 'placeholders, 'ctes> {
    instruction_stack: Vec<ValueInstruction<'expr, 'outer, 'placeholders, 'ctes>>,
}

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
                    let mut values = Vec::with_capacity(parts.len());
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

#[deprecated]
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

pub async fn construct_mapper<'rve, 'columns, 'placeholders, 'c, 'o, 'f, SE>(
    expr: &'rve ra::RaValueExpression,
    columns: &'columns [(String, DataType, AttributeId)],
    placeholders: &'placeholders HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
) -> Result<Mapper<'rve, 'o, 'placeholders, 'c>, EvaulateRaError<SE>>
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

    let mut results: Vec<ValueInstruction> = Vec::with_capacity(instructions.len());

    while let Some(instruction) = instructions.pop() {
        let partial_res: ValueInstruction = match instruction {
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
                        value,
                        is_called,
                    } => {
                        dbg!(&name, &value, &is_called);

                        Ok(FunctionInstruction::SetValue {
                            name: name.clone(),
                            is_called: *is_called,
                        })
                    }
                    ra::RaFunction::Lower(val) => {
                        dbg!(&val);

                        Ok(FunctionInstruction::Lower {})
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
                }?;

                Ok(ValueInstruction::Function { func })
            }
            ra::RaValueExpression::Renamed { name, value } => {
                dbg!(&name, &value);

                Err(EvaulateRaError::Other("Renamed Value Expression"))
            }
        }?;

        results.push(partial_res);
    }

    results.reverse();

    Ok(Mapper {
        instruction_stack: results,
    })
}

impl<'rve, 'columns, 'placeholders, 'c, 'o, 'f> Mapper<'rve, 'o, 'placeholders, 'c> {
    pub async fn evaluate<'row, 'engine, 'transaction, 'arena, S>(
        &self,
        row: &'row storage::Row,
        engine: &'engine NaiveEngine<S>,
        transaction: &'transaction S::TransactionGuard,
        arena: &'arena bumpalo::Bump,
    ) -> Option<storage::Data>
    where
        S: storage::Storage,
    {
        let mut value_stack: Vec<storage::Data> = Vec::with_capacity(self.instruction_stack.len());

        for instruction in self.instruction_stack.iter().rev() {
            let value_result = match instruction {
                ValueInstruction::Constant { value } => value.clone(),
                ValueInstruction::Attribute { idx } => row.data[*idx].clone(),
                ValueInstruction::Cast { target } => {
                    let input = value_stack.pop()?;
                    input.try_cast(target).ok()?
                }
                ValueInstruction::List { len } => {
                    let stack_len = value_stack.len();
                    let values: Vec<_> = value_stack.drain(stack_len - len..).collect();
                    storage::Data::List(values)
                }
                ValueInstruction::BinaryOp { operator } => {
                    let first_value = value_stack.pop()?;
                    let second_value = value_stack.pop()?;

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
                    }?
                }
                ValueInstruction::SubQuery {
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
                    storage::Data::List(parts)
                }
                ValueInstruction::Function { func } => {
                    match func {
                        FunctionInstruction::SetValue { name, is_called } => {
                            let value = value_stack.pop()?;

                            let value = match value {
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

                            value
                        }
                        FunctionInstruction::Lower {} => {
                            let value = value_stack.pop()?;

                            match value {
                                storage::Data::Text(d) => {
                                    Some(storage::Data::Text(d.to_lowercase()))
                                }
                                other => {
                                    dbg!(&other);
                                    //Err(EvaulateRaError::Other("Unexpected Type"))
                                    None
                                }
                            }?
                        }
                    }
                }
            };

            value_stack.push(value_result);
        }

        value_stack.pop()
    }
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
        let mapper = construct_mapper::<()>(
            &RaValueExpression::Literal(Literal::BigInteger(123)),
            &[],
            &placeholders,
            &ctes,
            &outer,
        )
        .await
        .unwrap();

        assert_eq!(
            Mapper {
                instruction_stack: vec![ValueInstruction::Constant {
                    value: storage::Data::BigInt(123),
                }],
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
        let mapper = construct_mapper::<()>(&expr, &[], &placeholders, &ctes, &outer)
            .await
            .unwrap();

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
        let mapper = construct_mapper::<()>(&expr, &[], &placeholders, &ctes, &outer)
            .await
            .unwrap();

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
            },
            mapper
        );
    }
}
