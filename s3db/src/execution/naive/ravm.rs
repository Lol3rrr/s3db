use std::collections::{HashMap, VecDeque};

use storage::Row;

use crate::ra::{RaExpression, AttributeId, self};
use super::{value, condition};

use futures::stream::StreamExt;

pub enum ExecuteResult {
    PendingInput(usize),
    Ok(Row),
    OkEmpty,
}

#[derive(Debug, Clone)]
pub struct Input {
    rows: VecDeque<Row>,
    done: bool,
}


pub enum RaInstruction<'expr, 'outer, 'placeholders, 'ctes, 'stream> {
    EmptyRelation {
        used: bool,
    },
    BaseRelation {
        relation: &'expr str,
        stream: Option<futures::stream::LocalBoxStream<'stream, storage::Row>>,
    },
    Projection {
        input: usize,
        expressions: Vec<value::Mapper<'expr, 'outer, 'placeholders, 'ctes>>,
    },
    Selection {
        input: usize,
        condition: condition::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
    },
    Limit {
        input: usize,
        limit: usize,
        offset: usize,
        count: usize,
    },
    Join {
        left_input: usize,
        right_input: usize,
        condition: condition::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        kind: sql::JoinKind,
    },
    LateralJoin {
        left_input: usize,
        right_expr: RaExpression,
        condition: condition::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        kind: sql::JoinKind,
    },
    Aggregation {
        input: usize,
        attributes: &'expr [ra::Attribute<ra::AggregateExpression>],
        grouping_func: Box<dyn Fn(&Row, &Row) -> bool>,
        groups: VecDeque<Vec<Row>>,
    columns: Vec<(String, sql::DataType, AttributeId)>,
    placeholders: &'placeholders HashMap<usize, storage::Data>,
    ctes: &'ctes HashMap<String, storage::EntireRelation>,
    outer: &'outer HashMap<AttributeId, storage::Data>
    },
    OrderBy {
        input: usize,
        attributes: Vec<(usize, sql::OrderBy)>
    },
    Chain {
        inputs: Vec<usize>,
    },
    CTE {
        rows: &'ctes storage::EntireRelation,
        part: usize,
        row: usize,
    },
}

pub struct RaVm<'expr, 'outer, 'placeholders, 'ctes, 'stream> {
    instructions: Vec<RaInstruction<'expr, 'outer, 'placeholders, 'ctes, 'stream>>,
}

impl<'expr, 'outer, 'placeholders, 'ctes, 'stream> RaVm<'expr, 'outer, 'placeholders, 'ctes, 'stream> {
    pub fn construct<'e, SE>(expr: &'e RaExpression, placeholders: &'placeholders HashMap<usize, storage::Data>, ctes: &'ctes HashMap<String, storage::EntireRelation>, outer: &'outer HashMap<AttributeId, storage::Data>) -> Result<Self, ()> where 'e: 'expr {
        let mut pending = vec![expr];
        let mut expression_stack = Vec::new();
        while let Some(tmp) = pending.pop() {
            expression_stack.push(tmp);

            match tmp {
                RaExpression::EmptyRelation | RaExpression::BaseRelation { .. } => {},
                RaExpression::Renamed { inner , ..} => {
                    pending.push(inner);
                }
                RaExpression::Projection { inner, .. } => {
                    pending.push(inner);
                }
                RaExpression::Selection { inner, .. } => {
                    pending.push(inner);
                }
                RaExpression::Join { left, right, .. } => {
                    pending.push(left);
                    pending.push(right);
                }
                RaExpression::LateralJoin { left, ..} => {
                    pending.push(left);
                }
                RaExpression::Aggregation { inner, .. } => {
                    pending.push(inner);
                }
                RaExpression::Limit { inner, .. } => {
                    pending.push(inner);
                }
                RaExpression::OrderBy { inner, .. } => {
                    pending.push(inner);
                }
                RaExpression::Chain { parts } => {
                    pending.extend(parts.iter());
                }
                RaExpression::CTE { .. } => {}
            };
        }

        let mut instructions = Vec::new();
        while let Some(expr) = expression_stack.pop() {
            let instr = match expr {
                RaExpression::EmptyRelation => RaInstruction::EmptyRelation { used: false },
                RaExpression::BaseRelation { name, .. } => RaInstruction::BaseRelation { relation: &name.0, stream: None },
                RaExpression::Projection { attributes, inner } => {
                    let columns: Vec<_> = inner.get_columns().into_iter().map(|(_, n, ty, id)| (n, ty, id)).collect();

                    let mut expressions = Vec::new();
                    for attribute in attributes {
                        let mapper = value::Mapper::construct::<SE>(&attribute.value, (&columns, placeholders, ctes, outer)).map_err(|e| ())?;
                        expressions.push(mapper);
                    }

                    RaInstruction::Projection { expressions, input: instructions.len() -1 }
                }
                RaExpression::Selection { inner, filter } => {
                    let columns: Vec<_> = inner.get_columns().into_iter().map(|(_, n, ty, id)| (n, ty, id)).collect();

                    let cond = condition::Mapper::construct::<SE>(filter, (&columns, placeholders, ctes, outer)).map_err(|e| ())?;

                    RaInstruction::Selection { condition: cond, input: instructions.len() - 1 }
                }
                RaExpression::Limit { limit, offset, .. } => {
                    RaInstruction::Limit { input: instructions.len() -1, limit: *limit , offset: *offset, count: 0,}
                }
                RaExpression::Renamed { .. } => continue,
                RaExpression::OrderBy { inner, attributes } => {
                    let columns = inner.get_columns();

                    let mut orderings = Vec::new();
                    for (attr_id, order) in attributes {
                        dbg!(&attr_id, &columns);

                        let idx = match columns.iter().enumerate().find(|(_, (_, _, _, id))| id == attr_id) {
                            Some((i, _)) => i,
                            None => todo!(),
                        };

                        orderings.push((idx, order.clone()));
                    }

                    RaInstruction::OrderBy { attributes: orderings, input: instructions.len() -1 }
                }
                RaExpression::Aggregation { inner, attributes, aggregation_condition } => {
                    let columns: Vec<_> = inner.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)).collect();

                    let grouping_func: Box<dyn Fn(&Row, &Row) -> bool> = match aggregation_condition {
                        crate::ra::AggregationCondition::Everything => {
                            Box::new(|_, _| true)
                        }
                        crate::ra::AggregationCondition::GroupBy { fields } => {
                                let compare_indices: Vec<_> = fields
                                    .iter()
                                    .map(|(_, src_id)| {
                                        columns
                                            .iter()
                                            .enumerate()
                                            .find(|(_, (_, _, c_id))| c_id == src_id)
                                            .map(|(i, _)| i)
                                            .unwrap()
                                    })
                                    .collect();

                            Box::new(move |first, second| {
                                compare_indices.iter().all(|idx| {
                                    first.data[*idx] == second.data[*idx]
                                })
                            })
                        }
                    };

                    RaInstruction::Aggregation { attributes: &attributes, input: instructions.len() - 1, grouping_func, groups: VecDeque::new(), columns, placeholders, ctes, outer }
                }
                RaExpression::Join { left, right, kind, condition } => {
                    let left_columns = left.get_columns();
                    let right_columns = right.get_columns();

                    let combined_columns: Vec<_> = left_columns.into_iter().chain(right_columns.into_iter()).map(|(_, n, t, i)| (n, t, i)).collect();

                    let join_cond = condition::Mapper::construct::<SE>(condition, (&combined_columns, placeholders, ctes, outer)).map_err(|e| ())?;

                    RaInstruction::Join {
                        left_input: instructions.len() - 2,
                        right_input: instructions.len() - 1,
                        condition: join_cond,
                        kind: kind.clone(),
                    }
                }
                RaExpression::LateralJoin { left, right, kind, condition } => {
                    let left_columns = left.get_columns();
                    let right_columns = right.get_columns();

                    let combined_columns: Vec<_> = left_columns.into_iter().chain(right_columns.into_iter()).map(|(_, n, t, i)| (n, t, i)).collect();

                    let join_cond = condition::Mapper::construct::<SE>(condition, (&combined_columns, placeholders, ctes, outer)).map_err(|e| ())?;

                    RaInstruction::LateralJoin {
                        left_input: instructions.len() - 1,
                        right_expr: *right.clone(),
                        condition: join_cond,
                        kind: kind.clone()
                    }
                }
                RaExpression::CTE { name, .. } => {
                    let cte = match ctes.get(name) {
                        Some(c) => c,
                        None => return Err(())?,
                    };

                    RaInstruction::CTE {
                        rows: cte,
                        part: 0,
                        row: 0,
                    }
                }
                RaExpression::Chain { parts } => {
                    let inputs: Vec<_> = (instructions.len() - 1 - parts.len()..instructions.len()-1).collect();

                    RaInstruction::Chain { inputs }
                }
            };

            instructions.push(instr);
        }

        Ok(Self {
            instructions,
        })
    }

    pub async fn get_next<'tg, 'engine, S>(&mut self, engine: &'engine super::NaiveEngine<S>, tguard: &'tg S::TransactionGuard) -> Option<storage::Row> where S: storage::Storage, 'tg: 'stream, 'engine: 'stream, 'expr: 'stream {
        let mut idx = self.instructions.len() - 1;

        let mut results = vec![Input { rows: VecDeque::new(), done: false }; self.instructions.len()];
        let mut return_stack = Vec::with_capacity(self.instructions.len());

        loop {
            let instr = self.instructions.get_mut(idx)?;
            let input = results.get_mut(idx)?;
            
            match instr.try_execute(input, engine, tguard).await {
                Ok(ExecuteResult::Ok(v)) => {
                    match return_stack.pop() {
                        Some(prev_idx) => {
                            let prev_inputs: &mut Input = results.get_mut(prev_idx)?;
                            prev_inputs.rows.push_back(v);
                            prev_inputs.done = false;

                            idx = prev_idx;
                        }
                        None => {
                            return Some(v)
                        },
                    };
                }
                Ok(ExecuteResult::OkEmpty) => {
                    let prev_idx = return_stack.pop()?;
                    let prev_inputs: &mut Input = results.get_mut(prev_idx)?;

                    prev_inputs.done = true;

                    idx = prev_idx;
                }
                Ok(ExecuteResult::PendingInput(_)) if input.done => {
                    return None;
                }
                Ok(ExecuteResult::PendingInput(input_idx)) => {
                    return_stack.push(idx);
                    idx = input_idx;
                }
                Err(v) => {
                    dbg!(v);
                    return None
                },
            };
        }
    }
}

impl<'expr, 'placeholders, 'ctes, 'outer, 'stream> RaInstruction<'expr, 'placeholders, 'ctes, 'outer, 'stream> where 'expr: 'stream {
    async fn try_execute<'tg, 'engine, S>(&mut self, input_data: &mut Input, engine: &'engine super::NaiveEngine<S>, tguard: &'tg S::TransactionGuard) -> Result<ExecuteResult, ()> where S: storage::Storage, 'tg: 'stream, 'engine: 'stream {
        match self {
            Self::EmptyRelation { used } => {
                if *used {
                    Ok(ExecuteResult::OkEmpty)
                } else {
                    *used = true;
                    Ok(ExecuteResult::Ok(storage::Row::new(0, Vec::new())))
                }
            },
            Self::Projection { input, expressions } => {
                let row = match input_data.rows.pop_front() {
                    Some(r) => r,
                    None => return Ok(ExecuteResult::PendingInput(*input)),
                };

                let row = storage::RowCow::Owned(row);

                let mut result = Vec::with_capacity(expressions.len());
                
                let arena = bumpalo::Bump::new();
                for expr in expressions.iter_mut() {
                    let value = expr.evaluate_mut(&row, engine, tguard, &arena).await.ok_or(())?;
                    result.push(match value {
                        storage::Data::List(mut v) if v.len() == 1 => {
                            v.pop().unwrap()
                        }
                        other => other,
                    });
                }

                Ok(ExecuteResult::Ok(storage::Row::new(0, result)))
            }
            Self::Selection { input, condition } => {
                let row = match input_data.rows.pop_front() {
                    Some(r) => r,
                    None => return Ok(ExecuteResult::PendingInput(*input)),
                };
                let row = storage::RowCow::Owned(row);

                let arena = bumpalo::Bump::new();
                let cond_res = condition.evaluate_mut(&row, engine, tguard, &arena).await.ok_or(())?;

                if cond_res {
                    Ok(ExecuteResult::Ok(row.into_owned()))
                } else {
                    Ok(ExecuteResult::PendingInput(*input))
                }
            }
            Self::BaseRelation { relation, stream } => {
                let stream = match stream {
                    Some(s) => s,
                    None => {
                        let (_, s) = engine.storage.stream_relation(relation, tguard).await.map_err(|e| ())?;
                        *stream = Some(s.map(|r| r.into_owned()).fuse().boxed_local());
                        stream.as_mut().unwrap()
                    }
                };

                let tmp = stream.next().await;

                match tmp {
                    Some(r) => Ok(ExecuteResult::Ok(r)),
                    None => Ok(ExecuteResult::OkEmpty)
                }
            }
            Self::Limit { input, limit, offset, count } => {
                if count < offset {
                    let tmp = input_data.rows.pop_front();
                    if tmp.is_some() {
                        *count += 1;
                    }

                    return Ok(ExecuteResult::PendingInput(*input));
                }

                if *count >= *offset + *limit {
                    return Ok(ExecuteResult::OkEmpty);
                }

                match input_data.rows.pop_front() {
                    Some(row) => {
                        *count += 1;
                        return Ok(ExecuteResult::Ok(row));
                    }
                    None => {
                        return Ok(ExecuteResult::PendingInput(*input))
                    }
                }
            }
            Self::CTE { rows, part, row } => {
                while let Some(part_ref) = rows.parts.get(*part) {
                    let row_ref = match part_ref.rows.get(*row) {
                        Some(r) => r,
                        None => {
                            *part += 1;
                            *row = 0;
                            continue;
                        }
                    };

                    *row += 1;

                    return Ok(ExecuteResult::Ok(row_ref.clone()))
                }

                Ok(ExecuteResult::OkEmpty)
            }
            Self::Chain { inputs } => {
                todo!("Chain")
            }
            Self::OrderBy { input, attributes } => {
                todo!("OrderBy")
            }
            Self::Aggregation { input, attributes, grouping_func , groups, columns, placeholders, ctes, outer } => {
                if !input_data.done {
                    match input_data.rows.pop_front() {
                        Some(row) => {
                            for group in groups.iter_mut() {
                                let rep = group.first().expect("Every group has at least one member row");

                                if grouping_func(rep, &row) {
                                    group.push(row);

                                    return Ok(ExecuteResult::PendingInput(*input));
                                }
                            }

                            groups.push_back(vec![row]);

                            return Ok(ExecuteResult::PendingInput(*input));
                        }
                        None => return Ok(ExecuteResult::PendingInput(*input)),
                    };
                }

                let group = match groups.pop_front() {
                    Some(g) => g,
                    None => return Ok(ExecuteResult::OkEmpty),
                };
                
                let mut states = Vec::with_capacity(attributes.len());
                for attr in attributes.iter() {
                    let state = super::AggregateState::new::<S::LoadingError>(&attr.value, &columns, placeholders, ctes, outer);
                    states.push(state);
                }

                let arena = bumpalo::Bump::new();
                for row in group {
                    let row = storage::RowCow::Owned(row);
                    for state in states.iter_mut() {
                        state.update(engine, &row, tguard, &arena).await.unwrap();
                    }
                }

                let resulting_row: Vec<_> = states.into_iter().map(|r| r.to_data().unwrap()).collect();
                
                Ok(ExecuteResult::Ok(storage::Row::new(0, resulting_row)))
            }
            Self::Join { left_input, right_input, condition, kind } => {
                todo!("Join")
            }
            Self::LateralJoin { left_input, right_expr, condition, kind } => {
                todo!("LateralJoin")
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    use crate::ra;

    #[test]
    fn construct_project_empty() {
        let expr = RaExpression::Projection {
            attributes: vec![ra::ProjectionAttribute {
                name: "test".into(),
                id: ra::AttributeId::new(0),
                value: ra::RaValueExpression::Literal(sql::Literal::Integer(13)),
            }],
            inner: Box::new(RaExpression::EmptyRelation),
        };

        let placeholders = HashMap::new();
        let ctes = HashMap::new();
        let outer = HashMap::new();

        let constructed = RaVm::construct::<()>(&expr, &placeholders, &ctes, &outer).unwrap();
    }
}
