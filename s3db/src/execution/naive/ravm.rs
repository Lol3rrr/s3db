use std::collections::{HashMap, VecDeque};

use storage::Row;

use crate::ra::{RaExpression, AttributeId};
use super::{value, condition};

use futures::stream::StreamExt;

pub enum ExecuteResult {
    PendingInput(usize),
    Ok(Vec<Row>),
    OkEmpty,
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
        attributes: Vec<super::AggregateState<'expr, 'outer, 'placeholders, 'ctes>>,
        condition: crate::ra::AggregationCondition,
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
                    RaInstruction::Limit { input: instructions.len() -1, limit: *limit , offset: *offset}
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
                    dbg!(attributes, aggregation_condition);

                    let columns: Vec<_> = inner.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)).collect();

                    let mut states = Vec::new();
                    for attr in attributes.iter() {
                        let state = super::AggregateState::new::<SE>(&attr.value, &columns, placeholders, ctes, outer);
                        states.push(state);
                    }

                    RaInstruction::Aggregation { attributes: states, condition: aggregation_condition.clone(), input: instructions.len() - 1 }
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

        let mut results = vec![VecDeque::new(); self.instructions.len()];
        let mut return_stack = Vec::new();

        loop {
            let instr = self.instructions.get_mut(idx)?;
            let input = results.get_mut(idx)?;
            
            match instr.try_execute(input, engine, tguard).await {
                Ok(ExecuteResult::Ok(mut v)) => {
                    match return_stack.pop() {
                        Some(prev_idx) => {
                            let prev_inputs: &mut VecDeque<_> = results.get_mut(prev_idx)?;
                            prev_inputs.extend(v);

                            idx = prev_idx;
                        }
                        None => {
                            let value = v.pop()?;
                            return Some(value)
                        },
                    };
                }
                Ok(ExecuteResult::OkEmpty) => {
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
    async fn try_execute<'tg, 'engine, S>(&mut self, input_rows: &mut VecDeque<storage::Row>, engine: &'engine super::NaiveEngine<S>, tguard: &'tg S::TransactionGuard) -> Result<ExecuteResult, ()> where S: storage::Storage, 'tg: 'stream, 'engine: 'stream {
        match self {
            Self::EmptyRelation { used } => {
                if *used {
                    Ok(ExecuteResult::OkEmpty)
                } else {
                    *used = true;
                    Ok(ExecuteResult::Ok(vec![storage::Row::new(0, Vec::new())]))
                }
            },
            Self::Projection { input, expressions } => {
                let row = match input_rows.pop_front() {
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

                Ok(ExecuteResult::Ok(vec![storage::Row::new(0, result)]))
            }
            Self::Selection { input, condition } => {
                let row = match input_rows.pop_front() {
                    Some(r) => r,
                    None => return Ok(ExecuteResult::PendingInput(*input)),
                };
                let row = storage::RowCow::Owned(row);

                let arena = bumpalo::Bump::new();
                let cond_res = condition.evaluate_mut(&row, engine, tguard, &arena).await.ok_or(())?;

                if cond_res {
                    Ok(ExecuteResult::Ok(vec![row.into_owned()]))
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
                    Some(r) => Ok(ExecuteResult::Ok(vec![r])),
                    None => Ok(ExecuteResult::OkEmpty)
                }
            }
            Self::Limit { input, limit, offset } => {
                todo!("Limit")
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

                    return Ok(ExecuteResult::Ok(vec![row_ref.clone()]))
                }

                Ok(ExecuteResult::OkEmpty)
            }
            Self::Chain { inputs } => {
                todo!("Chain")
            }
            Self::OrderBy { input, attributes } => {
                todo!("OrderBy")
            }
            Self::Aggregation { input, attributes, condition } => {
                todo!("Aggregation")
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
