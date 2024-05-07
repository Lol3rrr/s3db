use std::collections::{HashMap, VecDeque};

use storage::Row;

use super::{condition, value};
use crate::ra::{self, AttributeId, RaExpression};

use futures::{future::FutureExt, stream::StreamExt};

pub enum ExecuteResult {
    PendingInput(usize),
    Ok(Row),
    OkEmpty,
}

#[derive(Debug, Clone)]
pub struct Input {
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
        arena: bumpalo::Bump,
    },
    Selection {
        input: usize,
        condition: condition::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        arena: bumpalo::Bump,
    },
    Limit {
        input: usize,
        limit: usize,
        offset: usize,
        count: usize,
    },
    Join {
        left_input: usize,
        returned_left: bool,
        right_input: usize,
        condition: condition::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        kind: sql::JoinKind,
        right_rows: Vec<Row>,
        right_index: usize,
        right_done: bool,
        right_column_count: usize,
    },
    LateralJoin {
        left_input: usize,
        left_ids: Vec<AttributeId>,
        left_row: Option<Row>,
        right_rows: Vec<Row>,
        right_expr: RaExpression,
        condition: condition::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
        kind: sql::JoinKind,
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
        outer: &'outer HashMap<AttributeId, storage::Data>,
    },
    Aggregation {
        input: usize,
        attributes: &'expr [ra::Attribute<ra::AggregateExpression>],
        grouping_func: Box<dyn Fn(&Row, &Row) -> bool>,
        groups: VecDeque<Vec<Row>>,
        columns: Vec<(String, sql::DataType, AttributeId)>,
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
        outer: &'outer HashMap<AttributeId, storage::Data>,
    },
    OrderBy {
        input: usize,
        attributes: Vec<(usize, sql::OrderBy)>,
        sorted: bool,
        rows: Vec<Row>,
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

impl<'expr, 'outer, 'placeholders, 'ctes, 'stream> core::fmt::Debug for RaInstruction<'expr, 'outer, 'placeholders, 'ctes, 'stream> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRelation { .. } => f.debug_struct("EmptyRelation").finish(),
            Self::BaseRelation { relation, .. } => f.debug_struct("BaseRelation").field("relation", relation).finish(),
            Self::Projection { input, .. } => f.debug_struct("Projection").field("input", input).finish(),
            Self::Selection { input, .. } => f.debug_struct("Selection").field("input", input).finish(),
            Self::Limit { input, limit, offset, .. } => f.debug_struct("Limit").field("inout", input).field("limit", limit).field("offset", offset).finish(),
            Self::CTE { .. }  => f.debug_struct("CTE").finish(),
            Self::Chain { inputs } => f.debug_struct("Chain").field("inputs", inputs).finish(),
            Self::Join { .. } => f.debug_struct("Join").finish(),
            Self::LateralJoin { .. } => f.debug_struct("LateralJoin").finish(),
            other => write!(f, "Other RaInstruction"),
        }
    }
}

pub struct RaVm<'expr, 'outer, 'placeholders, 'ctes, 'stream> {
    instructions: Vec<RaInstruction<'expr, 'outer, 'placeholders, 'ctes, 'stream>>,
    return_stack: Vec<usize>,
    result_stack: Vec<Input>,
}

#[derive(Debug)]
pub enum VmConstructError<SE> {
    Storage(SE),
    ConstructValueMapper(super::EvaulateRaError<SE>),
    Other(&'static str),
}

impl<'expr, 'outer, 'placeholders, 'ctes, 'stream>
    RaVm<'expr, 'outer, 'placeholders, 'ctes, 'stream>
{
    pub fn construct<'e, SE>(
        expr: &'e RaExpression,
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
        outer: &'outer HashMap<AttributeId, storage::Data>,
    ) -> Result<Self, VmConstructError<SE>>
    where
        'e: 'expr,
        SE: core::fmt::Debug,
    {

        let mut pending = vec![expr];
        let mut expression_stack = Vec::new();
        while let Some(tmp) = pending.pop() {
            expression_stack.push(tmp);

            match tmp {
                RaExpression::EmptyRelation | RaExpression::BaseRelation { .. } => {}
                RaExpression::Renamed { inner, .. } => {
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
                RaExpression::LateralJoin { left, .. } => {
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
                    pending.extend(parts.iter().rev());
                }
                RaExpression::CTE { .. } => {}
            };
        }

        let mut instruction_children = Vec::new();
        let mut instructions = Vec::new();
        while let Some(expr) = expression_stack.pop() {
            let (children, instr) = match expr {
                RaExpression::EmptyRelation => (0, RaInstruction::EmptyRelation { used: false }),
                RaExpression::BaseRelation { name, .. } => (0, RaInstruction::BaseRelation {
                    relation: &name.0,
                    stream: None,
                }),
                RaExpression::Projection { attributes, inner } => {
                    let columns: Vec<_> = inner
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, ty, id)| (n, ty, id))
                        .collect();

                    let mut expressions = Vec::new();
                    for attribute in attributes {
                        let mapper = value::Mapper::construct::<SE>(
                            &attribute.value,
                            (&columns, placeholders, ctes, outer),
                        )
                        .map_err(|e| VmConstructError::ConstructValueMapper(e))?;
                        expressions.push(mapper);
                    }

                    let children = instruction_children.last().unwrap();

                    (children + 1, RaInstruction::Projection {
                        expressions,
                        input: instructions.len() - 1,
                        arena: bumpalo::Bump::new(),
                    })
                }
                RaExpression::Selection { inner, filter } => {
                    let columns: Vec<_> = inner
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, ty, id)| (n, ty, id))
                        .collect();

                    let cond = condition::Mapper::construct::<SE>(
                        filter,
                        (&columns, placeholders, ctes, outer),
                    )
                    .map_err(|e| {
                        VmConstructError::Other(concat!("Constructing Mapper - ", line!()))
                    })?;

                    let children = instruction_children.last().unwrap();

                    (children + 1, RaInstruction::Selection {
                        condition: cond,
                        input: instructions.len() - 1,
                        arena: bumpalo::Bump::new(),
                    })
                }
                RaExpression::Limit { limit, offset, .. } => {
                    let children = instruction_children.last().unwrap();

                    (children + 1, RaInstruction::Limit {
                        input: instructions.len() - 1,
                        limit: *limit,
                        offset: *offset,
                        count: 0,
                    })
                },
                RaExpression::Renamed { .. } => continue,
                RaExpression::OrderBy { inner, attributes } => {
                    let columns = inner.get_columns();

                    let mut orderings = Vec::new();
                    for (attr_id, order) in attributes {
                        dbg!(&attr_id, &columns);

                        let idx = match columns
                            .iter()
                            .enumerate()
                            .find(|(_, (_, _, _, id))| id == attr_id)
                        {
                            Some((i, _)) => i,
                            None => todo!(),
                        };

                        orderings.push((idx, order.clone()));
                    }

                    let children = instruction_children.last().unwrap();

                    (children + 1, RaInstruction::OrderBy {
                        attributes: orderings,
                        input: instructions.len() - 1,
                        sorted: false,
                        rows: Vec::new(),
                    })
                }
                RaExpression::Aggregation {
                    inner,
                    attributes,
                    aggregation_condition,
                } => {
                    let columns: Vec<_> = inner
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, t, i)| (n, t, i))
                        .collect();

                    let grouping_func: Box<dyn Fn(&Row, &Row) -> bool> = match aggregation_condition
                    {
                        crate::ra::AggregationCondition::Everything => Box::new(|_, _| true),
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
                                compare_indices
                                    .iter()
                                    .all(|idx| first.data[*idx] == second.data[*idx])
                            })
                        }
                    };

                    let children = instruction_children.last().unwrap();

                    (children + 1, RaInstruction::Aggregation {
                        attributes: &attributes,
                        input: instructions.len() - 1,
                        grouping_func,
                        groups: VecDeque::new(),
                        columns,
                        placeholders,
                        ctes,
                        outer,
                    })
                }
                RaExpression::Join {
                    left,
                    right,
                    kind,
                    condition,
                } => {
                    let left_columns = left.get_columns();
                    let right_columns = right.get_columns();

                    let right_column_count = right_columns.len();

                    let combined_columns: Vec<_> = left_columns
                        .into_iter()
                        .chain(right_columns.into_iter())
                        .map(|(_, n, t, i)| (n, t, i))
                        .collect();

                    let join_cond = condition::Mapper::construct::<SE>(
                        condition,
                        (&combined_columns, placeholders, ctes, outer),
                    )
                    .map_err(|e| {
                        VmConstructError::Other(concat!("Constructing Mapper - ", line!()))
                    })?;

                    let first_child = instruction_children.last().unwrap() + 1;
                    let second_child = instruction_children.get(instruction_children.len() - first_child).unwrap() + 1;

                    (first_child + second_child, RaInstruction::Join {
                        returned_left: false,
                        left_input: instructions.len() - 1 - first_child,
                        right_input: instructions.len() - 1,
                        condition: join_cond,
                        kind: kind.clone(),
                        right_rows: Vec::new(),
                        right_index: 0,
                        right_done: false,
                        right_column_count,
                    })
                }
                RaExpression::LateralJoin {
                    left,
                    right,
                    kind,
                    condition,
                } => {
                    let left_columns = left.get_columns();
                    let right_columns = right.get_columns();

                    let combined_columns: Vec<_> = left_columns
                        .iter()
                        .cloned()
                        .chain(right_columns.into_iter())
                        .map(|(_, n, t, i)| (n, t, i))
                        .collect();

                    let join_cond = condition::Mapper::construct::<SE>(
                        condition,
                        (&combined_columns, placeholders, ctes, outer),
                    )
                    .map_err(|e| {
                        VmConstructError::Other(concat!("Constructing Condition: ", line!()))
                    })?;

                    dbg!(&instruction_children);

                    let first_child = instruction_children.last().unwrap() + 1;

                    (first_child, RaInstruction::LateralJoin {
                        left_ids: left_columns.into_iter().map(|(_, _, _, id)| id).collect(),
                        left_input: instructions.len() - 1,
                        left_row: None,
                        right_rows: Vec::new(),
                        right_expr: *right.clone(),
                        condition: join_cond,
                        kind: kind.clone(),
                        placeholders,
                        outer,
                        ctes,
                    })
                }
                RaExpression::CTE { name, .. } => {
                    let cte = match ctes.get(name) {
                        Some(c) => c,
                        None => return Err(VmConstructError::Other("Getting CTE"))?,
                    };

                    (0, RaInstruction::CTE {
                        rows: cte,
                        part: 0,
                        row: 0,
                    })
                }
                RaExpression::Chain { parts } => {
                    let (children, inputs) = (0..parts.len()).fold((0, Vec::new()), |(children, mut inputs), _| {
                        inputs.push(instructions.len() - 1 - children);
                        let input_child = instruction_children.get(instruction_children.len() - 1 - children).unwrap();

                        (children + input_child + 1, inputs)
                    });

                    (children, RaInstruction::Chain { inputs })
                }
            };

            dbg!(&instr, &children);

            instruction_children.push(children);
            instructions.push(instr);
        }

        dbg!(&instructions);

        let return_stack = Vec::with_capacity(instructions.len());
        let result_stack = vec![Input { done: false }; instructions.len()];

        Ok(Self {
            instructions,
            return_stack,
            result_stack,
        })
    }

    pub async fn get_next<'tg, 'engine, S>(
        &mut self,
        engine: &'engine super::NaiveEngine<S>,
        tguard: &'tg S::TransactionGuard,
    ) -> Option<storage::Row>
    where
        S: storage::Storage,
        'tg: 'stream,
        'engine: 'stream,
        'expr: 'stream,
    {
        let mut idx = self.instructions.len() - 1;

        for input in self.result_stack.iter_mut() {
            input.done = false;
        }

        let mut row = None;

        loop {
            let instr = self.instructions.get_mut(idx).expect("");
            let input = self.result_stack.get_mut(idx).expect("");

            match instr.try_execute(input, &mut row, engine, tguard).await {
                Ok(ExecuteResult::Ok(v)) => {
                    match self.return_stack.pop() {
                        Some(prev_idx) => {
                            let prev_inputs: &mut Input = self.result_stack.get_mut(prev_idx)?;
                            let _ = row.replace(v);
                            prev_inputs.done = false;

                            idx = prev_idx;
                        }
                        None => return Some(v),
                    };
                }
                Ok(ExecuteResult::OkEmpty) => {
                    let prev_idx = self.return_stack.pop()?;
                    let prev_inputs: &mut Input = self.result_stack.get_mut(prev_idx)?;

                    prev_inputs.done = true;

                    idx = prev_idx;
                }
                Ok(ExecuteResult::PendingInput(_)) if input.done => {
                    let prev_idx = self.return_stack.pop()?;
                    let prev_inputs: &mut Input = self.result_stack.get_mut(prev_idx)?;

                    prev_inputs.done = true;

                    idx = prev_idx;
                }
                Ok(ExecuteResult::PendingInput(input_idx)) => {
                    self.return_stack.push(idx);
                    idx = input_idx;
                }
                Err(v) => {
                    dbg!(v);
                    return None;
                }
            };
        }
    }
}

impl<'expr, 'placeholders, 'ctes, 'outer, 'stream>
    RaInstruction<'expr, 'placeholders, 'ctes, 'outer, 'stream>
where
    'expr: 'stream,
{
    async fn try_execute<'tg, 'engine, S>(
        &mut self,
        input_data: &mut Input,
        input_row: &mut Option<Row>,
        engine: &'engine super::NaiveEngine<S>,
        tguard: &'tg S::TransactionGuard,
    ) -> Result<ExecuteResult, ()>
    where
        S: storage::Storage,
        'tg: 'stream,
        'engine: 'stream,
    {
        match self {
            Self::EmptyRelation { used } => {
                dbg!("Empty Relation");

                if *used {
                    Ok(ExecuteResult::OkEmpty)
                } else {
                    *used = true;
                    Ok(ExecuteResult::Ok(storage::Row::new(0, Vec::new())))
                }
            }
            Self::Projection {
                input,
                expressions,
                arena,
            } => {
                dbg!("Projection", &input);

                let row = match input_row.take() {
                    Some(r) => r,
                    None => return Ok(ExecuteResult::PendingInput(*input)),
                };

                dbg!(&row);

                let row = storage::RowCow::Owned(row);

                let mut result = Vec::with_capacity(expressions.len());

                arena.reset();
                for expr in expressions.iter_mut() {
                    let value = expr
                        .evaluate_mut(&row, engine, tguard, &arena)
                        .await
                        .ok_or(())?;
                    result.push(match value {
                        storage::Data::List(mut v) if v.len() == 1 => v.pop().unwrap(),
                        other => other,
                    });
                }

                Ok(ExecuteResult::Ok(storage::Row::new(0, result)))
            }
            Self::Selection {
                input,
                condition,
                arena,
            } => {
                let row = match input_row.take() {
                    Some(r) => r,
                    None => return Ok(ExecuteResult::PendingInput(*input)),
                };
                let row_cow = storage::RowCow::Borrowed(storage::BorrowedRow {
                    rid: row.id(),
                    data: &row.data,
                });

                arena.reset();
                let cond_res = condition
                    .evaluate_mut(&row_cow, engine, tguard, &arena)
                    .await
                    .ok_or(())?;

                if cond_res {
                    Ok(ExecuteResult::Ok(row))
                } else {
                    Ok(ExecuteResult::PendingInput(*input))
                }
            }
            Self::BaseRelation { relation, stream } => {
                let stream = match stream {
                    Some(s) => s,
                    None => {
                        let (_, s) = engine
                            .storage
                            .stream_relation(relation, tguard)
                            .await
                            .map_err(|e| ())?;
                        *stream = Some(s.map(|r| r.into_owned()).fuse().boxed_local());
                        stream.as_mut().unwrap()
                    }
                };

                let tmp = stream.next().await;

                match tmp {
                    Some(r) => Ok(ExecuteResult::Ok(r)),
                    None => Ok(ExecuteResult::OkEmpty),
                }
            }
            Self::Limit {
                input,
                limit,
                offset,
                count,
            } => {
                if count < offset {
                    let tmp = input_row.take();
                    if tmp.is_some() {
                        *count += 1;
                    }

                    return Ok(ExecuteResult::PendingInput(*input));
                }

                if *count >= *offset + *limit {
                    return Ok(ExecuteResult::OkEmpty);
                }

                match input_row.take() {
                    Some(row) => {
                        *count += 1;
                        return Ok(ExecuteResult::Ok(row));
                    }
                    None => return Ok(ExecuteResult::PendingInput(*input)),
                }
            }
            Self::CTE { rows, part, row } => {
                while let Some(part_ref) = rows.parts.get(*part) {
                    let row_ref = match part_ref.rows.get(*row ) {
                        Some(r) => r,
                        None => {
                            *part += 1;
                            *row = 0;
                            continue;
                        }
                    };

                    *row += 1;

                    return Ok(ExecuteResult::Ok(row_ref.clone()));
                }

                Ok(ExecuteResult::OkEmpty)
            }
            Self::Chain { inputs } => {
                dbg!(&inputs);

                dbg!(&input_row);
                if let Some (row) = input_row.take() {
                    return Ok(ExecuteResult::Ok(row))
                }

                dbg!(input_data.done);

                if input_data.done {
                    if !inputs.is_empty() {
                        inputs.remove(0);
                        input_data.done = false;
                    }
                }

                dbg!(input_data.done);

                match inputs.first() {
                    Some(idx) => return Ok(ExecuteResult::PendingInput(*idx)),
                    None => return Ok(ExecuteResult::OkEmpty)
                }
            }
            Self::OrderBy {
                input,
                attributes,
                sorted,
                rows,
            } => {
                dbg!(&rows, &input_data);

                if !input_data.done {
                    if let Some(r) = input_row.take() {
                        rows.push(r);
                    }

                    dbg!(&rows, &input_data);

                    return Ok(ExecuteResult::PendingInput(*input));
                }

                if !*sorted {
                    rows.sort_unstable_by(|first, second| {
                        for (idx, order) in attributes.iter() {
                            let fvalue: &storage::Data = &first.data[*idx];
                            let svalue: &storage::Data = &second.data[*idx];

                            match fvalue.partial_cmp(svalue) {
                                Some(core::cmp::Ordering::Equal) => continue,
                                Some(core::cmp::Ordering::Less) => match order {
                                    sql::OrderBy::Ascending => return core::cmp::Ordering::Greater,
                                    sql::OrderBy::Descending => return core::cmp::Ordering::Less,
                                },
                                Some(core::cmp::Ordering::Greater) => match order {
                                    sql::OrderBy::Ascending => return core::cmp::Ordering::Less,
                                    sql::OrderBy::Descending => {
                                        return core::cmp::Ordering::Greater
                                    }
                                },
                                None => continue,
                            };
                        }

                        core::cmp::Ordering::Equal
                    });
                }
                *sorted = true;

                match rows.pop() {
                    Some(r) => Ok(ExecuteResult::Ok(r)),
                    None => Ok(ExecuteResult::OkEmpty),
                }
            }
            Self::Aggregation {
                input,
                attributes,
                grouping_func,
                groups,
                columns,
                placeholders,
                ctes,
                outer,
            } => {
                if !input_data.done {
                    match input_row.take() {
                        Some(row) => {
                            for group in groups.iter_mut() {
                                let rep = group
                                    .first()
                                    .expect("Every group has at least one member row");

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
                    let state = super::AggregateState::new::<S::LoadingError>(
                        &attr.value,
                        &columns,
                        placeholders,
                        ctes,
                        outer,
                    );
                    states.push(state);
                }

                let arena = bumpalo::Bump::new();
                for row in group {
                    let row = storage::RowCow::Owned(row);
                    for state in states.iter_mut() {
                        state.update(engine, &row, tguard, &arena).await.unwrap();
                    }
                }

                let resulting_row: Vec<_> =
                    states.into_iter().map(|r| r.to_data().unwrap()).collect();

                Ok(ExecuteResult::Ok(storage::Row::new(0, resulting_row)))
            }
            Self::Join {
                returned_left,
                left_input,
                right_input,
                condition,
                kind,
                right_rows,
                right_done,
                right_index,
                right_column_count,
            } => {
                if !*right_done {
                    if !input_data.done {
                        if let Some(row) = input_row.take() {
                            right_rows.push(row);
                        }

                        *right_index = 0;
                        *returned_left = false;
                        return Ok(ExecuteResult::PendingInput(*right_input));
                    } else {
                        *right_done = true;
                        input_data.done = false;
                        *returned_left = false;
                        return Ok(ExecuteResult::PendingInput(*left_input));
                    }
                }

                let left_row = match input_row.as_ref() {
                    Some(r) => r,
                    None => {
                        *returned_left = false;
                        *right_index = 0;
                        return Ok(ExecuteResult::PendingInput(*left_input));
                    }
                };

                let arena = bumpalo::Bump::new();
                match kind {
                    sql::JoinKind::Inner => {
                        while *right_index < right_rows.len() {
                            let right = right_rows.get(*right_index).unwrap();
                            let tmp_row = storage::Row::new(
                                0,
                                left_row
                                    .data
                                    .iter()
                                    .cloned()
                                    .chain(right.data.iter().cloned())
                                    .collect(),
                            );
                            let row = storage::RowCow::Owned(tmp_row);

                            *right_index += 1;

                            if condition
                                .evaluate_mut(&row, engine, tguard, &arena)
                                .await
                                .unwrap_or(false)
                            {
                                return Ok(ExecuteResult::Ok(row.into_owned()));
                            }
                        }
                        input_row.take();

                        Ok(ExecuteResult::PendingInput(*left_input))
                    }
                    sql::JoinKind::LeftOuter => {
                        while *right_index < right_rows.len() {
                            let right = right_rows.get(*right_index).unwrap();
                            let tmp_row = storage::Row::new(
                                0,
                                left_row
                                    .data
                                    .iter()
                                    .cloned()
                                    .chain(right.data.iter().cloned())
                                    .collect(),
                            );
                            let row = storage::RowCow::Owned(tmp_row);

                            *right_index += 1;

                            if condition
                                .evaluate_mut(&row, engine, tguard, &arena)
                                .await
                                .unwrap_or(false)
                            {
                                *returned_left = true;
                                return Ok(ExecuteResult::Ok(row.into_owned()));
                            }
                        }

                        if !*returned_left {
                            *returned_left = true;
                            let row = storage::Row::new(
                                0,
                                left_row
                                    .data
                                    .iter()
                                    .cloned()
                                    .chain((0..*right_column_count).map(|_| storage::Data::Null))
                                    .collect(),
                            );
                            return Ok(ExecuteResult::Ok(row));
                        }

                        input_row.take();

                        Ok(ExecuteResult::PendingInput(*left_input))
                    }
                    sql::JoinKind::RightOuter => todo!("Right Outer Join"),
                    sql::JoinKind::FullOuter => todo!("Full Outer Join"),
                    sql::JoinKind::Cross => todo!("Cross Join"),
                }
            }
            Self::LateralJoin {
                left_ids,
                left_input,
                left_row,
                right_rows,
                right_expr,
                condition,
                kind,
                ctes,
                outer,
                placeholders,
            } => {
                if right_rows.is_empty() {
                    left_row.take();
                }

                if left_row.is_none() {
                    match input_row.take() {
                        Some(r) => {
                            *left_row = Some(r);
                        }
                        None => return Ok(ExecuteResult::PendingInput(*left_input)),
                    }
                }

                let left_row = left_row.as_ref().expect("");

                if right_rows.is_empty() {
                    let mut outer = outer.clone();
                    for (idx, id) in left_ids.iter().enumerate() {
                        outer.insert(id.clone(), left_row.data[idx].clone());
                    }

                    let mut right_vm =
                        RaVm::construct::<S::LoadingError>(right_expr, placeholders, ctes, &outer)
                            .expect("");
                    async {
                        while let Some(n_row) = right_vm.get_next(engine, tguard).await {
                            right_rows.push(n_row);
                        }
                    }
                    .boxed_local()
                    .await;
                }

                let arena = bumpalo::Bump::new();
                match kind {
                    sql::JoinKind::Inner => {
                        while !right_rows.is_empty() {
                            let right = right_rows.remove(0);
                            let tmp_row = storage::Row::new(
                                0,
                                left_row
                                    .data
                                    .iter()
                                    .cloned()
                                    .chain(right.data.into_iter())
                                    .collect(),
                            );
                            let row = storage::RowCow::Owned(tmp_row);

                            if condition
                                .evaluate_mut(&row, engine, tguard, &arena)
                                .await
                                .unwrap_or(false)
                            {
                                return Ok(ExecuteResult::Ok(row.into_owned()));
                            }
                        }

                        Ok(ExecuteResult::PendingInput(*left_input))
                    }
                    sql::JoinKind::LeftOuter => todo!("Left Outer Join"),
                    sql::JoinKind::RightOuter => todo!("Right Outer Join"),
                    sql::JoinKind::FullOuter => todo!("Full Outer Join"),
                    sql::JoinKind::Cross => {
                        if !right_rows.is_empty() {
                            let right = right_rows.remove(0);
                            let row = storage::Row::new(
                                0,
                                left_row
                                    .data
                                    .iter()
                                    .cloned()
                                    .chain(right.data.into_iter())
                                    .collect(),
                            );

                            return Ok(ExecuteResult::Ok(row));
                        }

                        Ok(ExecuteResult::PendingInput(*left_input))
                    }
                }
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
