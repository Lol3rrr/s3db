use super::{condition, value};

use std::collections::HashMap;

use futures::stream::StreamExt;

pub struct RaVmInstruction<S> {
    _marker: core::marker::PhantomData<S>,
}

impl<'i, S> vm::Instruction<'i> for RaVmInstruction<S>
where
    S: storage::Storage,
    S::TransactionGuard: 'i,
    S: 'i,
{
    type ConstructError = ();
    type Input = ra::RaExpression;
    type Value = storage::Row;
    type Arguments<'args> = (
        &'args HashMap<usize, storage::Data>,
        &'args HashMap<String, storage::EntireRelation>,
        &'args HashMap<ra::AttributeId, storage::Data>,
        &'args super::NaiveEngine<S>,
        &'args S::TransactionGuard,
    ) where 'args: 'i, Self: 'args;

    async fn construct<'p, 'args>(
        input: &'i ra::RaExpression,
        pending: &'p mut Vec<(&'i Self::Input, vm::Output<Self::Value>)>,
        mut out: vm::Output<Self::Value>,
        ctx: &mut vm::ConstructionContext<Self::Value>,
        &(placeholders, ctes, outer, engine, tguard): &Self::Arguments<'args>,
    ) -> Result<Box<dyn core::future::Future<Output = ()> + 'i>, Self::ConstructError>
    where
        'args: 'i,
    {
        match input {
            ra::RaExpression::EmptyRelation { .. } => Ok(Box::new(async move {
                out.store(storage::Row::new(0, Vec::new())).await;
            })),
            ra::RaExpression::BaseRelation { name, .. } => {
                let (_, mut relation) = engine
                    .storage
                    .stream_relation(&name.0, tguard)
                    .await
                    .map_err(|e| ())?;

                Ok(Box::new(async move {
                    while let Some(r) = relation.next().await {
                        out.store(r.into_owned()).await;
                    }
                }))
            }
            ra::RaExpression::Projection {
                inner, attributes, ..
            } => {
                let columns: Vec<_> = inner
                    .get_columns()
                    .into_iter()
                    .map(|(_, n, ty, id)| (n, ty, id))
                    .collect();

                let mut expressions = Vec::new();
                for attribute in attributes {
                    let mapper = value::Mapper::construct::<()>(
                        &attribute.value,
                        (&columns, placeholders, ctes, outer),
                    )
                    .map_err(|e| ())?;
                    expressions.push(mapper);
                }

                let (mut input, output) = ctx.io();

                pending.push((inner, output));

                Ok(Box::new(async move {
                    let mut arena = bumpalo::Bump::new();

                    while let Some(row) = input.get_value().await {
                        arena.reset();

                        let mut result = Vec::with_capacity(expressions.len());

                        let borrowed_row = storage::RowCow::Borrowed(storage::BorrowedRow {
                            rid: row.id(),
                            data: &row.data,
                        });

                        for expr in expressions.iter_mut() {
                            let value = match expr
                                .evaluate_mut(&borrowed_row, engine, tguard, &arena)
                                .await
                            {
                                Some(v) => v,
                                None => return,
                            };
                            result.push(match value {
                                storage::Data::List(mut v) if v.len() == 1 => v.pop().unwrap(),
                                other => other,
                            });
                        }

                        out.store(storage::Row::new(0, result)).await;
                    }
                }))
            }
            ra::RaExpression::Selection { inner, filter } => {
                let columns: Vec<_> = inner
                    .get_columns()
                    .into_iter()
                    .map(|(_, n, ty, id)| (n, ty, id))
                    .collect();

                let mut cond = condition::Mapper::construct::<()>(
                    filter,
                    (&columns, placeholders, ctes, outer),
                )
                .map_err(|e| ())?;

                let (mut input, output) = ctx.io();

                pending.push((inner, output));

                Ok(Box::new(async move {
                    let mut arena = bumpalo::Bump::new();

                    while let Some(row) = input.get_value().await {
                        arena.reset();

                        let row_cow = storage::RowCow::Borrowed(storage::BorrowedRow {
                            rid: row.id(),
                            data: &row.data,
                        });

                        let cond_res =
                            match cond.evaluate_mut(&row_cow, engine, tguard, &arena).await {
                                Some(v) => v,
                                None => return,
                            };

                        if cond_res {
                            out.store(row).await;
                        }
                    }
                }))
            }
            ra::RaExpression::Limit {
                inner,
                limit,
                offset,
            } => {
                let (mut input, output) = ctx.io();

                pending.push((inner, output));

                Ok(Box::new(async move {
                    for _ in 0..*offset {
                        match input.get_value().await {
                            Some(_) => {}
                            None => return,
                        };
                    }

                    for _ in 0..*limit {
                        match input.get_value().await {
                            Some(v) => {
                                out.store(v).await;
                            }
                            None => return,
                        };
                    }
                }))
            }
            ra::RaExpression::Renamed { inner, .. } => {
                let (mut input, output) = ctx.io();

                pending.push((inner, output));

                Ok(Box::new(async move {
                    while let Some(v) = input.get_value().await {
                        out.store(v).await;
                    }
                }))
            }
            ra::RaExpression::OrderBy { inner, attributes } => {
                let columns = inner.get_columns();

                let mut orderings = Vec::new();
                for (attr_id, order) in attributes {
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

                let (mut input, output) = ctx.io();

                pending.push((inner, output));

                Ok(Box::new(async move {
                    let mut rows = Vec::new();
                    while let Some(r) = input.get_value().await {
                        rows.push(r);
                    }

                    rows.sort_unstable_by(|first, second| {
                        for (idx, order) in orderings.iter() {
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

                    for row in rows.into_iter().rev() {
                        out.store(row).await;
                    }
                }))
            }
            ra::RaExpression::Aggregation {
                inner,
                attributes,
                aggregation_condition,
            } => {
                let columns: Vec<_> = inner
                    .get_columns()
                    .into_iter()
                    .map(|(_, n, t, i)| (n, t, i))
                    .collect();

                let grouping_func: Box<dyn Fn(&storage::Row, &storage::Row) -> bool> =
                    match aggregation_condition {
                        ra::AggregationCondition::Everything => Box::new(|_, _| true),
                        ra::AggregationCondition::GroupBy { fields } => {
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

                let (mut input, output) = ctx.io();

                pending.push((inner, output));

                Ok(Box::new(async move {
                    let mut groups: Vec<Vec<_>> = Vec::new();

                    while let Some(row) = input.get_value().await {
                        match groups.iter_mut().find(|g| grouping_func(&g[0], &row)) {
                            Some(v) => {
                                v.push(row);
                            }
                            None => {
                                groups.push(vec![row]);
                            }
                        };
                    }

                    for group in groups {
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

                        out.store(storage::Row::new(0, resulting_row)).await;
                    }
                }))
            }
            ra::RaExpression::Join {
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

                let mut join_cond = condition::Mapper::construct::<()>(
                    condition,
                    (&combined_columns, placeholders, ctes, outer),
                )
                .map_err(|e| ())?;

                let (mut left_in, left_out) = ctx.io();
                let (mut right_in, right_out) = ctx.io();

                pending.push((left, left_out));
                pending.push((right, right_out));

                Ok(Box::new(async move {
                    let mut right_rows = Vec::new();
                    while let Some(r) = right_in.get_value().await {
                        right_rows.push(r);
                    }

                    let mut arena = bumpalo::Bump::new();
                    while let Some(left_row) = left_in.get_value().await {
                        arena.reset();

                        match kind {
                            sql::JoinKind::Inner => {
                                for right in right_rows.iter() {
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

                                    if join_cond
                                        .evaluate_mut(&row, engine, tguard, &arena)
                                        .await
                                        .unwrap_or(false)
                                    {
                                        out.store(row.into_owned()).await;
                                    }
                                }
                            }
                            sql::JoinKind::LeftOuter => {
                                let mut returned = false;
                                for right in right_rows.iter() {
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

                                    if join_cond
                                        .evaluate_mut(&row, engine, tguard, &arena)
                                        .await
                                        .unwrap_or(false)
                                    {
                                        out.store(row.into_owned()).await;
                                        returned = true;
                                    }
                                }

                                if !returned {
                                    let row = storage::Row::new(
                                        0,
                                        left_row
                                            .data
                                            .iter()
                                            .cloned()
                                            .chain(
                                                (0..right_column_count)
                                                    .map(|_| storage::Data::Null),
                                            )
                                            .collect(),
                                    );
                                    out.store(row).await;
                                }
                            }
                            sql::JoinKind::RightOuter => todo!("Right Outer Join"),
                            sql::JoinKind::FullOuter => todo!("Full Outer Join"),
                            sql::JoinKind::Cross => todo!("Cross Join"),
                        };
                    }
                }))
            }
            ra::RaExpression::LateralJoin {
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

                let mut join_cond = condition::Mapper::construct::<()>(
                    condition,
                    (&combined_columns, placeholders, ctes, outer),
                )
                .map_err(|e| ())?;

                let left_ids: Vec<_> = left_columns.into_iter().map(|(_, _, _, id)| id).collect();

                let (mut left_in, left_out) = ctx.io();

                pending.push((left, left_out));

                Ok(Box::new(async move {
                    let mut arena = bumpalo::Bump::new();

                    while let Some(left_row) = left_in.get_value().await {
                        let mut outer = outer.clone();
                        for (idx, id) in left_ids.iter().enumerate() {
                            outer.insert(id.clone(), left_row.data[idx].clone());
                        }

                        let right_ctx = (placeholders, ctes, &outer, engine, tguard);
                        let mut right_vm = ::vm::VM::construct::<Self>(right, &right_ctx)
                            .await
                            .unwrap();

                        match kind {
                            sql::JoinKind::Inner => {
                                while let Some(right) = right_vm.next().await {
                                    arena.reset();
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

                                    if join_cond
                                        .evaluate_mut(&row, engine, tguard, &arena)
                                        .await
                                        .unwrap_or(false)
                                    {
                                        out.store(row.into_owned()).await;
                                    }
                                }
                            }
                            sql::JoinKind::LeftOuter => todo!("Left Outer Join"),
                            sql::JoinKind::RightOuter => todo!("Right Outer Join"),
                            sql::JoinKind::FullOuter => todo!("Full Outer Join"),
                            sql::JoinKind::Cross => {
                                while let Some(right) = right_vm.next().await {
                                    let row = storage::Row::new(
                                        0,
                                        left_row
                                            .data
                                            .iter()
                                            .cloned()
                                            .chain(right.data.into_iter())
                                            .collect(),
                                    );

                                    out.store(row).await;
                                }
                            }
                        };
                    }
                }))
            }
            ra::RaExpression::CTE { name, .. } => {
                let relation = ctes.get(name).ok_or(())?;

                Ok(Box::new(async move {
                    for part in &relation.parts {
                        for row in &part.rows {
                            out.store(row.clone()).await;
                        }
                    }
                }))
            }
            ra::RaExpression::Chain { parts } => {
                let mut inputs = Vec::with_capacity(parts.len());
                for part in parts {
                    let (input, output) = ctx.io();

                    pending.push((part, output));
                    inputs.push(input);
                }

                Ok(Box::new(async move {
                    for mut input in inputs {
                        while let Some(row) = input.get_value().await {
                            out.store(row).await;
                        }
                    }
                }))
            }
        }
    }
}
