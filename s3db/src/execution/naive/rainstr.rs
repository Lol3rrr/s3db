use super::{condition, value};
use crate::ra;

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
                todo!("Order By");
            }
            ra::RaExpression::Aggregation {
                inner,
                attributes,
                aggregation_condition,
            } => {
                todo!("Aggregation")
            }
            ra::RaExpression::Join {
                left,
                right,
                kind,
                condition,
            } => {
                todo!("Join")
            }
            ra::RaExpression::LateralJoin {
                left,
                right,
                kind,
                condition,
            } => {
                todo!("LateralJoin")
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
