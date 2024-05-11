use futures::future::{FutureExt, LocalBoxFuture};
use std::collections::HashMap;

use ra::AttributeId;

use sql::DataType;
use storage::{self, Storage};

use super::{value, EvaulateRaError, NaiveEngine};

#[derive(Debug)]
pub enum AggregateState<'expr, 'outer, 'placeholders, 'ctes> {
    CountRows {
        attribute_index: Option<usize>,
        count: i64,
    },
    Column {
        attribute_index: usize,
        value: Option<storage::Data>,
    },
    Max {
        value: Option<storage::Data>,
        expr: value::Mapper<'expr, 'outer, 'placeholders, 'ctes>,
    },
}

impl<'expr, 'outer, 'placeholders, 'ctes> AggregateState<'expr, 'outer, 'placeholders, 'ctes> {
    pub fn new<SE>(
        expr: &'expr ra::AggregateExpression,
        columns: &[(String, DataType, ra::AttributeId)],
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'ctes HashMap<String, storage::EntireRelation>,
        outer: &'outer HashMap<AttributeId, storage::Data>,
    ) -> Self {
        match expr {
            ra::AggregateExpression::CountRows => Self::CountRows {
                attribute_index: None,
                count: 0,
            },
            ra::AggregateExpression::Count { a_id } => Self::CountRows {
                attribute_index: Some(
                    columns
                        .iter()
                        .enumerate()
                        .find(|(_, (_, _, id))| a_id == id)
                        .map(|(i, _)| i)
                        .unwrap(),
                ),
                count: 0,
            },
            ra::AggregateExpression::Column { a_id, .. } => Self::Column {
                attribute_index: columns
                    .iter()
                    .enumerate()
                    .find(|(_, (_, _, id))| a_id == id)
                    .map(|(i, _)| i)
                    .unwrap(),
                value: None,
            },
            ra::AggregateExpression::Renamed { inner, .. } => {
                Self::new::<SE>(inner, columns, placeholders, ctes, outer)
            }
            ra::AggregateExpression::Max { inner, .. } => Self::Max {
                value: None,
                expr: value::Mapper::construct::<SE>(&inner, (columns, placeholders, ctes, outer))
                    .map_err(|e| ())
                    .unwrap(),
            },
            other => todo!("{:?}", other),
        }
    }

    pub fn update<'s, 'engine, 'row, 'transaction, 'arena, 'f, S>(
        &'s mut self,
        engine: &'engine NaiveEngine<S>,
        row: &'row storage::RowCow<'_>,
        transaction: &'transaction S::TransactionGuard,
        arena: &'arena bumpalo::Bump,
    ) -> LocalBoxFuture<'f, Result<(), EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'engine: 'f,
        'row: 'f,
        'outer: 'f,
        'transaction: 'f,
        'arena: 'f,
        S: Storage,
    {
        async move {
            match self {
                Self::CountRows {
                    attribute_index,
                    count,
                } => {
                    if let Some(idx) = attribute_index {
                        let value = &row.as_ref()[*idx];
                        if value == &storage::Data::Null {
                            return Ok(());
                        }
                    }

                    *count += 1;
                    Ok(())
                }
                Self::Column {
                    attribute_index,
                    value,
                } => {
                    let row_value = &row.as_ref()[*attribute_index];

                    match &value {
                        Some(v) => {
                            assert_eq!(v, row_value);
                            Ok(())
                        }
                        None => {
                            *value = Some(row_value.clone());
                            Ok(())
                        }
                    }
                }
                Self::Max { value, expr, .. } => {
                    let tmp = expr
                        .evaluate_mut(row, engine, transaction, arena)
                        .await
                        .ok_or_else(|| EvaulateRaError::Other("Test"))?;

                    match value.as_mut() {
                        Some(v) => {
                            if &tmp > v {
                                *v = tmp;
                            }
                        }
                        None => {
                            *value = Some(tmp);
                        }
                    };

                    Ok(())
                }
            }
        }
        .boxed_local()
    }

    pub fn to_data(self) -> Option<storage::Data> {
        match self {
            Self::CountRows { count, .. } => Some(storage::Data::BigInt(count)),
            Self::Column { value, .. } => value,
            Self::Max { value, .. } => value,
        }
    }
}
