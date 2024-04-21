use futures::future::{FutureExt, LocalBoxFuture};
use std::collections::HashMap;

use crate::{
    ra::{self, AttributeId, RaValueExpression},
    storage::{self, Storage},
};
use sql::DataType;

use super::{value::evaluate_ra_value, EvaulateRaError, NaiveEngine};

pub enum AggregateState {
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
        expr: Box<RaValueExpression>,
        columns: Vec<(String, DataType, ra::AttributeId)>,
    },
}

impl AggregateState {
    pub fn new(
        expr: &ra::AggregateExpression,
        columns: &[(String, DataType, ra::AttributeId)],
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
            ra::AggregateExpression::Renamed { inner, .. } => Self::new(inner, columns),
            ra::AggregateExpression::Max { inner, .. } => Self::Max {
                value: None,
                expr: inner.clone(),
                columns: columns.to_vec(),
            },
            other => todo!("{:?}", other),
        }
    }

    pub fn update<'s, 'engine, 'row, 'outer, 'transaction, 'arena, 'f, S>(
        &'s mut self,
        engine: &'engine NaiveEngine<S>,
        row: &'row storage::Row,
        outer: &'outer HashMap<AttributeId, storage::Data>,
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
                        let value = &row.data[*idx];
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
                    let row_value = &row.data[*attribute_index];

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
                Self::Max {
                    value,
                    expr,
                    columns,
                    ..
                } => {
                    let tmp = evaluate_ra_value(
                        engine,
                        expr,
                        row,
                        columns,
                        &HashMap::new(),
                        &HashMap::new(),
                        outer,
                        transaction,
                        arena,
                    )
                    .await?;

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
