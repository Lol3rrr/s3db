//! A naive execution engine, that is mostly there to have a correct implementation of all the
//! parts needed, however it is not very efficient or performant

use std::collections::HashMap;

use futures::{future::LocalBoxFuture, FutureExt};

use crate::{
    execution::algorithms::{self, joins::Join}, postgres::FormatCode, ra::{self, AttributeId, RaExpression, RaUpdate}, storage::{self, Data, Storage, TableSchema}
};
use sql::{ BinaryOperator,  DataType, Query, TypeModifier, CompatibleParser};

use super::{Context, CopyState, Execute, ExecuteResult, PreparedStatement};

mod aggregate;
use aggregate::AggregateState;

mod pattern;

pub struct NaiveEngine<S> {
    storage: S,
}

pub struct NaiveEngineConditionEval<'engine, 'columns, 'placeholders, 'ctes, 'outer, 'transaction,S, TG> {
    engine: &'engine NaiveEngine<S>,
    columns: &'columns [(String, DataType, AttributeId)],
    placeholders: &'placeholders HashMap<usize, Data>,
    ctes: &'ctes HashMap<String, storage::EntireRelation>,
    outer: &'outer HashMap<AttributeId, storage::Data>,
    transaction: &'transaction TG,
}

impl<S> algorithms::joins::EvaluateConditions<S::LoadingError> for NaiveEngineConditionEval<'_, '_, '_, '_, '_, '_, S, S::TransactionGuard> where S: Storage {
    async fn evaluate(
                &self,
                condition: &ra::RaCondition,
                row: &storage::Row,
            ) -> Result<bool, EvaulateRaError<S::LoadingError>> {
        self.engine.evaluate_ra_cond(condition, row, self.columns, self.placeholders, self.ctes, self.outer, self.transaction).await
    }
}

#[derive(Debug)]
pub struct NaivePrepared {
    query: Query<'static>,
    expected_parameters: Vec<DataType>,
    columns: Vec<(String, DataType)>,
}

#[derive(Debug)]
pub struct NaiveBound {
    query: Query<'static>,
    expected_parameters: Vec<DataType>,
    values: Vec<Vec<u8>>,
    result_columns: Vec<FormatCode>,
}

pub struct NaiveCopyState<'engine, S, T> {
    table: String,
    engine: &'engine S,
    schema: TableSchema,
    tx: &'engine T,
}

impl<S> NaiveEngine<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

#[derive(Debug)]
pub enum EvaulateRaError<SE> {
    UnknownAttribute {
        name: String,
        id: AttributeId,
    },
    CastingType {
        value: storage::Data,
        target: DataType,
    },
    StorageError(SE),
    Other(&'static str),
}

struct StackedExpr<'r> {
    expr: &'r RaExpression,
}

impl<S> NaiveEngine<S>
where
    S: storage::Storage,
{
    fn evaluate_ra<'s, 'p, 'f, 'o, 'c, 't>(
        &'s self,
        expr: ra::RaExpression,
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
        outer: &'o HashMap<AttributeId, storage::Data>,
        transaction: &'t S::TransactionGuard,
    ) -> LocalBoxFuture<'f, Result<storage::EntireRelation, EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'p: 'f,
        'c: 'f,
        'o: 'f,
        't: 'f,
    {
        async move {
            let mut pending: Vec<&RaExpression> = vec![&expr];
            let mut expression_stack: Vec<StackedExpr> = Vec::new();
            while let Some(tmp) = pending.pop() {
                expression_stack.push(StackedExpr {
                    expr: tmp,
                });
                match &tmp {
                    ra::RaExpression::Renamed { inner, .. } => { 
                        pending.push(inner);
                    }
                    ra::RaExpression::EmptyRelation => {}
                    ra::RaExpression::BaseRelation { .. } => {}
                    ra::RaExpression::Projection { inner, .. } => {
                        pending.push(inner);
                    }
                    ra::RaExpression::Selection { inner, .. } => {
                        pending.push(inner);
                    }
                    ra::RaExpression::Join { left, right, lateral, .. } => {
                        pending.push(left);
                        pending.push(right);
                    }
                    ra::RaExpression::Aggregation { inner, .. } => {
                        pending.push(inner);
                    }
                    ra::RaExpression::Limit { inner, ..} => {
                        pending.push(inner);
                    }
                    ra::RaExpression::OrderBy { inner, .. } => {
                        pending.push(inner);
                    }
                    ra::RaExpression::Chain { parts } => {
                        pending.extend(parts.iter());
                    }
                    ra::RaExpression::CTE { .. } => {}
                };
            }

            let mut results: Vec<storage::EntireRelation> = Vec::with_capacity(expression_stack.len());
            for stacked_expr in expression_stack.into_iter().rev() {
                let expr = stacked_expr.expr;

                let result = match expr {
                    ra::RaExpression::EmptyRelation => {
                        storage::EntireRelation {
                            columns: Vec::new(),
                            parts: vec![storage::PartialRelation {
                                rows: vec![storage::Row::new(0, Vec::new())]
                            }],
                        }
                    }
                    ra::RaExpression::Renamed {  .. } => {
                        results.pop().unwrap()
                    }
                    ra::RaExpression::BaseRelation { name, columns } => {
                        self.storage
                        .get_entire_relation(&name.0, transaction)
                        .await
                        .map_err(EvaulateRaError::StorageError)?
                    }
                    ra::RaExpression::CTE { name, columns } => {
                        let cte_value = ctes.get(name).ok_or_else(|| EvaulateRaError::Other("Getting CTE"))?;

                        storage::EntireRelation { columns: cte_value.columns.clone(), parts: cte_value.parts.iter().map(|part| {
                            storage::PartialRelation {
                                rows: part.rows.clone(),
                            }
                        }).collect() }
                    }
                    ra::RaExpression::Projection { inner, attributes } => {
                        let input = results.pop().unwrap();

                        

                        let inner_columns = inner.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)).collect::<Vec<_>>();

                        assert_eq!(
                            inner_columns
                                .iter()
                                .map(|(n, t, _)| (n, t))
                                .collect::<Vec<_>>(),
                            input
                                .columns
                                .iter()
                                .map(|(n, t, _)| (n, t))
                                .collect::<Vec<_>>()
                        );

                        let columns: Vec<_> = attributes
                            .iter()
                            .map(|attr| {
                                (attr.name.clone(), attr.value.datatype().unwrap(), Vec::new())
                            })
                            .collect();

                        let mut rows = Vec::new();
                        for row in input.into_rows() {
                            let mut data = Vec::new();
                            for attribute in attributes.iter() {
                                let tmp = self.evaluate_ra_value(&attribute.value, &row, &inner_columns, placeholders, ctes, &outer, transaction).await?;

                                // TODO
                                // This is not a good fix
                                let tmp = match tmp {
                                    Data::List(mut v) => {
                                        assert_eq!(1, v.len());
                                        v.pop().unwrap()
                                    }
                                    other => other,
                                };

                                data.push(tmp);
                            }

                            rows.push(storage::Row::new(0, data));
                        }

                        storage::EntireRelation {
                            columns,
                            parts: vec![storage::PartialRelation { rows }],
                        }
                    }
                    ra::RaExpression::Selection { inner, filter } => {
                        let input = results.pop().unwrap();

                        let inner_columns = inner.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)).collect::<Vec<_>>();

                    assert_eq!(
                        inner_columns
                            .iter()
                            .map(|(n, t, _)| (n, t))
                            .collect::<Vec<_>>(),
                        input
                            .columns
                            .iter()
                            .map(|(n, t, _)| (n, t))
                            .collect::<Vec<_>>()
                    );

                    let result = {
                        let mut tmp = Vec::new();

                        for row in input
                            .parts
                            .into_iter()
                            .flat_map(|i| i.rows.into_iter())
                        {
                            // TODO
                            tracing::trace!("Row: {:?}", row);

                            let include_row = self
                                .evaluate_ra_cond(
                                    filter,
                                    &row,
                                    &inner_columns,
                                    placeholders,
                                    ctes,
                                    outer,
                                    transaction
                                )
                                .await?;

                            if include_row {
                                tmp.push(row);
                            }
                        }

                        tmp
                    };

                    storage::EntireRelation {
                        columns: input.columns,
                        parts: vec![storage::PartialRelation { rows: result }],
                    }
                    }
                    ra::RaExpression::Aggregation { inner, attributes, aggregation_condition } => {
                        let input = results.pop().unwrap();

                        let inner_columns = inner.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)).collect::<Vec<_>>();

                    let result_columns: Vec<_> = attributes
                                .iter()
                                .map(|attr| (attr.name.clone(), attr.value.return_ty(), Vec::new()))
                                .collect();

                    match aggregation_condition {
                        ra::AggregationCondition::Everything => {
                            let mut states: Vec<_> = attributes
                                .iter()
                                .map(|attr| AggregateState::new(&attr.value, &inner_columns))
                                .collect();

                            for row in input
                                .parts
                                .into_iter()
                                .flat_map(|p| p.rows.into_iter())
                            {
                                for state in states.iter_mut() {
                                    state.update(self, &row, outer, transaction).await?;
                                }
                            }

                            let row_data: Vec<_> = states
                                .into_iter()
                                .map(|s| s.to_data().ok_or(EvaulateRaError::Other("Converting AggregateState to Data")))
                                .collect::<Result<_, _>>()?;

                            storage::EntireRelation {
                                columns: result_columns,
                                parts: vec![storage::PartialRelation {
                                    rows: vec![storage::Row::new(0, row_data)],
                                }],
                            }
                        }
                        ra::AggregationCondition::GroupBy { fields } => {
                            let groups: Vec<Vec<storage::Row>> = {
                                let mut tmp: Vec<Vec<storage::Row>> = Vec::new();

                                let compare_indices : Vec<_>= fields.iter().map(|(_, src_id)| {
                                    inner_columns.iter().enumerate().find(|(_, (_, _, c_id))| c_id == src_id).map(|(i, _)| i).unwrap()
                                }).collect();
                                let grouping_func =
                                    |first: &storage::Row, second: &storage::Row| {
                                        compare_indices.iter().all(|idx| first.data[*idx] == second.data[*idx])
                                    };

                                for row in input.parts.iter().flat_map(|p| p.rows.iter()) {
                                    let mut grouped = false;
                                    for group in tmp.iter_mut() {
                                        let group_row = group.first().expect("We only insert Groups with at least one Row in them, so every group must have a first element");

                                        if grouping_func(row, group_row) {
                                            grouped = true;
                                            group.push(row.clone());

                                            break;
                                        }
                                    }

                                    if !grouped {
                                        tmp.push(vec![row.clone()]);
                                    }
                                }

                                tmp
                            };


                            let mut result_rows: Vec<storage::Row> = Vec::new();
                            for group in groups.into_iter() {
                                let mut states: Vec<_> = attributes.iter().map(|attr| AggregateState::new(&attr.value, &inner_columns)).collect();

                                for row in group {
                                    for state in states.iter_mut() {
                                        state.update(self, &row, outer, transaction).await?;
                                    }
                                }

                                let row_data: Vec<_> = states
                                    .into_iter()
                                    .map(|s| s.to_data().ok_or(EvaulateRaError::Other("Converting AggregateState to Data")))
                                    .collect::<Result<_, _>>()?;

                                result_rows.push(storage::Row::new(0, row_data));
                            }


                            storage::EntireRelation {
                                columns: result_columns,
                                parts: vec![storage::PartialRelation {
                                    rows: result_rows,
                                }]
                            }
                        }
                    }
                    }
                    ra::RaExpression::Chain { parts } => {
                        let mut columns = Vec::new();

                        let mut part_results = Vec::new();
                        for _part in parts {
                            let input = results.pop().unwrap();

                            columns.clone_from(&input.columns);
                            part_results.push(input.parts);
                        }

                        part_results.reverse();


                        storage::EntireRelation { columns, parts: part_results.into_iter().flatten().collect() }
                    }
                    ra::RaExpression::OrderBy { inner, attributes } => {
                        let inner_columns = inner.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)).collect::<Vec<_>>();
                        let input = results.pop().unwrap();

                        let orders: Vec<(usize, sql::OrderBy)> = attributes.iter().map(|(id, order)| {
                            let attribute_idx = inner_columns.iter().enumerate().find(|( _, column)| id == &column.2).map(|( idx, _)| idx).ok_or_else(|| EvaulateRaError::Other("Could not find Attribute"))?;

                            Ok((attribute_idx, order.clone()))
                        }).collect::<Result<_, _>>()?;

                    let mut result_rows: Vec<_> = input.parts.into_iter().flat_map(|p| p.rows.into_iter()).collect();

                    result_rows.sort_unstable_by(|first, second| {
                        for (attribute_idx, order) in orders.iter() {
                            let first_value = &first.data[*attribute_idx];
                            let second_value = &second.data[*attribute_idx];

                            let (first_value, second_value) = match order {
                                sql::OrderBy::Ascending => (first_value, second_value),
                                sql::OrderBy::Descending => (second_value, first_value),
                            };

                            if core::mem::discriminant::<storage::Data>(first_value) == core::mem::discriminant(second_value) {
                                match first_value.partial_cmp(second_value).expect("") {
                                    core::cmp::Ordering::Equal => {}
                                    other => return other,
                                }
                            } else {
                                todo!("What to do when comparing data with different types")
                            }
                        }
                        core::cmp::Ordering::Equal
                    });

                    storage::EntireRelation {
                        columns: input.columns,
                        parts: vec![storage::PartialRelation {
                            rows: result_rows,
                        }]
                    }
                    }
                    ra::RaExpression::Join { left, right, kind, condition, .. } => {
                        tracing::info!("Executing Join");

                        let inner_columns = {
                            let mut tmp = Vec::new();
                            tmp.extend(left.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)));
                            tmp.extend(right.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)));
                            tmp
                        };

                        let right_result = results.pop().unwrap();
                        let left_result = results.pop().unwrap();

                        let result_columns = {
                            let mut tmp = left_result.columns.clone();
                            tmp.extend(right_result.columns.clone());
                            tmp
                        };

                        algorithms::joins::Naive {}.execute(algorithms::joins::JoinArguments {
                            kind: kind.clone(),
                            conditon: condition,
                        }, algorithms::joins::JoinContext {}, result_columns, left_result, right_result,  &NaiveEngineConditionEval {
                            engine: self,
                            columns: &inner_columns,
                            placeholders,
                            ctes,
                            outer,
                            transaction
                        }).await? 
                    }
                    ra::RaExpression::Limit { inner, limit, offset } => {
                        let input = results.pop().unwrap();

                        storage::EntireRelation {
                            columns: input.columns,
                            parts: vec![storage::PartialRelation {
                                rows: input.parts.into_iter().flat_map(|p| p.rows.into_iter()).skip(*offset).take(*limit).collect(),
                            }]
                        }
                    }
                };

                results.push(result);
            }

            let result = results.pop().unwrap();

            Ok(result)
        }
        .boxed_local()
    }

    fn evaluate_ra_cond<'s, 'cond, 'r, 'col, 'p, 'c, 'o, 't, 'f>(
        &'s self,
        condition: &'cond ra::RaCondition,
        row: &'r storage::Row,
        columns: &'col [(String, DataType, AttributeId)],
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
        outer: &'o HashMap<AttributeId, storage::Data>,
        transaction: &'t S::TransactionGuard
    ) -> LocalBoxFuture<'f, Result<bool, EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'cond: 'f,
        'r: 'f,
        'col: 'f,
        'p: 'f,
        'c: 'f,
        'o: 'f,
        't: 'f,
    {
        async move {
            match condition {
                ra::RaCondition::And(conditions) => {
                    for andcond in conditions.iter() {
                        if !self
                            .evaluate_ra_cond(andcond, row, columns, placeholders, ctes, outer, transaction)
                            .await?
                        {
                            return Ok(false);
                        }
                    }

                    Ok(true)
                }
                ra::RaCondition::Or(conditions) => {
                    for andcond in conditions.iter() {
                        if self
                            .evaluate_ra_cond(andcond, row, columns, placeholders, ctes, outer, transaction)
                            .await?
                        {
                            return Ok(true);
                        }
                    }

                    Ok(false)
                }
                ra::RaCondition::Value(value) => {
                    let res = self
                        .evaluate_ra_cond_val(value, row, columns, placeholders, ctes, outer,transaction)
                        .await?;
                    Ok(res)
                }
            }
        }
        .boxed_local()
    }

    fn evaluate_ra_cond_val<'s, 'cond, 'r, 'col, 'p, 'c, 'o, 't, 'f>(
        &'s self,
        condition: &'cond ra::RaConditionValue,
        row: &'r storage::Row,
        columns: &'col [(String, DataType, AttributeId)],
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
        outer: &'o HashMap<AttributeId, storage::Data>,
        transaction: &'t S::TransactionGuard
    ) -> LocalBoxFuture<'f, Result<bool, EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'cond: 'f,
        'r: 'f,
        'col: 'f,
        'p: 'f,
        'c: 'f,
        'o: 'f,
        't: 'f,
    {
        async move {
            match condition {
                ra::RaConditionValue::Constant { value } => Ok(*value),
                ra::RaConditionValue::Attribute { name,  a_id, .. } => {
                    let data_result = row.data.iter().zip(columns.iter()).find(|(_, column)| &column.2 == a_id).map(|(d, _)| d);
                    
                    match data_result {
                        Some(storage::Data::Boolean(v)) => Ok(*v),
                        Some(d) => {
                            dbg!(&d);
                            Err(EvaulateRaError::Other("Invalid Data Type"))
                        },
                        None => Err(EvaulateRaError::UnknownAttribute { name: name.clone(), id: *a_id }),
                    }

                }
                ra::RaConditionValue::Comparison {
                    first,
                    second,
                    comparison,
                } => {
                    let first_value = self
                        .evaluate_ra_value(first, row, columns, placeholders, ctes, outer, transaction )
                        .await?;
                    let second_value = self
                        .evaluate_ra_value(second, row, columns, placeholders, ctes, outer, transaction)
                        .await?;

                    match comparison {
                        ra::RaComparisonOperator::Equals => Ok(first_value == second_value),
                        ra::RaComparisonOperator::NotEquals => Ok(first_value != second_value),
                        ra::RaComparisonOperator::Greater => Ok(first_value > second_value),
                        ra::RaComparisonOperator::GreaterEqual => Ok(first_value >= second_value),
                        ra::RaComparisonOperator::Less => Ok(first_value < second_value),
                        ra::RaComparisonOperator::LessEqual => Ok(first_value <= second_value),
                        ra::RaComparisonOperator::In => {
                            dbg!(&first_value, &second_value);

                            let parts = match second_value {
                                Data::List(ps) => ps,
                                other => {
                                    dbg!(other);
                                    return Err(EvaulateRaError::Other(
                                        "Attempting IN comparison on non List",
                                    ));
                                }
                            };

                            Ok(parts.iter().any(|p| p == &first_value))
                        }
                        ra::RaComparisonOperator::NotIn => match second_value {
                            storage::Data::List(d) => Ok(d.iter().all(|v| v != &first_value)),
                            other => {
                                tracing::error!("Cannot perform NotIn Operation on not List Data");
                                dbg!(other);
                                Err(EvaulateRaError::Other("Incompatible Error"))
                            }
                        },
                        ra::RaComparisonOperator::Like => {
                            let haystack = match first_value {
                                Data::Text(t) => t,
                                other => {
                                    tracing::warn!("Expected Text Data, got {:?}", other);
                                    return Err(EvaulateRaError::Other("Wrong Type for Haystack"));
                                }
                            };
                            let raw_pattern = match second_value {
                                Data::Text(p) => p,
                                other => {
                                    tracing::warn!("Expected Text Data, got {:?}", other);
                                    return Err(EvaulateRaError::Other("Wrong Type for Pattern"));
                                }
                            };

                            let result = pattern::like_match(&haystack, &raw_pattern);

                            Ok(result)
                        }
                        ra::RaComparisonOperator::ILike => {
                            todo!("Perforing ILike Comparison")
                        }
                        ra::RaComparisonOperator::Is => match (first_value, second_value) {
                            (storage::Data::Boolean(val), storage::Data::Boolean(true)) => Ok(val),
                            (storage::Data::Boolean(val), storage::Data::Boolean(false)) => {
                                Ok(!val)
                            }
                            _ => Ok(false),
                        },
                        ra::RaComparisonOperator::IsNot => {
                            dbg!(&first_value, &second_value);

                            Err(EvaulateRaError::Other("Not implemented - IsNot Operator "))
                        }
                    }
                }
                ra::RaConditionValue::Negation { inner } => {
                    let inner_result = self
                        .evaluate_ra_cond_val(inner, row, columns, placeholders, ctes, outer, transaction)
                        .await?;

                    Ok(!inner_result)
                }
                ra::RaConditionValue::Exists { query } => {
                    let n_tmp = {
                        let mut tmp = outer.clone();
                        tmp.extend(columns.iter().zip(row.data.iter()).map(|(column, data)| (column.2, data.clone())));
                        tmp
                    };

                    match self.evaluate_ra(*query.clone(), placeholders, ctes, &n_tmp, transaction).await {
                        Ok(res) => Ok(res
                            .parts
                            .into_iter()
                            .flat_map(|p| p.rows.into_iter())
                            .any(|_| true)),
                        Err(e) => panic!("{:?}", e),
                    }
                }
            }
        }
        .boxed_local()
    }

    fn evaluate_ra_value<'s, 'rve, 'row, 'columns, 'placeholders, 'c, 'o, 't, 'f>(
        &'s self,
        expr: &'rve ra::RaValueExpression,
        row: &'row storage::Row,
        columns: &'columns [(String, DataType, AttributeId)],
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
        outer: &'o HashMap<AttributeId, storage::Data>,
        transaction: &'t S::TransactionGuard
    ) -> LocalBoxFuture<'f, Result<storage::Data, EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'rve: 'f,
        'row: 'f,
        'columns: 'f,
        'placeholders: 'f,
        'c: 'f,
        'o: 'f,
        't: 'f,
    {
        async move {
            match expr {
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
                    let mut result = Vec::with_capacity(elems.len());

                    for part in elems.iter() {
                        let tmp = self
                            .evaluate_ra_value(part, row, columns, placeholders, ctes, outer, transaction )
                            .await?;
                        result.push(tmp);
                    }

                    Ok(Data::List(result))
                }
                ra::RaValueExpression::SubQuery { query } => {
                    let n_outer = {
                        let tmp = outer.clone();
                        // TODO
                        dbg!(columns, row);
                        tmp
                    };
                    let result = self.evaluate_ra(query.clone(), placeholders, ctes, &n_outer, transaction).await?;

                    let parts: Vec<_> = result
                        .parts
                        .into_iter()
                        .flat_map(|p| p.rows.into_iter())
                        .map(|r| r.data[0].clone())
                        .collect();

                    Ok(storage::Data::List(parts))
                }
                ra::RaValueExpression::Cast { inner, target } => {
                    let result = self
                        .evaluate_ra_value(inner, row, columns, placeholders, ctes,outer, transaction)
                        .await?;

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
                    let first_value = self
                        .evaluate_ra_value(first, row, columns, placeholders, ctes, outer, transaction)
                        .await?;
                    let second_value = self
                        .evaluate_ra_value(second, row, columns, placeholders, ctes, outer, transaction)
                        .await?;

                    match operator {
                        BinaryOperator::Add => match (first_value, second_value) {
                            (Data::SmallInt(f), Data::SmallInt(s)) => Ok(Data::SmallInt(f + s)),
                            other => {
                                dbg!(other);
                                Err(EvaulateRaError::Other("Addition"))
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

                        let value_res = self
                            .evaluate_ra_value(value, row, columns, placeholders, ctes, outer, transaction)
                            .await?;
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

                        let data = self
                            .evaluate_ra_value(val, row, columns, placeholders, ctes, outer, transaction )
                            .await?;

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

                        let str_value = self
                            .evaluate_ra_value(str_value, row, columns, placeholders, ctes, outer, transaction )
                            .await?;
                        let start_value = self
                            .evaluate_ra_value(start, row, columns, placeholders, ctes, outer, transaction)
                            .await?;
                        let count_value = match count.as_ref() {
                            Some(c) => {
                                let val = self
                                    .evaluate_ra_value(c, row, columns, placeholders, ctes, outer, transaction)
                                    .await?;
                                Some(val)
                            }
                            None => None,
                        };

                        dbg!(&str_value, &start_value, &count_value);

                        Err(EvaulateRaError::Other("Executing Substr function"))
                    }
                },
                ra::RaValueExpression::Renamed { name, value } => {
                    dbg!(&name, &value);

                    Err(EvaulateRaError::Other("Renamed Value Expression"))
                }
            }
        }
        .boxed_local()
    }

    #[tracing::instrument(skip(self, cte))]
    async fn execute_cte<'p, 'c, 't>(
        &self,
        cte: &ra::CTE,
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
        transaction: &'t S::TransactionGuard,
    ) -> Result<storage::EntireRelation, EvaulateRaError<S::LoadingError>> {
        tracing::debug!("CTE: {:?}", cte);

        match &cte.value {
            ra::CTEValue::Standard { query } => match query {
                ra::CTEQuery::Select(s) => {
                    let evaluated = self.evaluate_ra(s.clone(), placeholders, ctes, &HashMap::new(), transaction).await?;
                    Ok(evaluated)
                }
            },
            ra::CTEValue::Recursive { query, columns } => match query {
                ra::CTEQuery::Select(s) => {
                    let s_columns = s.get_columns();

                    let result_columns: Vec<_> = s_columns
                        .into_iter()
                        .zip(columns.iter())
                        .map(|((_, n, ty, _), name)| (name.clone(), ty, Vec::new()))
                        .collect();

                    let result = storage::EntireRelation {
                        columns: result_columns,
                        parts: vec![],
                    };

                    let mut inner_cte: HashMap<_, _> = ctes
                        .iter()
                        .map(|(c, s)| {
                            (
                                c.clone(),
                                storage::EntireRelation {
                                    columns: s.columns.clone(),
                                    parts: s
                                        .parts
                                        .iter()
                                        .map(|p| storage::PartialRelation {
                                            rows: p.rows.clone(),
                                        })
                                        .collect(),
                                },
                            )
                        })
                        .collect();

                    inner_cte.insert(cte.name.clone(), result);

                    loop {
                        let mut tmp = self
                            .evaluate_ra(s.clone(), placeholders, &inner_cte, &HashMap::new(), transaction )
                            .await?;

                        for (column, name) in tmp.columns.iter_mut().zip(columns.iter()) {
                            column.0.clone_from(name);
                        }

                        let tmp_rows = tmp.parts.iter().flat_map(|p| p.rows.iter());
                        let previous_rows = inner_cte
                            .get(&cte.name)
                            .into_iter()
                            .flat_map(|s| s.parts.iter().flat_map(|p| p.rows.iter()));

                        if tmp_rows.count() == previous_rows.count() {
                            break;
                        }

                        inner_cte.insert(cte.name.clone(), tmp);
                    }

                    let result = inner_cte.remove(&cte.name).unwrap();

                    Ok(result)
                }
            },
        }
    }
}

#[derive(Debug)]
pub enum PrepareError<SE> {
    LoadingSchemas(SE),
    UnknownRelation,
    ParsingSelect(ra::ParseSelectError),
    Other,
}

#[derive(Debug)]
pub enum ExecuteBoundError<SE> {
    ParseRelationAlgebra(ra::ParseSelectError),
    MismatchedParameterCounts { expected: usize, received: usize },
    RealizingValueFromRaw { value: Vec<u8>, target: DataType },
    Executing(EvaulateRaError<SE>),
    NotImplemented(&'static str),
    StorageError(SE),
    Other(&'static str),
}

impl<S> Execute<S::TransactionGuard> for NaiveEngine<S>
where
    S: crate::storage::Storage,
{
    type Prepared = NaivePrepared;
    type PrepareError = PrepareError<S::LoadingError>;
    type ExecuteBoundError = ExecuteBoundError<S::LoadingError>;

    type CopyState<'e> = NaiveCopyState<'e, S, S::TransactionGuard> where S: 'e, S::TransactionGuard: 'e;

    async fn prepare<'q>(
        &self,
        query: &sql::Query<'q>,
        _ctx: &mut Context<S::TransactionGuard>,
    ) -> Result<Self::Prepared, Self::PrepareError> {
        let (expected, columns) = match query {
            Query::Select(s) => {
                let schemas = self
                    .storage
                    .schemas()
                    .await
                    .map_err(PrepareError::LoadingSchemas)?;

                let (ra_expression, expected_types) = ra::RaExpression::parse_select(s, &schemas)
                    .map_err(PrepareError::ParsingSelect)?;

                let parameters: Vec<_> = {
                    let mut tmp: Vec<_> = expected_types.into_iter().collect();
                    tmp.sort_by_key(|(i, _)| *i);
                    tmp.into_iter().map(|(_, d)| d).collect()
                };

                let columns: Vec<_> = ra_expression
                    .get_columns()
                    .into_iter()
                    .map(|(_, name, dtype, _)| (name, dtype))
                    .collect();

                (parameters, columns)
            }
            Query::Insert(ins) => {
                let expected_parameters = vec![DataType::Bool; query.parameter_count()];

                let results = match ins.returning.as_ref() {
                    Some(ident) => {
                        let schema = self
                            .storage
                            .schemas()
                            .await
                            .map_err(PrepareError::LoadingSchemas)?;
                        let table = schema
                            .get_table(ins.table.0.as_ref())
                            .ok_or_else(|| PrepareError::UnknownRelation)?;

                        let returned_type = table
                            .rows
                            .iter()
                            .find(|c| c.name.as_str() == ident.0.as_ref())
                            .map(|c| (c.name.clone(), c.ty.clone()))
                            .ok_or_else(|| PrepareError::Other)?;

                        vec![returned_type]
                    }
                    None => vec![],
                };

                (expected_parameters, results)
            }
            _ => {
                let expected_parameters = vec![DataType::Bool; query.parameter_count()];

                let results = vec![];

                (expected_parameters, results)
            }
        };

        Ok(NaivePrepared {
            query: query.to_static(),
            expected_parameters: expected,
            columns,
        })
    }

    async fn start_copy<'s, 'e, 'c>(
            &'s self,
            table_name: &str,
            ctx: &'c mut Context<S::TransactionGuard>,
        ) ->  Result<Self::CopyState<'e>, Self::ExecuteBoundError>
        where
            's: 'e, 'c: 'e {
        let schemas = self.storage.schemas().await.map_err(ExecuteBoundError::StorageError)?;

        let table = match schemas.get_table(table_name) {
            Some(t) => t.clone(),
            None => return Err(ExecuteBoundError::Other("Missing Table")),
        };

        Ok(NaiveCopyState { table: table_name.into(), engine: &self.storage, schema: table, tx: ctx.transaction.as_ref().unwrap() })
    }

    #[tracing::instrument(skip(self, query, ctx))]
    async fn execute_bound(
        &self,
        query: &<Self::Prepared as PreparedStatement>::Bound,
        ctx: &mut Context<S::TransactionGuard>,
    ) -> Result<ExecuteResult, Self::ExecuteBoundError> {
        tracing::debug!("Executing Bound Query");
        tracing::debug!("{:#?}", query.query);

        let mut parse_context = ra::ParsingContext::new();
        let mut to_process = Some(&query.query);

        let mut cte_queries: HashMap<String, storage::EntireRelation> = HashMap::new();

        while let Some(inner_query) = to_process.take() {
            let result = match inner_query {
                Query::Configuration(conf) => {
                    tracing::debug!("Updating Configuration: {:?}", conf);
                    Ok(ExecuteResult::Set)
                }
                Query::Prepare(prepare) => {
                    tracing::debug!("Creating Prepared statement");
                    dbg!(&prepare);

                    Err(ExecuteBoundError::NotImplemented("Creating Prepared Statement"))
                }
                Query::Select(select) => {
                    tracing::debug!("Selecting: {:?}", select);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    let schemas = self
                        .storage
                        .schemas()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let (ra_expression, placeholder_types) =
                        match crate::ra::RaExpression::parse_select_with_context(
                            select,
                            &schemas,
                            &parse_context,
                        ) {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("Parsing RA: {:?}", e);
                                return Err(ExecuteBoundError::ParseRelationAlgebra(e));
                            }
                        };
                    tracing::debug!("RA: {:#?}", ra_expression);
                    tracing::debug!("Placeholder-Types: {:#?}", placeholder_types);

                    if placeholder_types.len() != query.expected_parameters.len() {
                        tracing::error!("Mismatched Parameter counts");
                        return Err(ExecuteBoundError::MismatchedParameterCounts {
                            expected: query.expected_parameters.len(),
                            received: placeholder_types.len(),
                        });
                    }

                    let placeholder_values: HashMap<_, _> = placeholder_types
                        .iter()
                        .map(|(name, ty)| {
                            let value = query.values.get(*name - 1).unwrap();
                            let tmp = storage::Data::realize(ty, value).map_err(|e| {
                                Self::ExecuteBoundError::RealizingValueFromRaw {
                                    value: value.clone(),
                                    target: (*ty).clone(),
                                }
                            })?;

                            Ok((*name, tmp))
                        })
                        .collect::<Result<_, Self::ExecuteBoundError>>()?;

                    tracing::trace!("Placeholder-Values: {:#?}", placeholder_values);

                    let r = match self
                        .evaluate_ra(ra_expression, &placeholder_values, &cte_queries, &HashMap::new(), transaction)
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!("RA-Error: {:?}", e);
                            return Err(ExecuteBoundError::Executing(e));
                        }
                    };

                    tracing::debug!("RA-Result: {:?}", r);

                    Ok(ExecuteResult::Select {
                        content: r,
                        formats: query.result_columns.clone(),
                    })
                }
                Query::Insert(ins) => {
                    tracing::debug!("Inserting: {:?}", ins);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    let schemas = self
                        .storage
                        .schemas()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;
                    let table_schema =
                        schemas.get_table(ins.table.0.as_ref()).ok_or_else(|| {
                            ExecuteBoundError::Other("Could not find Table in schemas")
                        })?;

                    tracing::trace!("Table Schema: {:?}", table_schema);

                    let field_types: Vec<_> = ins
                        .fields
                        .iter()
                        .map(|ident| {
                            table_schema
                                .rows
                                .iter()
                                .find(|c| c.name.as_str() == ident.0.as_ref())
                                .map(|c| &c.ty)
                                .unwrap()
                        })
                        .collect();

                    let values: Vec<Vec<storage::Data>> = match &ins.values {
                        sql::InsertValues::Values(values) => {
                            values
                                .iter()
                                .map(|values| {
                                    field_types
                                        .iter()
                                        .zip(values.iter())
                                        .map(|(field_type, expr)| match expr {
                                            sql::ValueExpression::Placeholder(pnumb) => {
                                                storage::Data::realize(
                                                    field_type,
                                                    &query.values[*pnumb - 1],
                                                )
                                                .map_err(|e| {
                                                    ExecuteBoundError::RealizingValueFromRaw {
                                                        value: query.values[*pnumb - 1].to_vec(),
                                                        target: (*field_type).clone(),
                                                    }
                                                })
                                            }
                                            sql::ValueExpression::Literal(lit) => {
                                                let data = storage::Data::from_literal(lit);
                                                // TODO
                                                // Check if data and expected types match

                                                Ok(data)
                                            }
                                            sql::ValueExpression::FunctionCall(func) => match func {
                                                sql::FunctionCall::CurrentTimestamp => {
                                                    Ok(storage::Data::Timestamp("2024-04-12 12:00:00.000000-05".into()))
                                                }
                                                other => {
                                                    dbg!(other);
                                                    Err(ExecuteBoundError::NotImplemented("FunctionCall"))
                                                }
                                            },
                                            other => {
                                                dbg!(other);
                                                Err(ExecuteBoundError::NotImplemented(
                                                    "Executing Value Expression",
                                                ))
                                            }
                                        })
                                        .collect::<Result<_, _>>()
                                })
                                .collect::<Result<_, _>>()?
                        }
                        sql::InsertValues::Select(select) => {
                            tracing::trace!("Using Select as Values for Insert");

                            let content = match self
                                .execute(&Query::Select(select.to_static()), ctx)
                                .boxed_local()
                                .await
                                .map_err(|e| ExecuteBoundError::Other("Executing Query"))?
                            {
                                ExecuteResult::Select { content, formats } => content,
                                other => {
                                    tracing::error!("Expected Select result but got {:?}", other);
                                    return Err(ExecuteBoundError::Other(
                                        "Received unexpected Query result",
                                    ));
                                }
                            };

                            tracing::debug!("Insert Content: {:?}", content);

                            if content.parts.iter().flat_map(|p| p.rows.iter()).count() == 0 {
                                return Ok(ExecuteResult::Insert {
                                    returning: Vec::new(),
                                    inserted_rows: 0,
                                    formats: query.result_columns.clone(),
                                });
                            }

                            return Err(ExecuteBoundError::NotImplemented(
                                "Getting InsertValues from Select",
                            ));
                        }
                    };

                    tracing::trace!("Values: {:?}", values);

                    let relation = if table_schema.rows.iter().any(|c| c.ty == DataType::Serial) {
                        let relation = self
                            .storage
                            .get_entire_relation(&ins.table.0, transaction)
                            .await
                            .map_err(ExecuteBoundError::StorageError)?;
                        Some(relation)
                    } else {
                        None
                    };

                    let mut insert_rows: Vec<Vec<storage::Data>> = Vec::new();

                    for values in values {
                        let mut rows: Vec<storage::Data> =
                            Vec::with_capacity(table_schema.rows.len());
                        for (i, column) in table_schema.rows.iter().enumerate() {
                            let value = match ins
                                .fields
                                .iter()
                                .enumerate()
                                .find(|(_, n)| n.0.as_ref() == column.name)
                            {
                                Some((i, _)) => {
                                    let value = values[i].clone();

                                    match value.try_cast(&column.ty) {
                                        Ok(v) => v,
                                        Err(old) => {
                                            panic!("Casting {:?} -> {:?}", old, column.ty)
                                        }
                                    }
                                }
                                None => {
                                    if column.ty == DataType::Serial {
                                        let rel = relation.as_ref().ok_or_else(|| {
                                            ExecuteBoundError::Other("Missing Relation")
                                        })?;

                                        let n_serial = rel
                                            .parts
                                            .iter()
                                            .flat_map(|p| p.rows.iter())
                                            .filter_map(|row| match &row.data[i] {
                                                storage::Data::Serial(v) => Some(*v),
                                                _ => None,
                                            })
                                            .max()
                                            .map(|m| m + 1)
                                            .unwrap_or(2);

                                        storage::Data::Serial(n_serial)
                                    } else {
                                        storage::Data::as_null(&column.ty)
                                    }
                                }
                            };

                            tracing::trace!("Value: {:?}", value);

                            rows.push(value);
                        }

                        insert_rows.push(rows);
                    }

                    tracing::trace!("Inserting Row: {:?}", insert_rows);

                    let returning: Vec<Vec<storage::Data>> = match ins.returning.as_ref() {
                        Some(return_ident) => insert_rows
                            .iter()
                            .map(|row| {
                                let tmp = table_schema
                                    .rows
                                    .iter()
                                    .zip(row.iter())
                                    .find(|(c, _)| c.name.as_str() == return_ident.0.as_ref())
                                    .map(|(_, val)| val.clone())
                                    .ok_or_else(|| {
                                        ExecuteBoundError::Other("Unknown attribute for returning")
                                    })?;

                                Ok(vec![tmp])
                            })
                            .collect::<Result<_, _>>()?,
                        None => Vec::new(),
                    };

                    let inserted_rows = insert_rows.len();

                    self.storage
                        .insert_rows(&ins.table.0, &mut insert_rows.into_iter(), transaction )
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Insert {
                        returning,
                        inserted_rows,
                        formats: query.result_columns.clone(),
                    })
                }
                Query::Update(update) => {
                    tracing::info!("Update: {:#?}", update);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    let relation = self
                        .storage
                        .get_entire_relation(&update.table.0, transaction )
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let schemas = self
                        .storage
                        .schemas()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    tracing::info!("Relation: {:#?}", relation);

                    let (ra_update, ra_placeholders) = RaUpdate::parse(update, &schemas)
                        .map_err(ExecuteBoundError::ParseRelationAlgebra)?;

                    let placeholder_values: HashMap<_, _> = ra_placeholders
                        .iter()
                        .map(|(name, ty)| {
                            let value = query.values.get(*name - 1).unwrap();
                            let tmp = storage::Data::realize(ty, value).map_err(|e| {
                                Self::ExecuteBoundError::RealizingValueFromRaw {
                                    value: value.clone(),
                                    target: (*ty).clone(),
                                }
                            })?;

                            Ok((*name, tmp))
                        })
                        .collect::<Result<_, Self::ExecuteBoundError>>()?;

                    tracing::info!("Placerholder Types: {:?}", ra_placeholders);

                    let table_columns: Vec<_> = relation
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (c.0.clone(), c.1.clone(), AttributeId::new(i)))
                        .collect();

                    match ra_update {
                        ra::RaUpdate::Standard { fields, condition } => {
                            let mut count = 0;
                            for mut row in
                                relation.parts.into_iter().flat_map(|p| p.rows.into_iter())
                            {
                                let should_update = match condition.as_ref() {
                                    Some(cond) => self
                                        .evaluate_ra_cond(
                                            cond,
                                            &row,
                                            &table_columns,
                                            &placeholder_values,
                                            &cte_queries,
                                            &HashMap::new(),
                                            transaction,
                                        )
                                        .await
                                        .map_err(ExecuteBoundError::Executing)?,
                                    None => true,
                                };

                                if !should_update {
                                    continue;
                                }

                                count += 1;

                                let mut field_values = Vec::new();
                                for field in fields.iter() {
                                    let value = self
                                        .evaluate_ra_value(
                                            &field.value,
                                            &row,
                                            &table_columns,
                                            &placeholder_values,
                                            &cte_queries,
                                            &HashMap::new(),
                                            transaction
                                        )
                                        .await
                                        .map_err(ExecuteBoundError::Executing)?;

                                    field_values.push(value);
                                }

                                for (name, value) in fields
                                    .iter()
                                    .zip(field_values.into_iter())
                                    .map(|(first, val)| (&first.field, val))
                                {
                                    let idx = relation
                                        .columns
                                        .iter()
                                        .enumerate()
                                        .find(|(_, (n, _, _))| n == name)
                                        .map(|(i, _)| i)
                                        .ok_or(ExecuteBoundError::Other(
                                            "Unknown Attribute to update",
                                        ))?;

                                    row.data[idx] = value;
                                }

                                self.storage
                                    .update_rows(
                                        update.table.0.as_ref(),
                                        &mut core::iter::once((row.id(), row.data)),
                                        transaction
                                    )
                                    .await
                                    .map_err(ExecuteBoundError::StorageError)?;
                            }

                            Ok(ExecuteResult::Update {
                                updated_rows: count,
                            })
                        }
                        ra::RaUpdate::UpdateFrom {
                            fields,
                            condition,
                            other_table_src,
                            other_table_name,
                        } => {
                            dbg!(&fields, &condition, &other_table_src, &other_table_name);

                            let transaction = ctx.transaction.as_ref().unwrap();

                            let table = self
                                .storage
                                .get_entire_relation(update.table.0.as_ref(), transaction)
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            if !table.parts.iter().flat_map(|p| p.rows.iter()).any(|_| true) {
                                return Ok(ExecuteResult::Update { updated_rows: 0 });
                            }

                            Err(ExecuteBoundError::NotImplemented("Update FROM extension"))
                        }
                    }
                }
                Query::Copy_(c) => {
                    tracing::info!("Copy: {:?}", c);

                    Err(ExecuteBoundError::NotImplemented("Copy"))
                }
                Query::Delete(delete) => {
                    tracing::info!("Deleting: {:#?}", delete);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    let schemas = self
                        .storage
                        .schemas()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let (ra_delete, placeholders) = ra::RaDelete::parse(delete, &schemas)
                        .map_err(ExecuteBoundError::ParseRelationAlgebra)?;

                    let relation = self
                        .storage
                        .get_entire_relation(&ra_delete.table, transaction )
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let placeholders: HashMap<_, _> = placeholders
                        .iter()
                        .map(|(name, ty)| {
                            let value = query.values.get(*name - 1).unwrap();
                            let tmp = storage::Data::realize(ty, value).map_err(|e| {
                                Self::ExecuteBoundError::RealizingValueFromRaw {
                                    value: value.clone(),
                                    target: (*ty).clone(),
                                }
                            })?;

                            Ok((*name, tmp))
                        })
                        .collect::<Result<_, Self::ExecuteBoundError>>()?;

                    let table_columns: Vec<_> = relation
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (c.0.clone(), c.1.clone(), AttributeId::new(i)))
                        .collect();

                    let to_delete = {
                        let mut tmp = Vec::new();

                        for row in relation.parts.into_iter().flat_map(|p| p.rows.into_iter()) {
                            if let Some(condition) = ra_delete.condition.as_ref() {
                                let condition_result = self
                                    .evaluate_ra_cond(
                                        condition,
                                        &row,
                                        &table_columns,
                                        &placeholders,
                                        &cte_queries,
                                        &HashMap::new(),
                                        transaction
                                    )
                                    .await
                                    .map_err(ExecuteBoundError::Executing)?;

                                if !condition_result {
                                    continue;
                                }
                            }

                            tracing::debug!("Deleting Row: {:?}", row);
                            tmp.push(row.id());
                        }

                        tmp
                    };

                    let row_count = to_delete.len();

                    self.storage
                        .delete_rows(&delete.table.0, &mut to_delete.into_iter(), transaction )
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Delete {
                        deleted_rows: row_count,
                    })
                }
                Query::CreateTable(create) => {
                    tracing::debug!("Creating: {:?}", create);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    if create.if_not_exists
                        && self
                            .storage
                            .relation_exists(&create.identifier.0, transaction )
                            .await
                            .map_err(ExecuteBoundError::StorageError)?
                    {
                        tracing::trace!("Table already exists");
                        return Ok(ExecuteResult::Create);
                    }

                    let fields: Vec<_> = create
                        .fields
                        .iter()
                        .map(|tfield| {
                            (
                                tfield.ident.0.to_string(),
                                tfield.datatype.clone(),
                                tfield.modifiers.clone(),
                            )
                        })
                        .collect();

                    tracing::trace!("Creating Relation");
                    self.storage
                        .create_relation(&create.identifier.0, fields, transaction)
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Create)
                }
                Query::CreateIndex(create) => {
                    tracing::debug!("Creating Index: {:?}", create);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    self.storage
                        .insert_rows(
                            "pg_indexes",
                            &mut core::iter::once(vec![
                                storage::Data::Name(String::new()),
                                storage::Data::Name(create.table.0.to_string()),
                                storage::Data::Name(create.identifier.0.to_string()),
                                storage::Data::Name(String::new()),
                                storage::Data::Text(String::new()),
                            ]),
                            transaction,
                        )
                        .await
                        .unwrap();

                    Ok(ExecuteResult::Create)
                }
                Query::AlterTable(alter) => {
                    tracing::debug!("Alter Table");

let transaction = ctx.transaction.as_ref().unwrap();

                    match alter {
                        sql::AlterTable::Rename { from, to } => {
                            tracing::debug!("Renaming from {:?} -> {:?}", from, to);

                            self.storage
                                .rename_relation(&from.0, &to.0, transaction )
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                        sql::AlterTable::RenameColumn { table, from, to } => {
                            tracing::debug!("Renaming Column from {:?} -> {:?}", from, to);

                            let mut modifications = storage::ModifyRelation::new();

                            modifications.rename_column(from.0.as_ref(), to.0.as_ref());

                            self.storage
                                .modify_relation(&table.0, modifications, transaction)
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                        sql::AlterTable::AddColumn {
                            table,
                            column_name,
                            data_type,
                            type_modifiers,
                        } => {
                            let mut modifications = storage::ModifyRelation::new();

                            modifications.add_column(
                                &column_name.0,
                                data_type.clone(),
                                type_modifiers.clone(),
                            );

                            self.storage
                                .modify_relation(&table.0, modifications, transaction )
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                        sql::AlterTable::AlterColumnTypes { table, columns } => {
                            tracing::info!("Alter Column Types");

                            let mut modifications = storage::ModifyRelation::new();

                            for column in columns {
                                tracing::info!("Column: {:?}", column);

                                modifications.change_type(&column.0 .0, column.1.clone());
                            }

                            self.storage
                                .modify_relation(&table.0, modifications, transaction)
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                        sql::AlterTable::AtlerColumnDropNotNull { table, column } => {
                            let mut modifications = storage::ModifyRelation::new();

                            modifications.remove_modifier(&column.0, TypeModifier::NotNull);

                            self.storage
                                .modify_relation(&table.0, modifications, transaction)
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                        sql::AlterTable::SetColumnDefault {
                            table,
                            column,
                            value,
                        } => {
                            let mut modifications = storage::ModifyRelation::new();

                            modifications.set_default(&column.0, value.to_static());

                            self.storage
                                .modify_relation(&table.0, modifications, transaction)
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                        sql::AlterTable::AddPrimaryKey { table, column } => {
                            tracing::warn!(?table, ?column, "[TODO] Adding Primary Key");

                            Ok(ExecuteResult::Alter)
                        }
                    }
                }
                Query::DropIndex(drop_index) => {
                    tracing::debug!("Dropping Index: {:?}", drop_index);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    let pg_indexes_table = self
                        .storage
                        .get_entire_relation("pg_indexes", transaction )
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let mut cid = pg_indexes_table
                        .parts
                        .iter()
                        .flat_map(|p| p.rows.iter())
                        .filter(|row| {
                            row.data[2] == storage::Data::Name(drop_index.name.0.to_string())
                        })
                        .map(|row| row.id());

                    self.storage
                        .delete_rows("pg_indexes", &mut cid, transaction )
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Drop_)
                }
                Query::DropTable(drop_table) => {
                    let transaction = match ctx.transaction.as_ref() {
                        Some(t) => t,
                        None => {
                            return Err(ExecuteBoundError::Other("Missing Transaction State"))
                        }
                    };

                    for name in drop_table.names.iter() {
                    self.storage
                        .remove_relation(&name.0, transaction )
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;
                    }

                    Ok(ExecuteResult::Drop_)
                }
                Query::TruncateTable(trunc_table) => {
                    // TODO

                    Ok(ExecuteResult::Truncate)
                }
                Query::BeginTransaction(isolation) => {
                    tracing::debug!("Starting Transaction: {:?}", isolation);

                    let guard = self.storage.start_transaction().await.map_err(ExecuteBoundError::StorageError)?;
                    ctx.transaction = Some(guard);

                    Ok(ExecuteResult::Begin)
                }
                Query::CommitTransaction => {
                    tracing::debug!("Committing Transaction");
                    
                    let guard = ctx.transaction.take().unwrap();
                    self.storage.commit_transaction(guard).await.map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Commit)
                }
                Query::RollbackTransaction => {
                    tracing::debug!("Rollback Transaction");

                    let guard = ctx.transaction.take().unwrap();
                    self.storage.abort_transaction(guard).await.map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Rollback)
                }
                Query::WithCTE { cte, query } => {
let transaction = ctx.transaction.as_ref().unwrap();

                    let schemas = self
                        .storage
                        .schemas()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let (ra_cte, cte_placeholder_types) = ra::parse_ctes(cte, &schemas)
                        .map_err(ExecuteBoundError::ParseRelationAlgebra)?;

                    for cte in ra_cte {
                        parse_context.add_cte(cte.clone());

                        let cte_result = self
                            .execute_cte(&cte, &HashMap::new(), &HashMap::new(), transaction )
                            .await
                            .map_err(ExecuteBoundError::Executing)?;
                        cte_queries.insert(cte.name.clone(), cte_result);
                    }

                    to_process = Some(query);
                    continue;
                }
                Query::Vacuum(v) => {
                    tracing::info!("Vacuum");

                    // TODO

                    return Ok(ExecuteResult::Vacuum);
                }
            };

            return result;
        }

        Err(ExecuteBoundError::Other(""))
    }
}

impl PreparedStatement for NaivePrepared {
    type Bound = NaiveBound;
    type BindError = ();

    fn bind(
        &self,
        values: Vec<Vec<u8>>,
        return_formats: Vec<FormatCode>,
    ) -> Result<Self::Bound, ()> {
        Ok(NaiveBound {
            query: self.query.to_static(),
            expected_parameters: self.expected_parameters.clone(),
            values,
            result_columns: return_formats,
        })
    }

    fn parameters(&self) -> Vec<DataType> {
        self.expected_parameters.clone()
    }

    fn row_columns(&self) -> Vec<(String, DataType)> {
        self.columns.clone()
    }
}

impl<'e, S> CopyState for NaiveCopyState<'e, S, S::TransactionGuard> where S: Storage {
    fn columns(&self) -> Vec<()> {
        self.schema.rows.iter().map(|_| ()).collect()
    }

    async fn insert(&mut self, raw_column: &[u8]) -> Result<(), ()> {
        // TODO

        let raw_str = core::str::from_utf8(raw_column).map_err(|e| ())?;

        let parts: Vec<_> = raw_str.split('\t').collect();

        if parts.len() != self.schema.rows.len() {
            dbg!(parts, &self.schema.rows);

            return Err(());
        }

        let mut row_data = Vec::with_capacity(parts.len());
        for (column, raw) in self.schema.rows.iter().zip(parts) {
            let tmp = Data::realize(&column.ty, raw.as_bytes()).map_err(|e| ())?;
            row_data.push(tmp);
        }

        self.engine.insert_rows(&self.table, &mut core::iter::once(row_data), self.tx).await.unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{EntireRelation, PartialRelation, Row, Storage};

    use self::storage::inmemory::InMemoryStorage;

    use super::*;

    #[tokio::test]
    async fn execute_delete_with_subquery() {
        let query = "DELETE FROM dashboard_acl WHERE dashboard_id NOT IN (SELECT id FROM dashboard) AND dashboard_id != -1";

        let storage = {
            let storage = InMemoryStorage::new();

            let trans = storage.start_transaction().await.unwrap();

            storage
                .create_relation(
                    "dashboard_acl",
                    vec![("dashboard_id".into(), DataType::Integer, Vec::new())],
                    &trans
                )
                .await
                .unwrap();

            storage
                .create_relation(
                    "dashboard",
                    vec![("id".into(), DataType::Integer, Vec::new())],
                    &trans
                )
                .await
                .unwrap();

            storage
                .insert_rows("dashboard", &mut vec![vec![Data::Integer(1)]].into_iter(), &trans)
                .await
                .unwrap();

            storage
                .insert_rows(
                    "dashboard_acl",
                    &mut vec![vec![Data::Integer(132)], vec![Data::Integer(1)]].into_iter(),
                    &trans
                )
                .await
                .unwrap();

            storage.commit_transaction(trans).await.unwrap();

            storage
        };
        let engine = NaiveEngine::new(storage);

        let query = Query::parse(query.as_bytes()).unwrap();
        
        let mut ctx = Context::new();
        ctx.transaction = Some(engine.storage.start_transaction().await.unwrap());
        let res = engine.execute(&query, &mut ctx).await.unwrap();

        assert_eq!(ExecuteResult::Delete { deleted_rows: 1 }, res);
    }

    #[tokio::test]
    async fn using_is_true() {
        let query_str = "SELECT name FROM user WHERE active IS TRUE";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        let storage = {
            let storage = InMemoryStorage::new();

            let trans = storage.start_transaction().await.unwrap();

            storage
                .create_relation(
                    "user",
                    vec![
                        ("name".into(), DataType::Text, Vec::new()),
                        ("active".into(), DataType::Bool, Vec::new()),
                    ],
                    &trans
                )
                .await
                .unwrap();

            storage
                .insert_rows(
                    "user",
                    &mut vec![
                        vec![Data::Text("first-user".to_string()), Data::Boolean(false)],
                        vec![Data::Text("second-user".to_string()), Data::Boolean(true)],
                    ]
                    .into_iter(),
                    &trans,
                )
                .await
                .unwrap();

            storage.commit_transaction(trans).await;

            storage
        };
        let engine = NaiveEngine::new(storage);

        let mut ctx = Context::new();
        ctx.transaction = Some(engine.storage.start_transaction().await.unwrap());
        let res = engine.execute(&query, &mut ctx).await.unwrap();

        dbg!(&res);

        assert_eq!(
            ExecuteResult::Select {
                content: EntireRelation {
                    columns: vec![("name".to_string(), DataType::Text, Vec::new())],
                    parts: vec![PartialRelation {
                        rows: vec![Row::new(0, vec![Data::Text("second-user".to_string())])]
                    }]
                },
                formats: Vec::new(),
            },
            res
        );
    }
}
