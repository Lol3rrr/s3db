use std::{collections::HashMap, thread::panicking};

use futures::{future::LocalBoxFuture, FutureExt};

use crate::{
    ra::{self, AttributeId,  RaUpdate},
    sql::{self, BinaryOperator, ColumnReference, DataType, Query, TypeModifier},
    storage::{self, Data},
};

use super::{Context, Execute, ExecuteResult, PreparedStatement};

mod aggregate;
use aggregate::AggregateState;


pub struct NaiveEngine<S> {
    storage: S,
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
}

impl<S> NaiveEngine<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

#[derive(Debug)]
pub enum EvaulateRaError<SE> {
    UnknownAttribute {
        attribute: ColumnReference<'static>,
    },
    CastingType {
        value: storage::Data,
        target: DataType,
    },
    StorageError(SE),
    Other(&'static str),
}



impl<S> NaiveEngine<S>
where
    S: storage::Storage,
{
    fn evaluate_ra<'s, 'p, 'f, 'c>(
        &'s self,
        expr: ra::RaExpression,
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
    ) -> LocalBoxFuture<'f, Result<storage::EntireRelation, EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'p: 'f,
        'c: 'f,
    {
        async move {
            match expr {
                ra::RaExpression::Projection { inner, attributes } => {
                    let inner_columns = inner.get_columns();
                    let inner_result = self.evaluate_ra(*inner, placeholders, ctes).await?;

                    assert_eq!(
                        inner_columns
                            .iter()
                            .map(|(n, t, _)| (n, t))
                            .collect::<Vec<_>>(),
                        inner_result
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
                    for row in inner_result.into_rows() {
                        let mut data = Vec::new();
                        for attribute in attributes.iter() {
                            let tmp = self.evaluate_ra_value(&attribute.value, &row, &inner_columns, placeholders, ctes).await?;
                            data.push(tmp);
                        }

                        rows.push(storage::Row::new(0, data));
                    }

                    Ok(storage::EntireRelation {
                        columns,
                        parts: vec![storage::PartialRelation { rows }],
                    })
                }
                ra::RaExpression::Selection { inner, filter } => {
                    let inner_columns = inner.get_columns();
                    let inner_result = self.evaluate_ra(*inner, placeholders, ctes).await?;

                    assert_eq!(
                        inner_columns
                            .iter()
                            .map(|(n, t, _)| (n, t))
                            .collect::<Vec<_>>(),
                        inner_result
                            .columns
                            .iter()
                            .map(|(n, t, _)| (n, t))
                            .collect::<Vec<_>>()
                    );

                    let result = {
                        let mut tmp = Vec::new();

                        for row in inner_result
                            .parts
                            .into_iter()
                            .flat_map(|i| i.rows.into_iter())
                        {
                            // TODO
                            tracing::trace!("Row: {:?}", row);

                            let include_row = self
                                .evaluate_ra_cond(
                                    &filter,
                                    &row,
                                    &inner_columns,
                                    placeholders,
                                    ctes
                                )
                                .await?;

                            if include_row {
                                tmp.push(row);
                            }
                        }

                        tmp
                    };

                    Ok(storage::EntireRelation {
                        columns: inner_result.columns,
                        parts: vec![storage::PartialRelation { rows: result }],
                    })
                }
                ra::RaExpression::BaseRelation { name, .. } => {
                    //

                    self.storage
                        .get_entire_relation(&name.0)
                        .await
                        .map_err(|e| EvaulateRaError::StorageError(e))
                }
                ra::RaExpression::Join {
                    left,
                    right,
                    kind,
                    condition,
                } => {
                    tracing::info!("Executing Join");

                    let inner_columns = {
                        let mut tmp = left.get_columns();
                        tmp.extend(right.get_columns());
                        tmp
                    };

                    let left_result = self.evaluate_ra(*left, placeholders, ctes).await?;
                    let right_result = self.evaluate_ra(*right, placeholders, ctes).await?;

                    let result_columns = {
                        let mut tmp = left_result.columns.clone();
                        tmp.extend(right_result.columns.clone());
                        tmp
                    };

                    match kind {
                        sql::JoinKind::Inner => {
                            let mut result_rows = Vec::new();
                            for left_row in left_result
                                .parts
                                .into_iter()
                                .flat_map(|p| p.rows.into_iter())
                            {
                                for right_row in
                                    right_result.parts.iter().flat_map(|p| p.rows.iter())
                                {
                                    let joined_row_data = {
                                        let mut tmp = left_row.data.clone();
                                        tmp.extend(right_row.data.clone());
                                        tmp
                                    };

                                    let row = storage::Row::new(
                                        result_rows.len() as u64,
                                        joined_row_data,
                                    );
                                    
                                    if self.evaluate_ra_cond(&condition, &row, &inner_columns, placeholders, ctes).await? {
                                        result_rows.push(row);
                                    }
                                }
                            }

                            Ok(storage::EntireRelation {
                                columns: result_columns,
                                parts: vec![storage::PartialRelation { rows: result_rows }],
                            })
                        }
                        sql::JoinKind::LeftOuter => {
                            let mut result_rows = Vec::new();
                            for left_row in left_result
                                .parts
                                .into_iter()
                                .flat_map(|p| p.rows.into_iter())
                            {
                                let mut included = false;

                                for right_row in
                                    right_result.parts.iter().flat_map(|p| p.rows.iter())
                                {
                                    let joined_row_data = {
                                        let mut tmp = left_row.data.clone();
                                        tmp.extend(right_row.data.clone());
                                        tmp
                                    };

                                    let row = storage::Row::new(
                                        result_rows.len() as u64,
                                        joined_row_data,
                                    );

                                    if self.evaluate_ra_cond(&condition, &row, &inner_columns, placeholders, ctes).await? {
                                        result_rows.push(row);
                                        included = true;
                                    }
                                }

                                if !included {
                                    let joined_row_data = {
                                        let mut tmp = left_row.data.clone();
                                        tmp.extend(
                                            (0..(result_columns.len() - tmp.len()))
                                                .map(|_| storage::Data::Null),
                                        );
                                        tmp
                                    };

                                    let row = storage::Row::new(
                                        result_rows.len() as u64,
                                        joined_row_data,
                                    );

                                    result_rows.push(row);
                                }
                            }

                            Ok(storage::EntireRelation {
                                columns: result_columns,
                                parts: vec![storage::PartialRelation { rows: result_rows }],
                            })
                        }
                        other => {
                            dbg!(other);

                            Err(EvaulateRaError::Other("Unsupported Join Kind"))
                        }
                    }
                }
                ra::RaExpression::EmptyRelation => Ok(storage::EntireRelation {
                    columns: Vec::new(),
                    parts: vec![storage::PartialRelation {
                        rows: vec![storage::Row::new(0, Vec::new())]
                    }],
                }),
                ra::RaExpression::Aggregation {
                    inner,
                    attributes,
                    aggregation_condition,
                } => {
                    let inner_columns = inner.get_columns();
                    let inner_result = self.evaluate_ra(*inner, placeholders, ctes).await?;

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

                            for row in inner_result
                                .parts
                                .into_iter()
                                .flat_map(|p| p.rows.into_iter())
                            {
                                for state in states.iter_mut() {
                                    state.update(&self, &row).await?;
                                }
                            }

                            let row_data: Vec<_> = states
                                .into_iter()
                                .map(|s| s.to_data().ok_or(EvaulateRaError::Other("Converting AggregateState to Data")))
                                .collect::<Result<_, _>>()?;

                            Ok(storage::EntireRelation {
                                columns: result_columns,
                                parts: vec![storage::PartialRelation {
                                    rows: vec![storage::Row::new(0, row_data)],
                                }],
                            })
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

                                for row in inner_result.parts.iter().flat_map(|p| p.rows.iter()) {
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
                                        state.update(&self, &row).await?;
                                    }
                                }

                                let row_data: Vec<_> = states
                                    .into_iter()
                                    .map(|s| s.to_data().ok_or(EvaulateRaError::Other("Converting AggregateState to Data")))
                                    .collect::<Result<_, _>>()?;

                                result_rows.push(storage::Row::new(0, row_data));
                            }


                            Ok(storage::EntireRelation {
                                columns: result_columns,
                                parts: vec![storage::PartialRelation {
                                    rows: result_rows,
                                }]
                            })
                        }
                    }
                }
                ra::RaExpression::Limit { inner, limit } => {
                    let inner_result = self.evaluate_ra(*inner, placeholders, ctes).await?;

                    Ok(storage::EntireRelation {
                        columns: inner_result.columns,
                        parts: vec![storage::PartialRelation {
                            rows: inner_result.parts.into_iter().flat_map(|p| p.rows.into_iter()).take(limit).collect(),
                        }]
                    })
                }
                ra::RaExpression::OrderBy { inner, attributes } => {
                    let inner_columns = inner.get_columns();
                    let inner_result = self.evaluate_ra(*inner, placeholders, ctes).await?;

                    let orders: Vec<(usize, sql::OrderBy)> = attributes.iter().map(|(id, order)| {
                        let attribute_idx = inner_columns.iter().enumerate().find(|(_, column)| id == &column.2).map(|(idx, _)| idx).ok_or_else(|| EvaulateRaError::Other("Could not find Attribute"))?;

                        Ok((attribute_idx, order.clone()))
                    }).collect::<Result<_, _>>()?; 

                    let mut result_rows: Vec<_> = inner_result.parts.into_iter().flat_map(|p| p.rows.into_iter()).collect();

                    result_rows.sort_unstable_by(|first, second| {
                        for (attribute_idx, order) in orders.iter() {
                            let first_value = &first.data[*attribute_idx];
                        let second_value = &second.data[*attribute_idx];

                        let (first_value, second_value) = match order {
                            sql::OrderBy::Ascending => (first_value, second_value),
                            sql::OrderBy::Descending => (second_value, first_value),
                        };

                            if core::mem::discriminant(first_value) == core::mem::discriminant(second_value) {
                                match first_value.partial_cmp(second_value).expect("") {
                                    core::cmp::Ordering::Equal => {}
                                    other => return other,
                                }
                            } else {
                                todo!()
                            }
                        }
                        core::cmp::Ordering::Equal
                        
                    });

                    Ok(storage::EntireRelation {
                        columns: inner_result.columns,
                        parts: vec![storage::PartialRelation {
                            rows: result_rows,
                        }]
                    })
                }
                ra::RaExpression::CTE { name, columns } => {
                    let cte_value = ctes.get(&name).ok_or_else(|| EvaulateRaError::Other("Getting CTE"))?;

                    Ok(storage::EntireRelation { columns: cte_value.columns.clone(), parts: cte_value.parts.iter().map(|part| {
                        storage::PartialRelation {
                            rows: part.rows.clone(),
                        }
                    }).collect() })
                }
                ra::RaExpression::Chain { parts } => {
                    let mut columns = Vec::new();
                    let mut partials = Vec::new();

                    for part in parts {
                        let result = self.evaluate_ra(part, placeholders, ctes).await?;

                        columns = result.columns;
                        partials.extend(result.parts);
                    }

                    

                    Ok(storage::EntireRelation { columns, parts: partials })
                }
            }
        }
        .boxed_local()
    }

    fn evaluate_ra_cond<'s, 'cond, 'r, 'col, 'p, 'c, 'f>(
        &'s self,
        condition: &'cond ra::RaCondition,
        row: &'r storage::Row,
        columns: &'col [(String, DataType, AttributeId)],
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>
    ) -> LocalBoxFuture<'f,Result<bool, EvaulateRaError<S::LoadingError>>> where
        's: 'f,
        'cond: 'f,
        'r: 'f,
        'col: 'f,
        'p: 'f,
        'c: 'f

        {
        async move {
            match condition {
            ra::RaCondition::And(conditions) => {
                for andcond in conditions.iter() {
            if !self
                .evaluate_ra_cond(andcond, row, columns, placeholders, ctes)
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
                .evaluate_ra_cond(andcond, row, columns, placeholders, ctes)
                .await?
            {
                return Ok(true);
            }
        }

        Ok(false)
            }
            ra::RaCondition::Value(value) => {
                let res = self.evaluate_ra_cond_val(&value, row, columns, placeholders, ctes).await?;
                Ok(res)
            }
        }
        }.boxed_local()

        
    }

    fn evaluate_ra_cond_val<'s, 'cond, 'r, 'col, 'p, 'c, 'f>(
        &'s self,
        condition: &'cond ra::RaConditionValue,
        row: &'r storage::Row,
        columns: &'col [(String, DataType, AttributeId)],
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
    ) -> LocalBoxFuture<'f, Result<bool, EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'cond: 'f,
        'r: 'f,
        'col: 'f,
        'p: 'f,
        'c: 'f
    {
        async move {
            match condition {
                ra::RaConditionValue::Attribute { name, ty, a_id } => {
                    dbg!(&name, &ty, &a_id);

                    Err(EvaulateRaError::Other("Evaluating RA Condition Value not implemented yet"))
                }
                ra::RaConditionValue::Comparison {
                    first,
                    second,
                    comparison,
                } => {
                    let first_value = self
                        .evaluate_ra_value(first, row, columns, placeholders, ctes)
                        .await?;
                    let second_value = self
                        .evaluate_ra_value(second, row, columns, placeholders, ctes)
                        .await?;

                    match comparison {
                        ra::RaComparisonOperator::Equals => Ok(first_value == second_value),
                        ra::RaComparisonOperator::NotEquals => Ok(first_value != second_value),
                        ra::RaComparisonOperator::Greater => {
                            Ok(first_value > second_value)
                        }
                        ra::RaComparisonOperator::GreaterEqual => Ok(first_value >= second_value),
                        ra::RaComparisonOperator::Less => Ok(first_value < second_value),
                        ra::RaComparisonOperator::LessEqual => Ok(first_value <= second_value),
                        ra::RaComparisonOperator::In => {
                            todo!("Performing In Comparison");
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
                            todo!("Performing Like Comparison")
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
                        .evaluate_ra_cond_val(&inner, row, columns, placeholders, ctes)
                        .await?;

                    Ok(!inner_result)
                }
                ra::RaConditionValue::Exists { query } => {
                    match self.evaluate_ra(*query.clone(), placeholders, ctes).await {
                        Ok(res) => Ok(res
                            .parts
                            .into_iter()
                            .flat_map(|p| p.rows.into_iter())
                            .any(|_| true)),
                        Err(_) => Ok(false),
                    }
                }
            }
        }
        .boxed_local()
    }

    fn evaluate_ra_value<'s, 'rve, 'row, 'columns, 'placeholders, 'c, 'f>(
        &'s self,
        expr: &'rve ra::RaValueExpression,
        row: &'row storage::Row,
        columns: &'columns [(String, DataType, AttributeId)],
        placeholders: &'placeholders HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>
    ) -> LocalBoxFuture<'f, Result<storage::Data, EvaulateRaError<S::LoadingError>>>
    where
        's: 'f,
        'rve: 'f,
        'row: 'f,
        'columns: 'f,
        'placeholders: 'f,
        'c: 'f,
    {
        async move {
            match expr {
                ra::RaValueExpression::Attribute { a_id, .. } => {
                    // TODO

                    let column_index = columns
                        .iter()
                        .enumerate()
                        .find(|(_, (_, _, id))| id == a_id)
                        .map(|(i, _)| i)
                        .ok_or_else(|| EvaulateRaError::UnknownAttribute {
                            attribute: todo!()
                        })?;

                    Ok(row.data[column_index].clone())
                }
                ra::RaValueExpression::Placeholder(placeholder) => {
                    placeholders.get(placeholder).cloned().ok_or_else(|| {
                        dbg!(&placeholders, &placeholder);
                        EvaulateRaError::Other("Getting Placeholder Value")
                    })
                }
                ra::RaValueExpression::Literal(lit) => Ok(storage::Data::from_literal(lit)),
                ra::RaValueExpression::List(elems) => todo!("Evaluate List: {:?}", elems),
                ra::RaValueExpression::SubQuery { query } => {
                    let result = self.evaluate_ra(query.clone(), placeholders, ctes).await?;

                    let mut parts: Vec<_> = result
                            .parts
                            .into_iter()
                            .flat_map(|p| p.rows.into_iter())
                            .map(|r| r.data[0].clone())
                            .collect();

                                        Ok(storage::Data::List(
                        parts
                    ))
                }
                ra::RaValueExpression::Cast { inner, target } => {
                    let result = self
                        .evaluate_ra_value(&inner, row, columns, placeholders, ctes)
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
                    let first_value = self.evaluate_ra_value(&first, row, columns, placeholders, ctes).await?;
                    let second_value = self.evaluate_ra_value(&second, row, columns, placeholders, ctes).await?;

                    match operator {
                        BinaryOperator::Add => {
                            match (first_value, second_value) {
                                (Data::SmallInt(f), Data::SmallInt(s)) => Ok(Data::SmallInt(f + s)),
                                other => {
                                    dbg!(other);
                                    Err(EvaulateRaError::Other("Addition"))
                                }
                            }
                        }
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

                        let value_res = self.evaluate_ra_value(&value, row, columns, placeholders, ctes).await?;
                        dbg!(&value_res);

                        let value = match value_res {
                            Data::List(mut v) => {
                                if v.is_empty() {
                                    return Err(EvaulateRaError::Other(""));
                                }

                                v.swap_remove(0)
                            },
                            other => other,
                        };

                        dbg!(&value);

                        // TODO
                        // Actually update the Sequence Value

                        Ok(value)
                    }
                    ra::RaFunction::Lower(val) => {
                        dbg!(&val);

                        let data = self.evaluate_ra_value(&val, row, columns, placeholders, ctes).await?;

                        match data {
                            storage::Data::Text(d) => Ok(storage::Data::Text(d.to_lowercase())),
                            other => {
                                dbg!(&other);
                                Err(EvaulateRaError::Other("Unexpected Type"))
                            }
                        }                        
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
    async fn execute_cte<'p, 'c>(&self, cte: &ra::CTE, placeholders: &'p HashMap<usize, storage::Data>, ctes: &'c HashMap<String, storage::EntireRelation>) -> Result<storage::EntireRelation, EvaulateRaError<S::LoadingError>> {
        tracing::debug!("CTE: {:?}", cte);

        match &cte.value {
            ra::CTEValue::Standard { query } => {
                match query {
                    ra::CTEQuery::Select(s) => {
                        let evaluated = self.evaluate_ra(s.clone(), placeholders, ctes).await?;
                        Ok(evaluated)
                    }
                }
            }
            ra::CTEValue::Recursive { query, columns } => {
                match query {
                    ra::CTEQuery::Select(s) => {
                        let s_columns = s.get_columns();

                        let result_columns: Vec<_> = s_columns.into_iter().zip(columns.iter()).map(|((n, ty, _), name)| (name.clone(), ty, Vec::new())).collect();

                        let result = storage::EntireRelation {
                            columns: result_columns,
                            parts: vec![],
                        };

                        let mut inner_cte: HashMap<_, _> = ctes.iter().map(|(c, s)| (c.clone(), storage::EntireRelation {
                            columns: s.columns.clone(),
                            parts: s.parts.iter().map(|p| storage::PartialRelation {
                                rows: p.rows.clone(),
                            }).collect(),
                        })).collect();

                        inner_cte.insert(cte.name.clone(), result);

                        loop {
                            let mut tmp = self.evaluate_ra(s.clone(), placeholders, &inner_cte).await?;

                            for (column, name) in tmp.columns.iter_mut().zip(columns.iter()) {
                                column.0 = name.clone();
                            }
                            
                            let tmp_rows = tmp.parts.iter().flat_map(|p| p.rows.iter());
                            let previous_rows = inner_cte.get(&cte.name).into_iter().flat_map(|s| s.parts.iter().flat_map(|p| p.rows.iter()));

                            if tmp_rows.count() == previous_rows.count() {
                                break;
                            }

                            inner_cte.insert(cte.name.clone(), tmp);
                        }

                        let result = inner_cte.remove(&cte.name).unwrap();

                        Ok(result)
                    }
                }
            }
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

impl<S> Execute for NaiveEngine<S>
where
    S: crate::storage::Storage,
{
    type Prepared = NaivePrepared;
    type PrepareError = PrepareError<S::LoadingError>;
    type ExecuteBoundError = ExecuteBoundError<S::LoadingError>;

    async fn prepare<'q>(
        &self,
        query: &crate::sql::Query<'q>,
        _ctx: &mut Context,
    ) -> Result<Self::Prepared, Self::PrepareError> {
        let (expected, columns) = match query {
            Query::Select(s) => {
                let schemas = self
                    .storage
                    .schemas()
                    .await
                    .map_err(|e| PrepareError::LoadingSchemas(e))?;

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
                    .map(|(name, dtype, _)| (name, dtype))
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
                            .map_err(|e| PrepareError::LoadingSchemas(e))?;
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

    #[tracing::instrument(skip(self, query, ctx))]
    async fn execute_bound(
        &self,
        query: &<Self::Prepared as PreparedStatement>::Bound,
        ctx: &mut Context,
    ) -> Result<ExecuteResult, Self::ExecuteBoundError> {
        tracing::debug!("Executing Bound Query");
        tracing::debug!("{:#?}", query.query);

        let mut parse_context  = ra::ParsingContext::new();
        let mut to_process = Some(&query.query);

        let mut cte_queries: HashMap<String, storage::EntireRelation> = HashMap::new();

        while let Some(inner_query) = to_process.take() {
            let result = match inner_query {
            Query::Select(select) => {
                tracing::debug!("Selecting: {:?}", select);

                let schemas = self
                    .storage
                    .schemas()
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                let (ra_expression, placeholder_types) =
                    match crate::ra::RaExpression::parse_select_with_context(select, &schemas, &parse_context) {
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
                        let tmp = storage::Data::realize(ty, &value).map_err(|e| {
                            Self::ExecuteBoundError::RealizingValueFromRaw {
                                value: value.clone(),
                                target: (*ty).clone(),
                            }
                        })?;

                        Ok((*name, tmp))
                    })
                    .collect::<Result<_, Self::ExecuteBoundError>>()?;

                tracing::trace!("Placeholder-Values: {:#?}", placeholder_values);

                let r = match self.evaluate_ra(ra_expression, &placeholder_values, &cte_queries).await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!("RA-Error: {:?}", e);
                        return Err(ExecuteBoundError::Executing(e));
                    }
                };

                tracing::debug!("RA-Result: {:?}", r);

                Ok(ExecuteResult::Select { content: r })
            }
            Query::Insert(ins) => {
                tracing::debug!("Inserting: {:?}", ins);

                let schemas = self
                    .storage
                    .schemas()
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;
                let table_schema = schemas
                    .get_table(ins.table.0.as_ref())
                    .ok_or_else(|| ExecuteBoundError::Other("Could not find Table in schemas"))?;

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
                                            .map_err(
                                                |e| ExecuteBoundError::RealizingValueFromRaw {
                                                    value: query.values[*pnumb - 1].to_vec(),
                                                    target: (*field_type).clone(),
                                                },
                                            )
                                        }
                                        sql::ValueExpression::Literal(lit) => {
                                            let data = storage::Data::from_literal(lit);
                                            // TODO
                                            // Check if data and expected types match

                                            Ok(data)
                                        }
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
                            ExecuteResult::Select { content } => content,
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
                            });
                        }

                        return Err(ExecuteBoundError::NotImplemented(
                            "Getting InsertValues from Select",
                        ));
                    }
                };

                tracing::trace!("Values: {:?}", values);

                let relation = if table_schema.rows.iter().any(|c| &c.ty == &DataType::Serial) {
                    let relation = self
                        .storage
                        .get_entire_relation(&ins.table.0)
                        .await
                        .map_err(|e| ExecuteBoundError::StorageError(e))?;
                    Some(relation)
                } else {
                    None
                };

                let mut insert_rows: Vec<Vec<storage::Data>> = Vec::new();

                for values in values {
                    let mut rows: Vec<storage::Data> = Vec::with_capacity(table_schema.rows.len());
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
                                if &column.ty == &DataType::Serial {
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
                    .insert_rows(&ins.table.0, &mut insert_rows.into_iter())
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                Ok(ExecuteResult::Insert {
                    returning,
                    inserted_rows,
                })
            }
            Query::Update(update) => {
                tracing::info!("Update: {:#?}", update);

                let relation = self
                    .storage
                    .get_entire_relation(&update.table.0)
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                let schemas = self
                    .storage
                    .schemas()
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                tracing::info!("Relation: {:#?}", relation);

                let (ra_update, ra_placeholders) = RaUpdate::parse(update, &schemas)
                    .map_err(|e| ExecuteBoundError::ParseRelationAlgebra(e))?;

                let placeholder_values: HashMap<_, _> = ra_placeholders
                    .iter()
                    .map(|(name, ty)| {
                        let value = query.values.get(*name - 1).unwrap();
                        let tmp = storage::Data::realize(ty, &value).map_err(|e| {
                            Self::ExecuteBoundError::RealizingValueFromRaw {
                                value: value.clone(),
                                target: (*ty).clone(),
                            }
                        })?;

                        Ok((*name, tmp))
                    })
                    .collect::<Result<_, Self::ExecuteBoundError>>()?;

                tracing::info!("Placerholder Types: {:?}", ra_placeholders);

                let table_columns: Vec<_> = relation.columns.iter().enumerate().map(|(i, c)| (c.0.clone(), c.1.clone(), AttributeId::new(i))).collect();

                match ra_update {
                    ra::RaUpdate::Standard { fields, condition } => {
                        let mut count = 0;
                        for mut row in relation.parts.into_iter().flat_map(|p| p.rows.into_iter()) {
                            let should_update = match condition.as_ref() {
                                Some(cond) => self
                                    .evaluate_ra_cond(
                                        cond,
                                        &row,
                                        &table_columns,
                                        &placeholder_values,&cte_queries
                                    )
                                    .await
                                    .map_err(|e| ExecuteBoundError::Executing(e))?,
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
                                        &placeholder_values,&cte_queries
                                    )
                                    .await
                                    .map_err(|e| ExecuteBoundError::Executing(e))?;

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
                                )
                                .await
                                .map_err(|e| ExecuteBoundError::StorageError(e))?;
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

                        let table = self
                            .storage
                            .get_entire_relation(update.table.0.as_ref())
                            .await
                            .map_err(|e| ExecuteBoundError::StorageError(e))?;

                        if !table.parts.iter().flat_map(|p| p.rows.iter()).any(|_| true) {
                            return Ok(ExecuteResult::Update { updated_rows: 0 });
                        }

                        Err(ExecuteBoundError::NotImplemented("Update FROM extension"))
                    }
                }
            }
            Query::Delete(delete) => {
                tracing::info!("Deleting: {:#?}", delete);

                let schemas = self
                    .storage
                    .schemas()
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                let (ra_delete, placeholders) = ra::RaDelete::parse(delete, &schemas)
                    .map_err(|e| ExecuteBoundError::ParseRelationAlgebra(e))?;

                let relation = self
                    .storage
                    .get_entire_relation(&ra_delete.table)
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                let placeholders: HashMap<_, _> = placeholders
                    .iter()
                    .map(|(name, ty)| {
                        let value = query.values.get(*name - 1).unwrap();
                        let tmp = storage::Data::realize(ty, &value).map_err(|e| {
                            Self::ExecuteBoundError::RealizingValueFromRaw {
                                value: value.clone(),
                                target: (*ty).clone(),
                            }
                        })?;

                        Ok((*name, tmp))
                    })
                    .collect::<Result<_, Self::ExecuteBoundError>>()?;

                    let table_columns: Vec<_> = relation.columns.iter().enumerate().map(|(i, c)| (c.0.clone(), c.1.clone(), AttributeId::new(i))).collect();

                let to_delete = {
                    let mut tmp = Vec::new();

                    for row in relation.parts.into_iter().flat_map(|p| p.rows.into_iter()) {
                        if let Some(condition) = ra_delete.condition.as_ref() {
                            tracing::info!("Evaluating Condition: {:#?}", condition);

                            let condition_result = self
                                .evaluate_ra_cond(
                                    condition,
                                    &row,
                                    &table_columns,
                                    &placeholders,
                                    &cte_queries
                                )
                                .await
                                .map_err(|e| ExecuteBoundError::Executing(e))?;

                            if !condition_result {
                                continue;
                            }
                        }

                        tmp.push(row.id());
                    }

                    tmp
                };

                let row_count = to_delete.len();

                self.storage
                    .delete_rows(&delete.table.0, &mut to_delete.into_iter())
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                Ok(ExecuteResult::Delete {
                    deleted_rows: row_count,
                })
            }
            Query::CreateTable(create) => {
                tracing::debug!("Creating: {:?}", create);

                if create.if_not_exists
                    && self
                        .storage
                        .relation_exists(&create.identifier.0)
                        .await
                        .map_err(|e| ExecuteBoundError::StorageError(e))?
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
                    .create_relation(&create.identifier.0, fields)
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                Ok(ExecuteResult::Create)
            }
            Query::CreateIndex(create) => {
                tracing::debug!("Creating Index: {:?}", create);

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
                    )
                    .await
                    .unwrap();

                Ok(ExecuteResult::Create)
            }
            Query::AlterTable(alter) => {
                tracing::debug!("Alter Table");

                match alter {
                    sql::AlterTable::Rename { from, to } => {
                        tracing::debug!("Renaming from {:?} -> {:?}", from, to);

                        self.storage
                            .rename_relation(&from.0, &to.0)
                            .await
                            .map_err(|e| ExecuteBoundError::StorageError(e))?;

                        Ok(ExecuteResult::Alter)
                    }
                    sql::AlterTable::RenameColumn { table, from, to } => {
                        tracing::debug!("Renaming Column from {:?} -> {:?}", from, to);

                        let mut modifications = storage::ModifyRelation::new();

                        modifications.rename_column(from.0.as_ref(), to.0.as_ref());

                        self.storage
                            .modify_relation(&table.0, modifications)
                            .await
                            .map_err(|e| ExecuteBoundError::StorageError(e))?;

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
                            .modify_relation(&table.0, modifications)
                            .await
                            .map_err(|e| ExecuteBoundError::StorageError(e))?;

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
                            .modify_relation(&table.0, modifications)
                            .await
                            .map_err(|e| ExecuteBoundError::StorageError(e))?;

                        Ok(ExecuteResult::Alter)
                    }
                    sql::AlterTable::AtlerColumnDropNotNull { table, column } => {
                        let mut modifications = storage::ModifyRelation::new();

                        modifications.remove_modifier(&column.0, TypeModifier::NotNull);

                        self.storage
                            .modify_relation(&table.0, modifications)
                            .await
                            .map_err(|e| ExecuteBoundError::StorageError(e))?;

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
                            .modify_relation(&table.0, modifications)
                            .await
                            .map_err(|e| ExecuteBoundError::StorageError(e))?;

                        Ok(ExecuteResult::Alter)
                    }
                }
            }
            Query::DropIndex(drop_index) => {
                tracing::debug!("Dropping Index: {:?}", drop_index);

                let pg_indexes_table = self
                    .storage
                    .get_entire_relation("pg_indexes")
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                let mut cid = pg_indexes_table
                    .parts
                    .iter()
                    .flat_map(|p| p.rows.iter())
                    .filter(|row| row.data[2] == storage::Data::Name(drop_index.name.0.to_string()))
                    .map(|row| row.id());

                self.storage
                    .delete_rows("pg_indexes", &mut cid)
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                Ok(ExecuteResult::Drop_)
            }
            Query::DropTable(drop_table) => {
                self.storage
                    .remove_relation(&drop_table.name.0)
                    .await
                    .map_err(|e| ExecuteBoundError::StorageError(e))?;

                Ok(ExecuteResult::Drop_)
            }
            Query::BeginTransaction(isolation) => {
                tracing::debug!("Starting Transaction: {:?}", isolation);

                ctx.transaction = Some(());

                Ok(ExecuteResult::Begin)
            }
            Query::CommitTransaction => {
                tracing::debug!("Committing Transaction");
                // TODO

                Ok(ExecuteResult::Commit)
            }
            Query::RollbackTransaction => {
                tracing::debug!("Rollback Transaction");

                Err(ExecuteBoundError::NotImplemented(
                    "Rolling back a transaction",
                ))
            }
            Query::WithCTE { cte, query } => {
                let schemas = self.storage.schemas().await.map_err(|e| ExecuteBoundError::StorageError(e))?;

                let (ra_cte, cte_placeholder_types) = ra::parse_ctes(cte, &schemas).map_err(|e| ExecuteBoundError::ParseRelationAlgebra(e))?;
                dbg!(&ra_cte);

                for cte in ra_cte {
                    parse_context.add_cte(cte.clone());
                    
                    let cte_result = self.execute_cte(&cte, &HashMap::new(), &HashMap::new()).await.map_err(|e| ExecuteBoundError::Executing(e))?;
                    cte_queries.insert(cte.name.clone(), cte_result);
                }

                to_process = Some(&query);
                continue;
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

    fn bind(&self, values: Vec<Vec<u8>>) -> Result<Self::Bound, ()> {
        Ok(NaiveBound {
            query: self.query.to_static(),
            expected_parameters: self.expected_parameters.clone(),
            // columns: self.columns.clone(),
            values,
        })
    }

    fn parameters(&self) -> Vec<DataType> {
        self.expected_parameters.clone()
    }

    fn row_columns(&self) -> Vec<(String, DataType)> {
        self.columns.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{Data, EntireRelation, PartialRelation, Row, Storage};

    use self::storage::inmemory::InMemoryStorage;

    use super::*;

    #[tokio::test]
    async fn execute_delete_with_subquery() {
        let query = "DELETE FROM dashboard_acl WHERE dashboard_id NOT IN (SELECT id FROM dashboard) AND dashboard_id != -1";

        let storage = {
            let storage = InMemoryStorage::new();

            storage
                .create_relation(
                    "dashboard_acl",
                    vec![("dashboard_id".into(), DataType::Integer, Vec::new())],
                )
                .await
                .unwrap();

            storage
                .create_relation(
                    "dashboard",
                    vec![("id".into(), DataType::Integer, Vec::new())],
                )
                .await
                .unwrap();

            storage
                .insert_rows("dashboard", &mut vec![vec![Data::Integer(1)]].into_iter())
                .await
                .unwrap();

            storage
                .insert_rows(
                    "dashboard_acl",
                    &mut vec![vec![Data::Integer(132)], vec![Data::Integer(1)]].into_iter(),
                )
                .await
                .unwrap();

            storage
        };
        let engine = NaiveEngine::new(storage);

        let query = Query::parse(query.as_bytes()).unwrap();
        let res = engine.execute(&query, &mut Context::new()).await.unwrap();

        assert_eq!(ExecuteResult::Delete { deleted_rows: 1 }, res);
    }

    #[tokio::test]
    async fn using_is_true() {
        let query_str = "SELECT name FROM user WHERE active IS TRUE";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        let storage = {
            let storage = InMemoryStorage::new();

            storage
                .create_relation(
                    "user",
                    vec![
                        ("name".into(), DataType::Text, Vec::new()),
                        ("active".into(), DataType::Bool, Vec::new()),
                    ],
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
                )
                .await
                .unwrap();

            storage
        };
        let engine = NaiveEngine::new(storage);

        let res = engine.execute(&query, &mut Context::new()).await.unwrap();

        dbg!(&res);

        assert_eq!(
            ExecuteResult::Select {
                content: EntireRelation {
                    columns: vec![("name".to_string(), DataType::Text, Vec::new())],
                    parts: vec![PartialRelation {
                        rows: vec![Row::new(0, vec![Data::Text("second-user".to_string())])]
                    }]
                }
            },
            res
        );
    }
}
