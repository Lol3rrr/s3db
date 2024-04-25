//! A naive execution engine, that is mostly there to have a correct implementation of all the
//! parts needed, however it is not very efficient or performant

use std::collections::HashMap;

use futures::{
    stream::{LocalBoxStream, StreamExt},
    FutureExt,
};

use crate::{
    execution::algorithms::{self, joins::Join},
    postgres::FormatCode,
    ra::{self, AttributeId, RaExpression, RaUpdate},
};

use sql::{CompatibleParser, DataType, Query, TypeModifier};
use storage::{self, Data, Storage, TableSchema};

use super::{Context, CopyState, Execute, ExecuteResult, PreparedStatement};

mod aggregate;
use aggregate::AggregateState;

mod condition;
mod mapping;
mod pattern;
mod value;

pub struct NaiveEngine<S> {
    storage: S,
}

pub struct NaiveEngineConditionEval<
    'engine,
    'columns,
    'placeholders,
    'ctes,
    'outer,
    'transaction,
    'arena,
    S,
    TG,
> {
    engine: &'engine NaiveEngine<S>,
    columns: &'columns [(String, DataType, AttributeId)],
    placeholders: &'placeholders HashMap<usize, Data>,
    ctes: &'ctes HashMap<String, storage::EntireRelation>,
    outer: &'outer HashMap<AttributeId, storage::Data>,
    transaction: &'transaction TG,
    arena: &'arena bumpalo::Bump,
}

impl<S> algorithms::joins::EvaluateConditions<S::LoadingError>
    for NaiveEngineConditionEval<'_, '_, '_, '_, '_, '_, '_, S, S::TransactionGuard>
where
    S: Storage,
{
    async fn evaluate(
        &self,
        condition: &ra::RaCondition,
        row: &storage::Row,
    ) -> Result<bool, EvaulateRaError<S::LoadingError>> {
        let mapper = condition::Mapper::construct(
            condition,
            (self.columns, self.placeholders, self.ctes, self.outer),
        )?;

        mapper
            .evaluate(row, self.engine, self.transaction, self.arena)
            .await
            .ok_or_else(|| EvaulateRaError::Other("testing"))
    }
}

#[derive(Debug)]
pub struct NaivePrepared {
    query: Query<'static, 'static>,
    expected_parameters: Vec<DataType>,
    columns: Vec<(String, DataType)>,
}

#[derive(Debug)]
pub struct NaiveBound {
    query: Query<'static, 'static>,
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
    async fn evaluate_ra<'s, 'exp, 'p, 'f, 'o, 'c, 't, 'arena>(
        &'s self,
        expr: &'exp ra::RaExpression,
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
        outer: &'o HashMap<AttributeId, storage::Data>,
        transaction: &'t S::TransactionGuard,
        arena: &'arena bumpalo::Bump,
    ) -> Result<
        (storage::TableSchema, LocalBoxStream<'f, storage::Row>),
        EvaulateRaError<S::LoadingError>,
    >
    where
        's: 'f,
        'p: 'f,
        'c: 'f,
        'o: 'f,
        't: 'f,
        'exp: 'f,
        'arena: 'f,
    {
        let mut pending: bumpalo::collections::Vec<'_, &RaExpression> =
            bumpalo::vec![in arena; expr];
        let mut expression_stack: bumpalo::collections::Vec<'_, StackedExpr> =
            bumpalo::collections::Vec::new_in(arena);
        while let Some(tmp) = pending.pop() {
            expression_stack.push(StackedExpr { expr: tmp });
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
                ra::RaExpression::Join { left, right, .. } => {
                    pending.push(left);
                    pending.push(right);
                }
                ra::RaExpression::LateralJoin { left, .. } => {
                    // We only push the left side, because we want to evaluate the right side
                    // in a recursive way later
                    pending.push(left);
                }
                ra::RaExpression::Aggregation { inner, .. } => {
                    pending.push(inner);
                }
                ra::RaExpression::Limit { inner, .. } => {
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

        let mut results: bumpalo::collections::Vec<
            '_,
            (TableSchema, LocalBoxStream<'_, storage::Row>),
        > = bumpalo::collections::Vec::with_capacity_in(expression_stack.len(), arena);
        for stacked_expr in expression_stack.into_iter().rev() {
            let expr = stacked_expr.expr;

            let result = match expr {
                ra::RaExpression::EmptyRelation => (
                    TableSchema { rows: Vec::new() },
                    futures::stream::iter(vec![storage::Row::new(0, Vec::new())]).boxed_local(),
                ),
                ra::RaExpression::Renamed { .. } => results.pop().unwrap(),
                ra::RaExpression::BaseRelation { name, .. } => self
                    .storage
                    .stream_relation(&name.0, transaction)
                    .await
                    .map_err(EvaulateRaError::StorageError)?,
                ra::RaExpression::CTE { name, .. } => {
                    let cte_value = ctes
                        .get(name)
                        .ok_or_else(|| EvaulateRaError::Other("Getting CTE"))?;

                    let table_schema = TableSchema {
                        rows: cte_value
                            .columns
                            .iter()
                            .map(|(name, ty, mods)| storage::ColumnSchema {
                                name: name.to_string(),
                                mods: mods.to_vec(),
                                ty: ty.clone(),
                            })
                            .collect(),
                    };

                    let stream = futures::stream::iter(
                        cte_value.parts.iter().flat_map(|p| p.rows.iter().cloned()),
                    )
                    .boxed_local();

                    (table_schema, stream)
                }
                ra::RaExpression::Projection { inner, attributes } => {
                    let (table_schema, rows) = results.pop().unwrap();

                    let inner_columns = inner
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, t, i)| (n, t, i))
                        .collect::<Vec<_>>();

                    assert_eq!(
                        inner_columns
                            .iter()
                            .map(|(n, t, _)| (n, t))
                            .collect::<Vec<_>>(),
                        table_schema
                            .rows
                            .iter()
                            .map(|c| (&c.name, &c.ty))
                            .collect::<Vec<_>>()
                    );

                    let columns: Vec<_> = attributes
                        .iter()
                        .map(|attr| storage::ColumnSchema {
                            name: attr.name.clone(),
                            ty: attr.value.datatype().unwrap(),
                            mods: Vec::new(),
                        })
                        .collect();

                    let mut mappers = Vec::new();
                    for attribute in attributes.iter() {
                        let mapper = value::Mapper::construct(
                            &attribute.value,
                            (&inner_columns, placeholders, ctes, &outer),
                        )?;
                        mappers.push(mapper);
                    }

                    let mappers = std::rc::Rc::new(std::cell::RefCell::new(mappers));

                    let stream: LocalBoxStream<'_, storage::Row> =
                        StreamExt::boxed_local(StreamExt::then(rows, move |row| {
                            let mappers = mappers.clone();

                            async move {
                                let mut mappers = mappers.try_borrow_mut().expect("");
                                let mut data = Vec::with_capacity(mappers.len());
                                for mapper in mappers.iter_mut() {
                                    let tmp = mapper
                                        .evaluate_mut(&row, self, transaction, arena)
                                        .await
                                        .unwrap();

                                    let tmp = match tmp {
                                        Data::List(mut v) => {
                                            assert_eq!(1, v.len());
                                            v.pop().unwrap()
                                        }
                                        other => other,
                                    };

                                    data.push(tmp);
                                }
                                storage::Row::new(0, data)
                            }
                        }));

                    (TableSchema { rows: columns }, stream)
                }
                ra::RaExpression::Selection { inner, filter } => {
                    let (table_schema, rows) = results.pop().unwrap();

                    let inner_columns = inner
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, t, i)| (n, t, i))
                        .collect::<Vec<_>>();

                    assert_eq!(
                        inner_columns
                            .iter()
                            .map(|(n, t, _)| (n, t))
                            .collect::<Vec<_>>(),
                        table_schema
                            .rows
                            .iter()
                            .map(|c| (&c.name, &c.ty))
                            .collect::<Vec<_>>()
                    );

                    let condition_mapper = condition::Mapper::construct::<S::LoadingError>(
                        filter,
                        (&inner_columns, placeholders, ctes, outer),
                    )?;
                    let condition_mapper =
                        std::rc::Rc::new(std::cell::RefCell::new(condition_mapper));

                    let stream = StreamExt::filter_map(rows, move |row| {
                        let condition_mapper = condition_mapper.clone();

                        async move {
                            let mut condition_mapper = condition_mapper.try_borrow_mut().unwrap();
                            if condition_mapper
                                .evaluate_mut(&row, self, transaction, arena)
                                .await
                                .unwrap()
                            {
                                Some(row)
                            } else {
                                None
                            }
                        }
                    })
                    .boxed_local();

                    (table_schema, stream)
                }
                ra::RaExpression::Aggregation {
                    inner,
                    attributes,
                    aggregation_condition,
                } => {
                    let (_, mut rows) = results.pop().unwrap();

                    let inner_columns = inner
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, t, i)| (n, t, i))
                        .collect::<Vec<_>>();

                    let result_columns: Vec<_> = attributes
                        .iter()
                        .map(|attr| storage::ColumnSchema {
                            name: attr.name.clone(),
                            ty: attr.value.return_ty(),
                            mods: Vec::new(),
                        })
                        .collect();

                    let table_schema = TableSchema {
                        rows: result_columns,
                    };

                    match aggregation_condition {
                        ra::AggregationCondition::Everything => {
                            let mut states = Vec::with_capacity(attributes.len());
                            for attr in attributes.iter() {
                                states.push(
                                    AggregateState::new::<S>(
                                        &attr.value,
                                        &inner_columns,
                                        placeholders,
                                        ctes,
                                        outer,
                                    )
                                    .await,
                                );
                            }

                            while let Some(row) = rows.next().await {
                                for state in states.iter_mut() {
                                    state.update(self, &row, transaction, &arena).await?;
                                }
                            }

                            let row_data: Vec<_> = states
                                .into_iter()
                                .map(|s| {
                                    s.to_data().ok_or(EvaulateRaError::Other(
                                        "Converting AggregateState to Data",
                                    ))
                                })
                                .collect::<Result<_, _>>()?;

                            (
                                table_schema,
                                futures::stream::once(async { storage::Row::new(0, row_data) })
                                    .boxed_local(),
                            )
                        }
                        ra::AggregationCondition::GroupBy { fields } => {
                            let groups: Vec<Vec<storage::Row>> = {
                                let mut tmp: Vec<Vec<storage::Row>> = Vec::new();

                                let compare_indices: Vec<_> = fields
                                    .iter()
                                    .map(|(_, src_id)| {
                                        inner_columns
                                            .iter()
                                            .enumerate()
                                            .find(|(_, (_, _, c_id))| c_id == src_id)
                                            .map(|(i, _)| i)
                                            .unwrap()
                                    })
                                    .collect();
                                let grouping_func =
                                    |first: &storage::Row, second: &storage::Row| {
                                        compare_indices
                                            .iter()
                                            .all(|idx| first.data[*idx] == second.data[*idx])
                                    };

                                while let Some(row) = rows.next().await {
                                    let mut grouped = false;
                                    for group in tmp.iter_mut() {
                                        let group_row = group.first().expect("We only insert Groups with at least one Row in them, so every group must have a first element");

                                        if grouping_func(&row, group_row) {
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
                                let mut states = Vec::with_capacity(attributes.len());
                                for attr in attributes.iter() {
                                    states.push(
                                        AggregateState::new::<S>(
                                            &attr.value,
                                            &inner_columns,
                                            placeholders,
                                            ctes,
                                            outer,
                                        )
                                        .await,
                                    );
                                }

                                for row in group {
                                    for state in states.iter_mut() {
                                        state.update(self, &row, transaction, &arena).await?;
                                    }
                                }

                                let row_data: Vec<_> = states
                                    .into_iter()
                                    .map(|s| {
                                        s.to_data().ok_or(EvaulateRaError::Other(
                                            "Converting AggregateState to Data",
                                        ))
                                    })
                                    .collect::<Result<_, _>>()?;

                                result_rows.push(storage::Row::new(0, row_data));
                            }

                            (
                                table_schema,
                                futures::stream::iter(result_rows).boxed_local(),
                            )
                        }
                    }
                }
                ra::RaExpression::Chain { parts } => {
                    let mut table_schema = TableSchema { rows: Vec::new() };

                    let mut part_results = Vec::with_capacity(parts.len());
                    for _ in parts {
                        let (part_table, part_rows) = results.pop().unwrap();

                        table_schema = part_table;
                        part_results.push(part_rows);
                    }

                    part_results.reverse();

                    let stream = futures::stream::iter(part_results).flatten().boxed_local();

                    (table_schema, stream)
                }
                ra::RaExpression::OrderBy { inner, attributes } => {
                    let inner_columns = inner
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, t, i)| (n, t, i))
                        .collect::<Vec<_>>();
                    let (table_schema, rows) = results.pop().unwrap();

                    let orders: Vec<(usize, sql::OrderBy)> = attributes
                        .iter()
                        .map(|(id, order)| {
                            let attribute_idx = inner_columns
                                .iter()
                                .enumerate()
                                .find(|(_, column)| id == &column.2)
                                .map(|(idx, _)| idx)
                                .ok_or_else(|| {
                                    EvaulateRaError::Other("Could not find Attribute")
                                })?;

                            Ok((attribute_idx, order.clone()))
                        })
                        .collect::<Result<_, _>>()?;

                    let mut result_rows: Vec<_> = rows.collect().await;

                    result_rows.sort_unstable_by(|first, second| {
                        for (attribute_idx, order) in orders.iter() {
                            let first_value = &first.data[*attribute_idx];
                            let second_value = &second.data[*attribute_idx];

                            let (first_value, second_value) = match order {
                                sql::OrderBy::Ascending => (first_value, second_value),
                                sql::OrderBy::Descending => (second_value, first_value),
                            };

                            if core::mem::discriminant::<storage::Data>(first_value)
                                == core::mem::discriminant(second_value)
                            {
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

                    (
                        table_schema,
                        futures::stream::iter(result_rows).boxed_local(),
                    )
                }
                ra::RaExpression::Join {
                    left,
                    right,
                    kind,
                    condition,
                    ..
                } => {
                    tracing::info!("Executing Join");

                    let inner_columns = {
                        let mut tmp = Vec::new();
                        tmp.extend(left.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)));
                        tmp.extend(
                            right
                                .get_columns()
                                .into_iter()
                                .map(|(_, n, t, i)| (n, t, i)),
                        );
                        tmp
                    };

                    let (right_schema, right_rows) = results.pop().unwrap();
                    let (left_schema, left_rows) = results.pop().unwrap();

                    let result_columns = {
                        let mut tmp = left_schema.rows.clone();
                        tmp.extend(right_schema.rows.clone());
                        tmp
                    };

                    let (schema, rows) = algorithms::joins::Naive {}
                        .execute(
                            algorithms::joins::JoinArguments {
                                kind: kind.clone(),
                                conditon: condition,
                            },
                            algorithms::joins::JoinContext {},
                            result_columns,
                            left_rows,
                            right_rows,
                            &NaiveEngineConditionEval {
                                engine: self,
                                columns: &inner_columns,
                                placeholders,
                                ctes,
                                outer,
                                transaction,
                                arena,
                            },
                        )
                        .await?;

                    (schema, rows.boxed_local())
                }
                ra::RaExpression::LateralJoin {
                    left,
                    right,
                    kind,
                    condition,
                } => {
                    tracing::info!("Executing LateralJoin");

                    async {
                        let inner_columns = {
                            let mut tmp = Vec::new();
                            tmp.extend(
                                left.get_columns().into_iter().map(|(_, n, t, i)| (n, t, i)),
                            );
                            tmp.extend(
                                right
                                    .get_columns()
                                    .into_iter()
                                    .map(|(_, n, t, i)| (n, t, i)),
                            );
                            tmp
                        };

                        let (left_schema, mut left_rows) = results.pop().unwrap();

                        let left_columns = left.get_columns();
                        let right_columns = right.get_columns();

                        let mut total_result: Option<(TableSchema, Vec<storage::Row>)> = None;

                        let left_result_columns = left_schema.rows.clone();
                        while let Some(row) = left_rows.next().await {
                            let mut outer = outer.clone();
                            outer.extend(
                                left_columns
                                    .iter()
                                    .map(|(_, _, _, id)| *id)
                                    .zip(row.data.iter().cloned()),
                            );
                            let (right_schema, right_stream) = self
                                .evaluate_ra(
                                    &right,
                                    placeholders,
                                    ctes,
                                    &outer,
                                    transaction,
                                    &arena,
                                )
                                .await?;

                            let result_columns = {
                                let mut tmp = left_result_columns.clone();
                                tmp.extend(right_schema.rows);
                                tmp
                            };

                            let row_result = futures::stream::once(async { row }).boxed_local();

                            let (joined_schema, joined_rows) = algorithms::joins::Naive {}
                                .execute(
                                    algorithms::joins::JoinArguments {
                                        kind: kind.clone(),
                                        conditon: condition,
                                    },
                                    algorithms::joins::JoinContext {},
                                    result_columns,
                                    row_result,
                                    right_stream,
                                    &NaiveEngineConditionEval {
                                        engine: self,
                                        columns: &inner_columns,
                                        placeholders,
                                        ctes,
                                        outer: &outer,
                                        transaction,
                                        arena,
                                    },
                                )
                                .await?;

                            let joined_rows_vec: Vec<_> = joined_rows.collect().await;

                            match total_result.as_mut() {
                                Some(existing) => {
                                    existing.1.extend(joined_rows_vec);
                                }
                                None => {
                                    total_result = Some((joined_schema, joined_rows_vec));
                                }
                            };
                        }

                        match total_result {
                            Some((schema, rows)) => {
                                Ok((schema, futures::stream::iter(rows).boxed_local()))
                            }
                            None => {
                                let tmp = left_columns
                                    .into_iter()
                                    .chain(right_columns.into_iter())
                                    .map(|(_, name, ty, _)| storage::ColumnSchema {
                                        name: name.clone(),
                                        ty: ty.clone(),
                                        mods: Vec::new(),
                                    })
                                    .collect();

                                Ok((
                                    TableSchema { rows: tmp },
                                    futures::stream::empty().boxed_local(),
                                ))
                            }
                        }
                    }
                    .boxed_local()
                    .await?
                }
                ra::RaExpression::Limit { limit, offset, .. } => {
                    let (table_schema, rows) = results.pop().unwrap();

                    (table_schema, rows.skip(*offset).take(*limit).boxed_local())
                }
            };

            results.push(result);
        }

        let result = results.pop().unwrap();
        Ok(result)
    }

    #[tracing::instrument(skip(self, cte, arena))]
    async fn execute_cte<'p, 'c, 't, 'arena>(
        &self,
        cte: &ra::CTE,
        placeholders: &'p HashMap<usize, storage::Data>,
        ctes: &'c HashMap<String, storage::EntireRelation>,
        transaction: &'t S::TransactionGuard,
        arena: &'arena bumpalo::Bump,
    ) -> Result<storage::EntireRelation, EvaulateRaError<S::LoadingError>> {
        tracing::debug!("CTE: {:?}", cte);

        match &cte.value {
            ra::CTEValue::Standard { query } => match query {
                ra::CTEQuery::Select(s) => {
                    let tmp = HashMap::new();
                    let (evaluated_schema, rows) = self
                        .evaluate_ra(&s, placeholders, ctes, &tmp, transaction, &arena)
                        .await?;
                    Ok(storage::EntireRelation::from_parts(
                        evaluated_schema,
                        rows.collect().await,
                    ))
                }
            },
            ra::CTEValue::Recursive { query, columns } => match query {
                ra::CTEQuery::Select(s) => {
                    let s_columns = s.get_columns();

                    let result_columns: Vec<_> = s_columns
                        .into_iter()
                        .zip(columns.iter())
                        .map(|((_, _, ty, _), name)| (name.clone(), ty, Vec::new()))
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
                        let tmp = HashMap::new();
                        let (mut tmp_schema, tmp_rows) = self
                            .evaluate_ra(&s, placeholders, &inner_cte, &tmp, transaction, &arena)
                            .await?;

                        for (column, name) in tmp_schema.rows.iter_mut().zip(columns.iter()) {
                            column.name.clone_from(name);
                        }

                        let tmp_rows: Vec<_> = tmp_rows.collect().await;
                        let previous_rows = inner_cte
                            .get(&cte.name)
                            .into_iter()
                            .flat_map(|s| s.parts.iter().flat_map(|p| p.rows.iter()));

                        if tmp_rows.len() == previous_rows.count() {
                            break;
                        }

                        inner_cte.insert(
                            cte.name.clone(),
                            storage::EntireRelation::from_parts(tmp_schema, tmp_rows),
                        );
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
    S: storage::Storage,
{
    type Prepared = NaivePrepared;
    type PrepareError = PrepareError<S::LoadingError>;
    type ExecuteBoundError = ExecuteBoundError<S::LoadingError>;

    type CopyState<'e> = NaiveCopyState<'e, S, S::TransactionGuard> where S: 'e, S::TransactionGuard: 'e;

    async fn prepare<'q, 'a>(
        &self,
        query: &sql::Query<'q, 'a>,
        _ctx: &mut Context<S::TransactionGuard>,
    ) -> Result<Self::Prepared, Self::PrepareError> {
        let (expected, columns) = match query {
            Query::Select(s) => {
                let schemas = self
                    .storage
                    .schemas()
                    .await
                    .map_err(PrepareError::LoadingSchemas)?;

                let (ra_expression, expected_types) = ra::RaExpression::parse_select(&s, &schemas)
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
    ) -> Result<Self::CopyState<'e>, Self::ExecuteBoundError>
    where
        's: 'e,
        'c: 'e,
    {
        let schemas = self
            .storage
            .schemas()
            .await
            .map_err(ExecuteBoundError::StorageError)?;

        let table = match schemas.get_table(table_name) {
            Some(t) => t.clone(),
            None => return Err(ExecuteBoundError::Other("Missing Table")),
        };

        Ok(NaiveCopyState {
            table: table_name.into(),
            engine: &self.storage,
            schema: table,
            tx: ctx.transaction.as_ref().unwrap(),
        })
    }

    #[tracing::instrument(skip(self, query, ctx, arena))]
    async fn execute_bound<'arena>(
        &self,
        query: &<Self::Prepared as PreparedStatement>::Bound,
        ctx: &mut Context<S::TransactionGuard>,
        arena: &'arena bumpalo::Bump,
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

                    Err(ExecuteBoundError::NotImplemented(
                        "Creating Prepared Statement",
                    ))
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
                            &select,
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
                            let tmp = storage::Data::realize(ty, value).map_err(
                                |(_realize, ty, _)| {
                                    Self::ExecuteBoundError::RealizingValueFromRaw {
                                        value: value.clone(),
                                        target: ty.clone(),
                                    }
                                },
                            )?;

                            Ok((*name, tmp))
                        })
                        .collect::<Result<_, Self::ExecuteBoundError>>()?;

                    tracing::trace!("Placeholder-Values: {:#?}", placeholder_values);

                    let tmp = HashMap::new();
                    let (scheme, rows) = match self
                        .evaluate_ra(
                            &ra_expression,
                            &placeholder_values,
                            &cte_queries,
                            &tmp,
                            transaction,
                            &arena,
                        )
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!("RA-Error: {:?}", e);
                            return Err(ExecuteBoundError::Executing(e));
                        }
                    };

                    let result = storage::EntireRelation::from_parts(scheme, rows.collect().await);

                    tracing::debug!("RA-Result: {:?}", result);

                    Ok(ExecuteResult::Select {
                        content: result,
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
                                                .map_err(|(_realize, ty, _)| {
                                                    ExecuteBoundError::RealizingValueFromRaw {
                                                        value: query.values[*pnumb - 1].to_vec(),
                                                        target: ty.clone(),
                                                    }
                                                })
                                            }
                                            sql::ValueExpression::Literal(lit) => {
                                                let data = storage::Data::from_literal(lit);
                                                // TODO
                                                // Check if data and expected types match

                                                Ok(data)
                                            }
                                            sql::ValueExpression::FunctionCall(func) => {
                                                match func {
                                                    sql::FunctionCall::CurrentTimestamp => {
                                                        Ok(storage::Data::Timestamp(
                                                            "2024-04-12 12:00:00.000000-05".into(),
                                                        ))
                                                    }
                                                    other => {
                                                        dbg!(other);
                                                        Err(ExecuteBoundError::NotImplemented(
                                                            "FunctionCall",
                                                        ))
                                                    }
                                                }
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
                                .execute(&Query::Select(select.to_static()), ctx, &arena)
                                .boxed_local()
                                .await
                                .map_err(|_e| ExecuteBoundError::Other("Executing Query"))?
                            {
                                ExecuteResult::Select { content, .. } => content,
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
                        .insert_rows(&ins.table.0, &mut insert_rows.into_iter(), transaction)
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Insert {
                        returning,
                        inserted_rows,
                        formats: query.result_columns.clone(),
                    })
                }
                Query::Update(update) => {
                    tracing::debug!("Update: {:#?}", update);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    let relation = self
                        .storage
                        .get_entire_relation(&update.table.0, transaction)
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let schemas = self
                        .storage
                        .schemas()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    // tracing::info!("Relation");

                    let (ra_update, ra_placeholders) = RaUpdate::parse(&update, &schemas)
                        .map_err(ExecuteBoundError::ParseRelationAlgebra)?;

                    let placeholder_values: HashMap<_, _> = ra_placeholders
                        .iter()
                        .map(|(name, ty)| {
                            let value = query.values.get(*name - 1).unwrap();
                            let tmp = storage::Data::realize(ty, value).map_err(
                                |(_realize, ty, _)| {
                                    Self::ExecuteBoundError::RealizingValueFromRaw {
                                        value: value.clone(),
                                        target: ty.clone(),
                                    }
                                },
                            )?;

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
                            let outer = HashMap::new();
                            let mapper = match condition.as_ref() {
                                Some(cond) => {
                                    let m = condition::Mapper::construct(
                                        cond,
                                        (&table_columns, &placeholder_values, &cte_queries, &outer),
                                    )
                                    .map_err(|ev| ExecuteBoundError::Executing(ev))?;
                                    Some(m)
                                }
                                None => None,
                            };

                            let mut field_mappers = Vec::with_capacity(fields.len());
                            for field in fields.iter() {
                                let mapping = value::Mapper::construct(
                                    &field.value,
                                    (&table_columns, &placeholder_values, &cte_queries, &outer),
                                )
                                .map_err(|e| ExecuteBoundError::Executing(e))?;
                                field_mappers.push(mapping);
                            }

                            let mut count = 0;

                            let mut rows_to_update = Vec::new();
                            for mut row in
                                relation.parts.into_iter().flat_map(|p| p.rows.into_iter())
                            {
                                let should_update = match mapper.as_ref() {
                                    Some(mapper) => mapper
                                        .evaluate(&row, self, transaction, arena)
                                        .await
                                        .ok_or_else(|| ExecuteBoundError::Other("Testing"))?,
                                    None => true,
                                };

                                if !should_update {
                                    continue;
                                }

                                count += 1;

                                let mut field_values = Vec::with_capacity(field_mappers.len());
                                for mapping in field_mappers.iter_mut() {
                                    let value = mapping
                                        .evaluate_mut(&row, self, transaction, arena)
                                        .await
                                        .ok_or_else(|| ExecuteBoundError::Other("Testing"))?;

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

                                rows_to_update.push((row.id(), row.data));
                            }

                            self.storage
                                .update_rows(
                                    update.table.0.as_ref(),
                                    &mut rows_to_update.into_iter(),
                                    transaction,
                                )
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

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

                    let (ra_delete, placeholders) = ra::RaDelete::parse(&delete, &schemas)
                        .map_err(ExecuteBoundError::ParseRelationAlgebra)?;

                    let relation = self
                        .storage
                        .get_entire_relation(&ra_delete.table, transaction)
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let placeholders: HashMap<_, _> = placeholders
                        .iter()
                        .map(|(name, ty)| {
                            let value = query.values.get(*name - 1).unwrap();
                            let tmp = storage::Data::realize(ty, value).map_err(
                                |(_realize, ty, _)| {
                                    Self::ExecuteBoundError::RealizingValueFromRaw {
                                        value: value.clone(),
                                        target: ty.clone(),
                                    }
                                },
                            )?;

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

                        let outer = HashMap::new();
                        let mapper = match ra_delete.condition.as_ref() {
                            Some(cond) => {
                                let m = condition::Mapper::construct(
                                    cond,
                                    (&table_columns, &placeholders, &cte_queries, &outer),
                                )
                                .map_err(|ev| ExecuteBoundError::Executing(ev))?;
                                Some(m)
                            }
                            None => None,
                        };

                        for row in relation.parts.into_iter().flat_map(|p| p.rows.into_iter()) {
                            if let Some(mapper) = mapper.as_ref() {
                                let condition_result = mapper
                                    .evaluate(&row, self, transaction, arena)
                                    .await
                                    .ok_or_else(|| ExecuteBoundError::Other("Testing"))?;

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
                        .delete_rows(&delete.table.0, &mut to_delete.into_iter(), transaction)
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
                            .relation_exists(&create.identifier.0, transaction)
                            .await
                            .map_err(ExecuteBoundError::StorageError)?
                    {
                        tracing::trace!("Table already exists");
                        return Ok(ExecuteResult::Create);
                    }

                    let fields: std::vec::Vec<_> = create
                        .fields
                        .iter()
                        .map(|tfield| {
                            (
                                tfield.ident.0.to_string(),
                                tfield.datatype.clone(),
                                tfield
                                    .modifiers
                                    .iter()
                                    .map(|m| m.clone())
                                    .collect::<Vec<_>>(),
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
                                .rename_relation(&from.0, &to.0, transaction)
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
                                type_modifiers.to_vec(),
                            );

                            self.storage
                                .modify_relation(&table.0, modifications, transaction)
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                        sql::AlterTable::AlterColumnTypes { table, columns } => {
                            tracing::info!("Alter Column Types");

                            let mut modifications = storage::ModifyRelation::new();

                            for column in columns.iter() {
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
                            let mut modifications = storage::ModifyRelation::new();

                            modifications.add_modifier(&column.0, TypeModifier::PrimaryKey);

                            self.storage
                                .modify_relation(&table.0, modifications, transaction)
                                .await
                                .map_err(ExecuteBoundError::StorageError)?;

                            Ok(ExecuteResult::Alter)
                        }
                    }
                }
                Query::DropIndex(drop_index) => {
                    tracing::debug!("Dropping Index: {:?}", drop_index);

                    let transaction = ctx.transaction.as_ref().unwrap();

                    let pg_indexes_table = self
                        .storage
                        .get_entire_relation("pg_indexes", transaction)
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
                        .delete_rows("pg_indexes", &mut cid, transaction)
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Drop_)
                }
                Query::DropTable(drop_table) => {
                    let transaction = match ctx.transaction.as_ref() {
                        Some(t) => t,
                        None => return Err(ExecuteBoundError::Other("Missing Transaction State")),
                    };

                    for name in drop_table.names.iter() {
                        self.storage
                            .remove_relation(&name.0, transaction)
                            .await
                            .map_err(ExecuteBoundError::StorageError)?;
                    }

                    Ok(ExecuteResult::Drop_)
                }
                Query::TruncateTable(trunc_table) => {
                    // TODO
                    tracing::error!(
                        ?trunc_table,
                        "[TODO] Truncating Table is not yet really supported"
                    );

                    Ok(ExecuteResult::Truncate)
                }
                Query::BeginTransaction(isolation) => {
                    tracing::debug!("Starting Transaction: {:?}", isolation);

                    let guard = self
                        .storage
                        .start_transaction()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;
                    ctx.transaction = Some(guard);

                    Ok(ExecuteResult::Begin)
                }
                Query::CommitTransaction => {
                    tracing::debug!("Committing Transaction");

                    let guard = ctx.transaction.take().unwrap();
                    self.storage
                        .commit_transaction(guard)
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Commit)
                }
                Query::RollbackTransaction => {
                    tracing::debug!("Rollback Transaction");

                    let guard = ctx.transaction.take().unwrap();
                    self.storage
                        .abort_transaction(guard)
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    Ok(ExecuteResult::Rollback)
                }
                Query::WithCTE { cte, query } => {
                    let transaction = ctx.transaction.as_ref().unwrap();

                    let schemas = self
                        .storage
                        .schemas()
                        .await
                        .map_err(ExecuteBoundError::StorageError)?;

                    let (ra_cte, _cte_placeholder_types) = ra::parse_ctes(&cte, &schemas)
                        .map_err(ExecuteBoundError::ParseRelationAlgebra)?;

                    for cte in ra_cte {
                        parse_context.add_cte(cte.clone());

                        let cte_result = self
                            .execute_cte(
                                &cte,
                                &HashMap::new(),
                                &HashMap::new(),
                                transaction,
                                &arena,
                            )
                            .await
                            .map_err(ExecuteBoundError::Executing)?;
                        cte_queries.insert(cte.name.clone(), cte_result);
                    }

                    to_process = Some(&query);
                    continue;
                }
                Query::Vacuum(v) => {
                    tracing::info!(?v, "Vacuum");

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

impl<'e, S> CopyState for NaiveCopyState<'e, S, S::TransactionGuard>
where
    S: Storage,
{
    fn columns(&self) -> Vec<()> {
        self.schema.rows.iter().map(|_| ()).collect()
    }

    async fn insert(&mut self, raw_column: &[u8]) -> Result<(), ()> {
        let raw_str = core::str::from_utf8(raw_column).map_err(|_e| ())?;

        let parts: Vec<_> = raw_str.split('\t').collect();

        if parts.len() != self.schema.rows.len() {
            // dbg!(parts, &self.schema.rows);

            return Err(());
        }

        let mut row_data = Vec::with_capacity(parts.len());
        for (column, raw) in self.schema.rows.iter().zip(parts) {
            let tmp = Data::realize(&column.ty, raw.as_bytes()).map_err(|_e| ())?;
            row_data.push(tmp);
        }

        self.engine
            .insert_rows(&self.table, &mut core::iter::once(row_data), self.tx)
            .await
            .unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use storage::{EntireRelation, PartialRelation, Row, Storage};

    use self::storage::inmemory::InMemoryStorage;

    use super::*;

    use bumpalo::Bump;

    #[tokio::test]
    async fn execute_delete_with_subquery() {
        let arena = Bump::new();
        let query = "DELETE FROM dashboard_acl WHERE dashboard_id NOT IN (SELECT id FROM dashboard) AND dashboard_id != -1";

        let storage = {
            let storage = InMemoryStorage::new();

            let trans = storage.start_transaction().await.unwrap();

            storage
                .create_relation(
                    "dashboard_acl",
                    vec![("dashboard_id".into(), DataType::Integer, Vec::new())],
                    &trans,
                )
                .await
                .unwrap();

            storage
                .create_relation(
                    "dashboard",
                    vec![("id".into(), DataType::Integer, Vec::new())],
                    &trans,
                )
                .await
                .unwrap();

            storage
                .insert_rows(
                    "dashboard",
                    &mut vec![vec![Data::Integer(1)]].into_iter(),
                    &trans,
                )
                .await
                .unwrap();

            storage
                .insert_rows(
                    "dashboard_acl",
                    &mut vec![vec![Data::Integer(132)], vec![Data::Integer(1)]].into_iter(),
                    &trans,
                )
                .await
                .unwrap();

            storage.commit_transaction(trans).await.unwrap();

            storage
        };
        let engine = NaiveEngine::new(storage);

        let query = Query::parse(query.as_bytes(), &arena).unwrap();

        let mut ctx = Context::new();
        ctx.transaction = Some(engine.storage.start_transaction().await.unwrap());
        let res = engine
            .execute(&query, &mut ctx, &bumpalo::Bump::new())
            .await
            .unwrap();

        assert_eq!(ExecuteResult::Delete { deleted_rows: 1 }, res);
    }

    #[tokio::test]
    async fn using_is_true() {
        let arena = Bump::new();
        let query_str = "SELECT name FROM user WHERE active IS TRUE";

        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();

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
                    &trans,
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

            storage.commit_transaction(trans).await.unwrap();

            storage
        };
        let engine = NaiveEngine::new(storage);

        let mut ctx = Context::new();
        ctx.transaction = Some(engine.storage.start_transaction().await.unwrap());
        let res = engine
            .execute(&query, &mut ctx, &bumpalo::Bump::new())
            .await
            .unwrap();

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
