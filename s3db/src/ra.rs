use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
};

use crate::storage::Schemas;
use sql::{
    self, ColumnReference, Combination, DataType, Identifier, Select, TableExpression,
    ValueExpression,
};

mod types;
pub use types::PossibleTypes;

mod condition;
pub use condition::{RaCondition, RaConditionValue};

mod value;
pub use value::{RaFunction, RaValueExpression};

mod delete;
pub use delete::RaDelete;

mod update;
pub use update::{RaUpdate, UpdateFields};

mod aggregate;
pub use aggregate::{AggregateExpression, AggregationCondition};

mod cte;
pub use cte::{parse_ctes, CTEQuery, CTEValue, CTE};

mod context;
pub use context::ParsingContext;

mod select;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct AttributeId(usize);

impl AttributeId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Attribute<V> {
    pub id: AttributeId,
    pub name: String,
    pub value: V,
}

pub type ProjectionAttribute = Attribute<RaValueExpression>;

#[derive(Debug, PartialEq, Clone)]
pub enum RaExpression {
    Renamed {
        name: String,
        inner: Box<Self>,
    },
    Projection {
        inner: Box<Self>,
        attributes: Vec<ProjectionAttribute>,
    },
    Selection {
        inner: Box<Self>,
        filter: RaCondition,
    },
    BaseRelation {
        name: Identifier<'static>,
        columns: Vec<(String, DataType, AttributeId)>,
    },
    Join {
        left: Box<Self>,
        right: Box<Self>,
        kind: sql::JoinKind,
        condition: RaCondition,
    },
    LateralJoin {
        left: Box<Self>,
        right: Box<Self>,
        kind: sql::JoinKind,
        condition: RaCondition,
    },
    EmptyRelation,
    /// This is very similar to the `Self::Projection` variant, but instead of working on
    /// individual rows, works on aggregation operations
    Aggregation {
        inner: Box<Self>,
        attributes: Vec<Attribute<AggregateExpression>>,
        aggregation_condition: AggregationCondition,
    },
    Limit {
        inner: Box<Self>,
        limit: usize,
        offset: usize,
    },
    OrderBy {
        inner: Box<Self>,
        attributes: Vec<(AttributeId, sql::OrderBy)>,
    },
    CTE {
        name: String,
        columns: Vec<(String, DataType, AttributeId)>,
    },
    Chain {
        parts: Vec<RaExpression>,
    },
}

macro_rules! error_context {
    ($context:literal) => {
        ::std::borrow::Cow::Owned(format!("[{}:{}] {}", file!(), line!(), $context))
    };
}
pub(crate) use error_context;

#[derive(Debug, PartialEq, Clone)]
pub enum Function {
    Coalesce(Vec<RaValueExpression>),
    LeftPad {
        base: RaValueExpression,
        padding: String,
        length: i64,
    },
    SetValue {
        sequence: String,
        value: Box<RaValueExpression>,
        is_called: bool,
    },
    Lower(Box<RaValueExpression>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum RaComparisonOperator {
    Equals,
    NotEquals,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    In,
    NotIn,
    Like,
    ILike,
    Is,
    IsNot,
}

#[derive(Debug, PartialEq)]
pub enum ParseRaError {
    UnknownRelation(String),
    UnknownAttribute {
        attr: ColumnReference<'static>,
        available: Vec<(String, String)>,
        context: Cow<'static, str>,
    },
    NotImplemented(&'static str),
    DeterminePossibleTypes {
        expr: RaValueExpression,
    },
    IncompatibleTypes {
        mismatching_types: Vec<types::PossibleTypes>,
    },
    AggregateExpressionOutsideOfAggregate {
        a_id: AttributeId,
        name: Option<String>,
        aggregated_fields: Vec<(String, AttributeId)>,
    },
    Other(&'static str),
}

pub type ParseSelectError = ParseRaError;

enum CustomCow<'s, T> {
    Owned(T),
    Borrowed(&'s T),
}

impl<'s, T> Borrow<T> for CustomCow<'s, T> {
    fn borrow(&self) -> &T {
        match self {
            Self::Owned(v) => v,
            Self::Borrowed(v) => v,
        }
    }
}
impl<'s, T> AsRef<T> for CustomCow<'s, T> {
    fn as_ref(&self) -> &T {
        match self {
            Self::Owned(v) => v,
            Self::Borrowed(v) => v,
        }
    }
}

struct Scope<'s> {
    current_attributes: HashMap<String, DataType>,
    schemas: &'s Schemas,
    context: Option<CustomCow<'s, ParsingContext>>,
    attribute_index: usize,
    named_tables: HashMap<String, Vec<(String, DataType)>>,
    renamings: HashMap<String, String>,
}

impl<'s> Scope<'s> {
    pub fn new<'r>(schemas: &'r Schemas) -> Self
    where
        'r: 's,
    {
        Self {
            current_attributes: HashMap::new(),
            schemas,
            context: None,
            attribute_index: 0,
            named_tables: HashMap::new(),
            renamings: HashMap::new(),
        }
    }

    pub fn attribute_id(&mut self) -> AttributeId {
        let value = self.attribute_index;
        self.attribute_index += 1;
        AttributeId(value)
    }
}

impl ProjectionAttribute {
    fn parse_internal(
        scope: &mut Scope<'_>,
        expr: &ValueExpression<'_, '_>,
        placeholders: &mut HashMap<usize, DataType>,
        table_expression: &mut RaExpression,
        outer: &mut Vec<RaExpression>,
    ) -> Result<Vec<ProjectionAttribute>, ParseSelectError> {
        if matches!(expr, ValueExpression::All) {
            let columns = table_expression.get_columns();

            return Ok(columns
                .into_iter()
                .map(|(_, name, ty, id)| ProjectionAttribute {
                    id: scope.attribute_id(),
                    name: name.clone(),
                    value: RaValueExpression::Attribute {
                        name: name.clone(),
                        ty: ty.clone(),
                        a_id: id,
                    },
                })
                .collect());
        }

        if let ValueExpression::AllFromRelation { relation } = &expr {
            let mut columns = table_expression.get_columns();

            let src_name = match scope.renamings.get(relation.0.as_ref()) {
                Some(n) => n,
                None => relation.0.as_ref(),
            };

            columns.retain(|(_, _, _, id)| match table_expression.get_source(*id) {
                Some(expr) => match expr {
                    RaExpression::BaseRelation { name, .. } => name.0.as_ref() == src_name,
                    _ => false,
                },
                None => false,
            });

            dbg!(&columns);

            return Ok(columns
                .into_iter()
                .map(|(_, name, ty, id)| ProjectionAttribute {
                    id: scope.attribute_id(),
                    name: name.clone(),
                    value: RaValueExpression::Attribute {
                        name: name.clone(),
                        ty: ty.clone(),
                        a_id: id,
                    },
                })
                .collect());
        }

        let raw_value =
            RaValueExpression::parse_internal(scope, expr, placeholders, table_expression, outer)?;

        let (name, value): (String, RaValueExpression) = match raw_value {
            RaValueExpression::Attribute { name, ty, a_id } => (
                name.clone(),
                RaValueExpression::Attribute { name, ty, a_id },
            ),
            RaValueExpression::Renamed { name, value } => (name, *value),
            other => ("".into(), other),
        };

        Ok(vec![ProjectionAttribute {
            id: scope.attribute_id(),
            name,
            value,
        }])
    }
}

impl RaExpression {
    pub fn get_source(&self, attribute: AttributeId) -> Option<&RaExpression> {
        match self {
            Self::Renamed { inner, .. } => inner.get_source(attribute),
            Self::Projection { inner, .. } => {
                if let Some(src) = inner.get_source(attribute) {
                    return Some(src);
                }

                None
            }
            Self::Selection { inner, .. } => inner.get_source(attribute),
            Self::BaseRelation { columns, .. } => columns
                .iter()
                .find(|(_, _, cid)| cid == &attribute)
                .map(|_| self),
            Self::Join { left, right, .. } => {
                let left_tmp = left.get_source(attribute);
                if left_tmp.is_some() {
                    left_tmp
                } else {
                    right.get_source(attribute)
                }
            }
            Self::LateralJoin { left, right, .. } => {
                let left_tmp = left.get_source(attribute);
                if left_tmp.is_some() {
                    left_tmp
                } else {
                    right.get_source(attribute)
                }
            }
            Self::EmptyRelation => None,
            Self::Aggregation { inner, .. } => inner.get_source(attribute), // TODO
            Self::Limit { inner, .. } => inner.get_source(attribute),
            Self::OrderBy { inner, .. } => inner.get_source(attribute),
            Self::CTE {
                name: _name,
                columns: _columns,
            } => None, // TODO
            Self::Chain { parts } => parts.iter().find_map(|p| p.get_source(attribute)),
        }
    }

    pub fn get_column(&self, name: &str) -> Option<DataType> {
        match self {
            Self::Renamed { name, inner } => inner.get_column(name),
            Self::Projection { attributes, .. } => attributes
                .iter()
                .find(|attr| attr.name == name)
                .and_then(|attr| attr.value.datatype()),
            Self::Selection { inner, .. } => inner.get_column(name),
            Self::BaseRelation {
                name: _name,
                columns,
            } => columns
                .iter()
                .find(|(n, _, _)| n == name)
                .map(|(_, d, _)| d.clone()),
            Self::Join { left, right, .. } => {
                if let Some(lc) = left.get_column(name) {
                    return Some(lc);
                }
                if let Some(rc) = right.get_column(name) {
                    return Some(rc);
                }
                None
            }
            Self::LateralJoin { left, right, .. } => {
                if let Some(lc) = left.get_column(name) {
                    return Some(lc);
                }
                if let Some(rc) = right.get_column(name) {
                    return Some(rc);
                }
                None
            }
            Self::EmptyRelation => None,
            Self::Aggregation { attributes, .. } => attributes
                .iter()
                .find(|attr| attr.name == name)
                .map(|attr| attr.value.return_ty()),
            Self::Limit { inner, .. } => inner.get_column(name),
            Self::OrderBy { inner, .. } => inner.get_column(name),
            Self::CTE { columns, .. } => columns
                .iter()
                .find(|(n, _, _)| n == name)
                .map(|(_, d, _)| d.clone()),
            Self::Chain { parts } => parts.iter().find_map(|p| p.get_column(name)),
        }
    }

    pub fn get_columns(&self) -> Vec<(String, String, DataType, AttributeId)> {
        match self {
            Self::Renamed { name, inner } => inner
                .get_columns()
                .into_iter()
                .map(|(_, c, t, i)| (name.clone(), c, t, i))
                .collect::<Vec<_>>(),
            Self::Projection { attributes, .. } => attributes
                .iter()
                .map(|attr| {
                    let dt = attr.value.datatype().unwrap();
                    ("".into(), attr.name.clone(), dt, attr.id)
                })
                .collect(),
            Self::Selection { inner, .. } => inner.get_columns(),
            Self::BaseRelation { columns, name } => columns
                .iter()
                .map(|(n, t, id)| (name.0.to_string(), n.clone(), t.clone(), *id))
                .collect(),
            Self::Join { left, right, .. } => {
                let mut result = Vec::new();

                result.append(&mut left.get_columns());
                result.append(&mut right.get_columns());

                result
            }
            Self::LateralJoin { left, right, .. } => {
                let mut result = Vec::new();

                result.append(&mut left.get_columns());
                result.append(&mut right.get_columns());

                result
            }
            Self::EmptyRelation => Vec::new(),
            Self::Aggregation { attributes, .. } => attributes
                .iter()
                .map(|attr| {
                    (
                        "".into(),
                        attr.name.clone(),
                        attr.value.return_ty(),
                        attr.id,
                    )
                })
                .collect(),
            Self::Limit { inner, .. } => inner.get_columns(),
            Self::OrderBy { inner, .. } => inner.get_columns(),
            Self::CTE { columns, .. } => columns
                .iter()
                .map(|(n, t, id)| ("".into(), n.clone(), t.clone(), *id))
                .collect(),
            Self::Chain { parts } => parts[0].get_columns(),
        }
    }

    pub fn parse_select(
        query: &Select<'_, '_>,
        schemas: &Schemas,
    ) -> Result<(Self, HashMap<usize, DataType>), ParseSelectError> {
        Self::parse_select_with_context(query, schemas, &ParsingContext::new())
    }

    pub fn parse_select_with_context(
        query: &Select<'_, '_>,
        schemas: &Schemas,
        parse_context: &ParsingContext,
    ) -> Result<(Self, HashMap<usize, DataType>), ParseSelectError> {
        let mut scope = Scope::new(schemas);

        let mut placeholders = HashMap::new();

        scope.context = Some(CustomCow::Borrowed(parse_context));

        let s = Self::parse_s(query, &mut scope, &mut placeholders, &mut Vec::new())?;

        Ok((s, placeholders))
    }

    fn parse_table_expression(
        table: &TableExpression,
        scope: &mut Scope<'_>,
        placeholders: &mut HashMap<usize, DataType>,
        left_side: Option<&RaExpression>,
        outer: &mut Vec<RaExpression>,
    ) -> Result<Self, ParseSelectError> {
        match table {
            TableExpression::Relation(relation) => {
                let columns = scope.schemas.get_table(relation.0.as_ref()).map(|columns| {
                    columns
                        .rows
                        .iter()
                        .map(|c| (c.name.clone(), c.ty.clone(), scope.attribute_id()))
                        .collect()
                });

                if let Some(columns) = columns {
                    return Ok(Self::BaseRelation {
                        name: relation.to_static(),
                        columns,
                    });
                }

                let context = match scope.context.as_ref() {
                    Some(c) => c,
                    None => return Err(ParseSelectError::UnknownRelation(relation.0.to_string())),
                };

                let tmp = context.as_ref().ctes.iter().find_map(|cte| {
                    if cte.name != relation.0.as_ref() {
                        return None;
                    }

                    match &cte.value {
                        CTEValue::Standard { query } => match query {
                            CTEQuery::Select(s) => Some(
                                s.get_columns()
                                    .into_iter()
                                    .map(|(_, n, t, i)| (n, t, i))
                                    .collect::<Vec<_>>(),
                            ),
                        },
                        CTEValue::Recursive { query, columns } => match query {
                            CTEQuery::Select(s) => {
                                let mut s_columns = s
                                    .get_columns()
                                    .into_iter()
                                    .map(|(_, n, t, i)| (n, t, i))
                                    .collect::<Vec<_>>();
                                for (c, name) in s_columns.iter_mut().zip(columns.iter()) {
                                    c.0.clone_from(name);
                                }

                                Some(s_columns)
                            }
                        },
                    }
                });

                if let Some(columns) = tmp {
                    return Ok(Self::CTE {
                        name: relation.0.to_string(),
                        columns,
                    });
                }

                Err(ParseSelectError::UnknownRelation(relation.0.to_string()))
            }
            TableExpression::Renamed {
                inner,
                name,
                column_rename,
            } => {
                let inner_result =
                    Self::parse_table_expression(inner, scope, placeholders, left_side, outer)?;

                match inner.as_ref() {
                    TableExpression::Relation(inner_name) => {
                        scope
                            .renamings
                            .insert(name.0.to_string(), inner_name.0.to_string());
                    }
                    _ => {}
                };

                scope.named_tables.insert(
                    name.0.to_string(),
                    inner_result
                        .get_columns()
                        .into_iter()
                        .map(|(_, n, t, _)| (n, t))
                        .collect(),
                );

                let inner = match column_rename {
                    Some(columns) => {
                        let inner_columns = inner_result.get_columns();

                        if inner_columns.len() != columns.len() {
                            dbg!(&inner_columns, &columns);
                            return Err(ParseSelectError::Other(
                                "Inner Result Colums and renamed columns are not matching in count",
                            ));
                        }

                        let columns: Vec<_> = inner_columns
                            .into_iter()
                            .zip(columns.iter())
                            .map(|((_, n2, ty, id), renamed)| ProjectionAttribute {
                                id: scope.attribute_id(),
                                name: renamed.0.to_string(),
                                value: RaValueExpression::Attribute {
                                    name: n2.clone(),
                                    ty,
                                    a_id: id,
                                },
                            })
                            .collect();

                        RaExpression::Projection {
                            inner: Box::new(inner_result),
                            attributes: columns,
                        }
                    }
                    None => inner_result,
                };

                Ok(RaExpression::Renamed {
                    name: name.0.to_string(),
                    inner: Box::new(inner),
                })
            }
            TableExpression::Join {
                left,
                right,
                kind,
                condition,
                lateral,
            } => {
                // TODO
                // If we have a lateral join, we should probably not model this as an actualy join
                // but rather turn this into a subquery

                let left_ra =
                    Self::parse_table_expression(left, scope, placeholders, left_side, outer)?;
                let right_ra = Self::parse_table_expression(
                    right,
                    scope,
                    placeholders,
                    lateral.then_some(&left_ra),
                    outer,
                )?;

                let mut joined = if *lateral {
                    Self::LateralJoin {
                        left: Box::new(left_ra),
                        right: Box::new(right_ra),
                        kind: kind.clone(),
                        condition: RaCondition::And(vec![]),
                    }
                } else {
                    Self::Join {
                        left: Box::new(left_ra),
                        right: Box::new(right_ra),
                        kind: kind.clone(),
                        condition: RaCondition::And(vec![]),
                    }
                };

                let ra_condition = RaCondition::parse_internal(
                    scope,
                    condition,
                    placeholders,
                    &mut joined,
                    &mut Vec::new(),
                )?;

                if let Self::Join { condition, .. } | Self::LateralJoin { condition, .. } =
                    &mut joined
                {
                    *condition = ra_condition;
                }

                Ok(joined)
            }
            TableExpression::SubQuery(query) => {
                let (outer, added) = match left_side {
                    Some(tmp) => {
                        outer.push(tmp.clone());
                        (outer, true)
                    }
                    None => (outer, false),
                };

                let query = Self::parse_s(query, scope, placeholders, outer)?;

                if added {
                    outer.pop();
                }

                Ok(query)
            }
        }
    }

    fn parse_s(
        query: &Select<'_, '_>,
        scope: &mut Scope<'_>,
        placeholders: &mut HashMap<usize, DataType>,
        outer: &mut Vec<RaExpression>,
    ) -> Result<Self, ParseSelectError> {
        let base_ra = match query.table.as_ref() {
            Some(table_expr) => {
                Self::parse_table_expression(table_expr, scope, placeholders, None, outer)?
            }
            None => Self::EmptyRelation,
        };

        scope
            .current_attributes
            .extend(base_ra.get_columns().into_iter().map(|(_, n, d, _)| (n, d)));

        let where_ra =
            select::parse_condition(base_ra, &query.where_condition, scope, placeholders, outer)?;

        // FIXME
        // This is currently the wrong way around, however this should yield the same result at
        // least at the moment
        let ordered_ra = select::parse_order(where_ra, query.order_by.as_deref())?;
        let selected_ra = select::parse_aggregate(ordered_ra, &query, scope, placeholders, outer)?;

        let limited_ra = match &query.limit {
            Some(limit) => Self::Limit {
                inner: Box::new(selected_ra),
                limit: limit.limit,
                offset: limit.offset.unwrap_or(0),
            },
            None => selected_ra,
        };

        match query.combine.as_ref() {
            Some((c, s)) => {
                let other = Self::parse_s(s, scope, placeholders, outer)?;

                match c {
                    Combination::Union => Ok(Self::Chain {
                        parts: vec![limited_ra, other],
                    }),
                    Combination::Intersection => Err(ParseSelectError::NotImplemented(
                        "Combine using Intersection",
                    )),
                    Combination::Except => {
                        Err(ParseSelectError::NotImplemented("Combine using Except"))
                    }
                }
            }
            None => Ok(limited_ra),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bumpalo::Bump;
    use sql::{Literal, Query};

    use pretty_assertions::assert_eq;

    #[test]
    fn basic_select() {
        let arena = Bump::new();
        let parsed = match Query::parse("SELECT user FROM testing".as_bytes(), &arena).unwrap() {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "testing".to_string(),
            vec![
                ("user".to_string(), DataType::Name),
                ("locked".to_string(), DataType::Bool),
            ],
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();

        let (expression, placeholders) = RaExpression::parse_select(&parsed, &schemas).unwrap();
        dbg!(&expression);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("testing".into()),
                    columns: vec![
                        ("user".to_string(), DataType::Name, AttributeId(0)),
                        ("locked".to_string(), DataType::Bool, AttributeId(1)),
                    ]
                }),
                attributes: vec![ProjectionAttribute {
                    value: RaValueExpression::Attribute {
                        name: "user".to_string(),
                        ty: DataType::Name,
                        a_id: AttributeId(0),
                    },
                    id: AttributeId(2),
                    name: "user".to_string(),
                }]
            },
            expression
        );

        assert_eq!(HashMap::new(), placeholders);
    }

    #[test]
    fn basic_select_where() {
        let arena = Bump::new();
        let parsed = match Query::parse(
            "SELECT user FROM testing WHERE user = 'bot'".as_bytes(),
            &arena,
        )
        .unwrap()
        {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "testing".to_string(),
            vec![
                ("user".to_string(), DataType::Name),
                ("locked".to_string(), DataType::Bool),
            ],
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();

        let (expression, placeholders) = RaExpression::parse_select(&parsed, &schemas).unwrap();
        dbg!(&expression);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::Selection {
                    inner: Box::new(RaExpression::BaseRelation {
                        name: Identifier("testing".into()),
                        columns: vec![
                            ("user".to_string(), DataType::Name, AttributeId(0)),
                            ("locked".to_string(), DataType::Bool, AttributeId(1)),
                        ]
                    }),
                    filter: RaCondition::And(vec![RaCondition::Value(Box::new(
                        RaConditionValue::Comparison {
                            first: RaValueExpression::Attribute {
                                ty: DataType::Name,
                                name: "user".into(),
                                a_id: AttributeId::new(0),
                            },
                            second: RaValueExpression::Cast {
                                inner: Box::new(RaValueExpression::Literal(Literal::Str(
                                    "bot".into()
                                ))),
                                target: DataType::Name
                            },
                            comparison: RaComparisonOperator::Equals,
                        }
                    ))]),
                }),
                attributes: vec![ProjectionAttribute {
                    value: RaValueExpression::Attribute {
                        name: "user".to_string(),
                        ty: DataType::Name,
                        a_id: AttributeId(0),
                    },
                    id: AttributeId(2),
                    name: "user".to_string(),
                }]
            },
            expression
        );

        assert_eq!(HashMap::new(), placeholders);
    }

    #[test]
    fn basic_select_where_with_placeholders() {
        let arena = Bump::new();
        let parsed = match Query::parse(
            "SELECT user FROM testing WHERE user = $1".as_bytes(),
            &arena,
        )
        .unwrap()
        {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "testing".to_string(),
            vec![
                ("user".to_string(), DataType::Name),
                ("locked".to_string(), DataType::Bool),
            ],
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();

        let (expression, placeholders) = RaExpression::parse_select(&parsed, &schemas).unwrap();
        dbg!(&expression);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::Selection {
                    inner: Box::new(RaExpression::BaseRelation {
                        name: Identifier("testing".into()),
                        columns: vec![
                            ("user".to_string(), DataType::Name, AttributeId(0)),
                            ("locked".to_string(), DataType::Bool, AttributeId(1)),
                        ]
                    }),
                    filter: RaCondition::And(vec![RaCondition::Value(Box::new(
                        RaConditionValue::Comparison {
                            first: RaValueExpression::Attribute {
                                ty: DataType::Name,
                                name: "user".into(),
                                a_id: AttributeId::new(0),
                            },
                            second: RaValueExpression::Placeholder(1),
                            comparison: RaComparisonOperator::Equals,
                        }
                    ))])
                }),
                attributes: vec![ProjectionAttribute {
                    value: RaValueExpression::Attribute {
                        name: "user".to_string(),
                        ty: DataType::Name,
                        a_id: AttributeId(0)
                    },
                    id: AttributeId(2),
                    name: "user".to_string(),
                }]
            },
            expression
        );

        assert_eq!(
            [(1, DataType::Name)].into_iter().collect::<HashMap<_, _>>(),
            placeholders
        );
    }

    #[test]
    fn select_with_coalesce() {
        let arena = Bump::new();
        let query = "SELECT\n\tdashboard.id,\n\tdashboard.version,\n\tdashboard.version,\n\tdashboard.version,\n\tdashboard.updated,\n\tCOALESCE(dashboard.updated_by, -1),\n\t'',\n\tdashboard.data\nFROM dashboard;";

        let parsed = match Query::parse(query.as_bytes(), &arena).unwrap() {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "dashboard".to_string(),
            vec![
                ("id".to_string(), DataType::Serial),
                ("version".to_string(), DataType::Integer),
                ("updated".to_string(), DataType::Timestamp),
                ("updated_by".to_string(), DataType::Integer),
                ("data".to_string(), DataType::Text),
            ],
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();

        let (ra_expression, param_values) = RaExpression::parse_select(&parsed, &schemas).unwrap();

        assert_eq!(0, param_values.len(), "{:?}", param_values);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("dashboard".into()),
                    columns: vec![
                        ("id".to_string(), DataType::Serial, AttributeId(0)),
                        ("version".to_string(), DataType::Integer, AttributeId(1)),
                        ("updated".to_string(), DataType::Timestamp, AttributeId(2)),
                        ("updated_by".to_string(), DataType::Integer, AttributeId(3)),
                        ("data".to_string(), DataType::Text, AttributeId(4)),
                    ]
                }),
                attributes: vec![
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "id".into(),
                            ty: DataType::Serial,
                            a_id: AttributeId(0),
                        },
                        id: AttributeId(5),
                        name: "id".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "version".into(),
                            ty: DataType::Integer,
                            a_id: AttributeId(1),
                        },
                        id: AttributeId(6),
                        name: "version".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "version".into(),
                            ty: DataType::Integer,
                            a_id: AttributeId(1),
                        },
                        id: AttributeId(7),
                        name: "version".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "version".into(),
                            ty: DataType::Integer,
                            a_id: AttributeId(1),
                        },
                        id: AttributeId(8),
                        name: "version".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "updated".into(),
                            ty: DataType::Timestamp,
                            a_id: AttributeId(2),
                        },
                        id: AttributeId(9),
                        name: "updated".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Function(RaFunction::Coalesce(vec![
                            RaValueExpression::Attribute {
                                ty: DataType::Integer,
                                name: "updated_by".into(),
                                a_id: AttributeId::new(3),
                            },
                            RaValueExpression::Cast {
                                inner: Box::new(RaValueExpression::Literal(Literal::SmallInteger(
                                    -1
                                ))),
                                target: DataType::Integer
                            }
                        ])),
                        id: AttributeId(10),
                        name: "".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Literal(Literal::Str("".into())),
                        id: AttributeId(11),
                        name: "".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "data".into(),
                            ty: DataType::Text,
                            a_id: AttributeId(4),
                        },
                        id: AttributeId(12),
                        name: "data".to_string(),
                    },
                ]
            },
            ra_expression,
        );
    }

    #[test]
    fn select_list_parameters() {
        let arena = Bump::new();
        let query_str = "SELECT \"id\", \"role_id\", \"action\", \"scope\", \"kind\", \"attribute\", \"identifier\", \"updated\", \"created\" FROM \"permission\" WHERE \"action\" IN ($1,$2,$3,$4)";

        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();
        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "permission".to_string(),
            vec![
                ("id".to_string(), DataType::Integer),
                ("role_id".to_string(), DataType::Integer),
                ("action".to_string(), DataType::Text),
                ("scope".to_string(), DataType::Text),
                ("kind".to_string(), DataType::Text),
                ("attribute".to_string(), DataType::Text),
                ("identifier".to_string(), DataType::Integer),
                ("updated".to_string(), DataType::Timestamp),
                ("created".to_string(), DataType::Timestamp),
            ],
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();

        let (ra_expr, param_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();

        assert_eq!(
            [
                (1, DataType::Text),
                (2, DataType::Text),
                (3, DataType::Text),
                (4, DataType::Text)
            ]
            .into_iter()
            .collect::<HashMap<_, _>>(),
            param_types
        );

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::Selection {
                    inner: Box::new(RaExpression::BaseRelation {
                        name: Identifier("permission".into()),
                        columns: vec![
                            ("id".to_string(), DataType::Integer, AttributeId(0)),
                            ("role_id".to_string(), DataType::Integer, AttributeId(1)),
                            ("action".to_string(), DataType::Text, AttributeId(2)),
                            ("scope".to_string(), DataType::Text, AttributeId(3)),
                            ("kind".to_string(), DataType::Text, AttributeId(4)),
                            ("attribute".to_string(), DataType::Text, AttributeId(5)),
                            ("identifier".to_string(), DataType::Integer, AttributeId(6)),
                            ("updated".to_string(), DataType::Timestamp, AttributeId(7)),
                            ("created".to_string(), DataType::Timestamp, AttributeId(8)),
                        ]
                    }),
                    filter: RaCondition::And(vec![RaCondition::Value(Box::new(
                        RaConditionValue::Comparison {
                            first: RaValueExpression::Attribute {
                                ty: DataType::Text,
                                name: "action".into(),
                                a_id: AttributeId::new(2),
                            },
                            second: RaValueExpression::List(vec![
                                RaValueExpression::Placeholder(1),
                                RaValueExpression::Placeholder(2),
                                RaValueExpression::Placeholder(3),
                                RaValueExpression::Placeholder(4)
                            ]),
                            comparison: RaComparisonOperator::In
                        }
                    ))]),
                }),
                attributes: vec![
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "id".to_string(),
                            ty: DataType::Integer,
                            a_id: AttributeId(0),
                        },
                        id: AttributeId(9),
                        name: "id".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "role_id".to_string(),
                            ty: DataType::Integer,
                            a_id: AttributeId(1),
                        },
                        id: AttributeId(10),
                        name: "role_id".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "action".to_string(),
                            ty: DataType::Text,
                            a_id: AttributeId(2),
                        },
                        id: AttributeId(11),
                        name: "action".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "scope".to_string(),
                            ty: DataType::Text,
                            a_id: AttributeId(3),
                        },
                        id: AttributeId(12),
                        name: "scope".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "kind".to_string(),
                            ty: DataType::Text,
                            a_id: AttributeId(4),
                        },
                        id: AttributeId(13),
                        name: "kind".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "attribute".to_string(),
                            ty: DataType::Text,
                            a_id: AttributeId(5),
                        },
                        id: AttributeId(14),
                        name: "attribute".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "identifier".to_string(),
                            ty: DataType::Integer,
                            a_id: AttributeId(6),
                        },
                        id: AttributeId(15),
                        name: "identifier".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "updated".to_string(),
                            ty: DataType::Timestamp,
                            a_id: AttributeId(7),
                        },
                        id: AttributeId(16),
                        name: "updated".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "created".to_string(),
                            ty: DataType::Timestamp,
                            a_id: AttributeId(8),
                        },
                        id: AttributeId(17),
                        name: "created".to_string(),
                    }
                ]
            },
            ra_expr
        );
    }

    #[test]
    fn select_with_join() {
        let arena = Bump::new();
        let query_str =
            "SELECT user.name, password.hash FROM user JOIN password ON user.id = password.uid";

        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();
        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [
            (
                "user".to_string(),
                vec![
                    ("name".to_string(), DataType::Text),
                    ("id".to_string(), DataType::Integer),
                ],
            ),
            (
                "password".to_string(),
                vec![
                    ("hash".to_string(), DataType::Text),
                    ("uid".to_string(), DataType::Integer),
                ],
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();

        let (ra_expr, param_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();

        assert_eq!(HashMap::new(), param_types);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::Join {
                    left: Box::new(RaExpression::BaseRelation {
                        name: Identifier("user".into()),
                        columns: vec![
                            ("name".to_string(), DataType::Text, AttributeId(0)),
                            ("id".to_string(), DataType::Integer, AttributeId(1)),
                        ]
                    }),
                    right: Box::new(RaExpression::BaseRelation {
                        name: Identifier("password".into()),
                        columns: vec![
                            ("hash".to_string(), DataType::Text, AttributeId(2)),
                            ("uid".to_string(), DataType::Integer, AttributeId(3)),
                        ]
                    }),
                    kind: sql::JoinKind::Inner,
                    condition: RaCondition::And(vec![RaCondition::Value(Box::new(
                        RaConditionValue::Comparison {
                            first: RaValueExpression::Attribute {
                                ty: DataType::Integer,
                                name: "id".into(),
                                a_id: AttributeId::new(1),
                            },
                            second: RaValueExpression::Attribute {
                                ty: DataType::Integer,
                                name: "uid".into(),
                                a_id: AttributeId::new(3),
                            },
                            comparison: RaComparisonOperator::Equals
                        }
                    ))]),
                }),
                attributes: vec![
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "name".to_string(),
                            ty: DataType::Text,
                            a_id: AttributeId(0)
                        },
                        id: AttributeId(4),
                        name: "name".to_string(),
                    },
                    ProjectionAttribute {
                        value: RaValueExpression::Attribute {
                            name: "hash".to_string(),
                            ty: DataType::Text,
                            a_id: AttributeId(2)
                        },
                        id: AttributeId(5),
                        name: "hash".to_string(),
                    }
                ]
            },
            ra_expr
        );
    }

    #[test]
    fn select_with_renamed_table() {
        let arena = Bump::new();
        let query_str = "SELECT name FROM users AS u";
        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();

        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "users".to_string(),
            vec![("name".to_string(), DataType::Text)],
        )]
        .into_iter()
        .collect::<HashMap<_, _>>()
        .into();

        let (ra_expr, param_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();

        assert_eq!(HashMap::new(), param_types);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::Renamed {
                    name: "u".into(),
                    inner: Box::new(RaExpression::BaseRelation {
                        name: Identifier("users".into()),
                        columns: vec![("name".to_string(), DataType::Text, AttributeId::new(0))]
                    })
                }),
                attributes: vec![Attribute {
                    id: AttributeId::new(1),
                    name: "name".into(),
                    value: RaValueExpression::Attribute {
                        name: "name".into(),
                        ty: DataType::Text,
                        a_id: AttributeId::new(0)
                    }
                }]
            },
            ra_expr
        );
    }

    #[test]
    fn select_with_renamed_attribute() {
        let arena = Bump::new();
        let query_str = "SELECT name AS user_name FROM users";
        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();

        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "users".to_string(),
            vec![("name".to_string(), DataType::Text)],
        )]
        .into_iter()
        .collect();

        let (ra_expr, param_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();

        assert_eq!(HashMap::new(), param_types);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("users".into()),
                    columns: vec![("name".to_string(), DataType::Text, AttributeId(0))]
                }),
                attributes: vec![ProjectionAttribute {
                    value: RaValueExpression::Attribute {
                        name: "name".to_string(),
                        ty: DataType::Text,
                        a_id: AttributeId(0)
                    },
                    id: AttributeId(1),
                    name: "user_name".to_string(),
                }]
            },
            ra_expr
        );
    }

    #[test]
    fn select_fully_qualified_with_renamed_table() {
        let arena = Bump::new();
        let query_str = "SELECT u.name FROM users AS u WHERE u.name = 'something'";
        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();

        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "users".to_string(),
            vec![("name".to_string(), DataType::Text)],
        )]
        .into_iter()
        .collect();

        dbg!(&select_query);

        let (ra_expr, param_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();

        assert_eq!(HashMap::new(), param_types);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::Selection {
                    inner: Box::new(RaExpression::Renamed {
                        name: "u".into(),
                        inner: Box::new(RaExpression::BaseRelation {
                            name: Identifier("users".into()),
                            columns: vec![("name".to_string(), DataType::Text, AttributeId(0))]
                        })
                    }),
                    filter: RaCondition::And(vec![RaCondition::Value(Box::new(
                        RaConditionValue::Comparison {
                            first: RaValueExpression::Attribute {
                                ty: DataType::Text,
                                name: "name".into(),
                                a_id: AttributeId::new(0),
                            },
                            second: RaValueExpression::Literal(Literal::Str("something".into())),
                            comparison: RaComparisonOperator::Equals
                        }
                    ))]),
                }),
                attributes: vec![ProjectionAttribute {
                    value: RaValueExpression::Attribute {
                        name: "name".to_string(),
                        ty: DataType::Text,
                        a_id: AttributeId(0)
                    },
                    id: AttributeId(1),
                    name: "name".to_string(),
                }]
            },
            ra_expr
        );
    }

    #[test]
    fn select_max_aggregate() {
        let arena = Bump::new();
        let query_str = "SELECT max(users.id) FROM users";
        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();

        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "users".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("id".to_string(), DataType::Integer),
            ],
        )]
        .into_iter()
        .collect();

        let (ra_expr, param_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();

        assert_eq!(HashMap::new(), param_types);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::Aggregation {
                    inner: Box::new(RaExpression::BaseRelation {
                        name: Identifier("users".into()),
                        columns: vec![
                            ("name".to_string(), DataType::Text, AttributeId::new(0)),
                            ("id".to_string(), DataType::Integer, AttributeId::new(1))
                        ]
                    }),
                    attributes: vec![Attribute {
                        id: AttributeId::new(2),
                        name: "".to_string(),
                        value: AggregateExpression::Max {
                            inner: Box::new(RaValueExpression::Attribute {
                                ty: DataType::Integer,
                                name: "id".into(),
                                a_id: AttributeId::new(1),
                            }),
                            dtype: DataType::Integer
                        }
                    }],
                    aggregation_condition: AggregationCondition::Everything
                }),
                attributes: vec![ProjectionAttribute {
                    id: AttributeId::new(3),
                    name: "".into(),
                    value: RaValueExpression::Attribute {
                        name: "".into(),
                        ty: DataType::Integer,
                        a_id: AttributeId::new(2)
                    },
                }]
            },
            ra_expr
        );
    }
}

#[cfg(test)]
mod error_tests {
    use self::sql::Query;

    use super::*;
    use bumpalo::Bump;

    use pretty_assertions::assert_eq;

    #[test]
    fn select_unknown_attribute() {
        let arena = Bump::new();
        let query_str = "SELECT something FROM users";
        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();

        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "users".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("id".to_string(), DataType::Integer),
            ],
        )]
        .into_iter()
        .collect();

        let _err = RaExpression::parse_select(&select_query, &schemas).unwrap_err();

        // TODO
    }

    #[test]
    fn aggregate_with_not_aggregated_column_over_everything() {
        let arena = Bump::new();
        let query_str = "SELECT count(*), name FROM users";
        let query = Query::parse(query_str.as_bytes(), &arena).unwrap();

        let select_query = match query {
            Query::Select(s) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "users".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("id".to_string(), DataType::Integer),
            ],
        )]
        .into_iter()
        .collect();

        let err = RaExpression::parse_select(&select_query, &schemas).unwrap_err();

        dbg!(err);

        // TODO
        // todo!()
    }

    #[test]
    fn select_all() {
        let arena = Bump::new();
        let query_str = "SELECT * FROM users";

        let select = match Query::parse(query_str.as_bytes(), &arena) {
            Ok(Query::Select(s)) => s,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "users".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("id".to_string(), DataType::Integer),
            ],
        )]
        .into_iter()
        .collect();

        let (ra_expr, placeholders) = RaExpression::parse_select(&select, &schemas).unwrap();
        assert_eq!(HashMap::new(), placeholders);

        dbg!(&ra_expr);

        assert_eq!(
            RaExpression::Projection {
                inner: Box::new(RaExpression::BaseRelation {
                    name: "users".into(),
                    columns: vec![
                        ("name".into(), DataType::Text, AttributeId::new(0)),
                        ("id".into(), DataType::Integer, AttributeId::new(1))
                    ]
                }),
                attributes: vec![
                    ProjectionAttribute {
                        id: AttributeId::new(2),
                        name: "name".into(),
                        value: RaValueExpression::Attribute {
                            name: "name".into(),
                            ty: DataType::Text,
                            a_id: AttributeId::new(0)
                        }
                    },
                    ProjectionAttribute {
                        id: AttributeId::new(3),
                        name: "id".into(),
                        value: RaValueExpression::Attribute {
                            name: "id".into(),
                            ty: DataType::Integer,
                            a_id: AttributeId::new(1)
                        }
                    }
                ]
            },
            ra_expr
        );
    }
}
