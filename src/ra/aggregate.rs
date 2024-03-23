use std::collections::HashMap;

use crate::{
    ra::RaValueExpression,
    sql::{self, DataType, Literal, ValueExpression},
    storage::Schemas,
};

use super::{Attribute, AttributeId, ParseSelectError, RaExpression, Scope};

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateExpression {
    Renamed {
        inner: Box<Self>,
        name: String,
    },
    CountRows,
    Count {
        a_id: AttributeId,
    },
    Column {
        name: String,
        dtype: DataType,
        a_id: AttributeId,
    },
    Max {
        inner: Box<RaValueExpression>,
        dtype: DataType,
    },
    Literal(Literal<'static>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregationCondition {
    Everything,
    GroupBy { fields: Vec<(String, AttributeId)> },
}

impl AggregateExpression {
    pub(super) fn parse(
        value: &ValueExpression,
        scope: &mut Scope<'_>,
        previous_columns: &[(String, DataType, AttributeId)],
        placeholders: &mut HashMap<usize, DataType>,
        ra_expr: &RaExpression,
    ) -> Result<Attribute<Self>, ParseSelectError> {
        match value {
            ValueExpression::AggregateExpression(exp) => match exp {
                sql::AggregateExpression::Count(tmp) => match tmp.as_ref() {
                    sql::ValueExpression::All => Ok(Attribute {
                        id: scope.attribute_id(),
                        name: "".to_string(),
                        value: AggregateExpression::CountRows,
                    }),
                    sql::ValueExpression::ColumnReference(cr) => {
                        let target_id = previous_columns
                            .iter()
                            .find(|(n, _, _)| n == cr.column.0.as_ref())
                            .map(|(_, _, id)| id)
                            .ok_or_else(|| ParseSelectError::UnknownAttribute {
                                attr: cr.to_static(),
                                available: previous_columns
                                    .iter()
                                    .map(|(n, _, _)| n.clone())
                                    .collect(),
                                context: "Parse Aggregate Expression".into(),
                            })?;

                        Ok(Attribute {
                            id: scope.attribute_id(),
                            name: "".to_string(),
                            value: AggregateExpression::Count { a_id: *target_id },
                        })
                    }
                    other => {
                        dbg!(other);

                        Err(ParseSelectError::NotImplemented(
                            "Parsing Other inner part of Count expression",
                        ))
                    }
                },
                sql::AggregateExpression::Sum(val) => {
                    dbg!(&val);

                    Err(ParseSelectError::NotImplemented("Parsing SUM Aggregate"))
                }
                sql::AggregateExpression::AnyValue(val) => Err(ParseSelectError::NotImplemented(
                    "Parsing AnyValue Aggregate",
                )),
                sql::AggregateExpression::Max(val) => {
                    let inner =
                        RaValueExpression::parse_internal(scope, &val, placeholders, ra_expr)?;

                    let inner_ty = inner.possible_type(&scope).map_err(|e| {
                        ParseSelectError::DeterminePossibleTypes {
                            expr: inner.clone(),
                        }
                    })?;

                    let ty = inner_ty.resolve().ok_or_else(|| {
                        ParseSelectError::DeterminePossibleTypes {
                            expr: inner.clone(),
                        }
                    })?;

                    Ok(Attribute {
                        id: scope.attribute_id(),
                        name: "".to_string(),
                        value: Self::Max {
                            inner: Box::new(inner),
                            dtype: ty,
                        },
                    })
                }
            },
            ValueExpression::ColumnReference(cr) => {
                let (ty, column) = previous_columns
                    .iter()
                    .find_map(|(n, ty, id)| {
                        if n == cr.column.0.as_ref() {
                            Some((ty, id))
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| ParseSelectError::UnknownAttribute {
                        attr: cr.to_static(),
                        available: previous_columns.iter().map(|(n, _, _)| n.clone()).collect(),
                        context: "Parse Aggregate Expression".into(),
                    })?;

                Ok(Attribute {
                    id: scope.attribute_id(),
                    name: cr.column.0.to_string(),
                    value: AggregateExpression::Column {
                        name: cr.column.0.to_string(),
                        dtype: ty.clone(),
                        a_id: column.clone(),
                    },
                })
            }
            ValueExpression::Literal(lit) => todo!(),
            ValueExpression::Renamed { inner, name } => {
                let inner = Self::parse(&inner, scope, previous_columns, placeholders, ra_expr)?;

                Ok(Attribute {
                    id: inner.id,
                    name: name.0.to_string(),
                    value: AggregateExpression::Renamed {
                        inner: Box::new(inner.value),
                        name: name.0.to_string(),
                    },
                })
            }
            other => {
                dbg!(&other);
                Err(ParseSelectError::Other)
            }
        }
    }

    pub fn return_ty(&self) -> DataType {
        match self {
            Self::Renamed { inner, name } => inner.return_ty(),
            Self::CountRows => DataType::BigInteger,
            Self::Count { .. } => DataType::BigInteger,
            Self::Column { dtype, .. } => dtype.clone(),
            Self::Literal(lit) => lit.datatype().unwrap(),
            Self::Max { dtype, .. } => dtype.clone(),
        }
    }

    pub fn check(
        &self,
        cond: &AggregationCondition,
        schemas: &Schemas,
    ) -> Result<(), ParseSelectError> {
        match cond {
            AggregationCondition::Everything => match self {
                Self::CountRows => Ok(()),
                Self::Count { .. } => Ok(()),
                Self::Literal(_) => Ok(()),
                Self::Max { .. } => Ok(()),
                Self::Renamed { inner, .. } => inner.check(cond, schemas),
                other => {
                    dbg!(other);
                    Err(ParseSelectError::NotImplemented("Other AggregateExpression than a Literal or Count(*) for not grouped query"))
                }
            },
            AggregationCondition::GroupBy { fields } => match self {
                AggregateExpression::CountRows => Ok(()),
                AggregateExpression::Renamed { inner, .. } => inner.check(cond, schemas),
                AggregateExpression::Count { a_id } => Ok(()),
                AggregateExpression::Column { name, dtype, a_id } => {
                    if fields.iter().find(|(_, id)| a_id == id).is_none() {
                        dbg!(&name, &dtype, &a_id);
                        Err(ParseSelectError::NotImplemented(
                            "Not Aggregated Attribute used",
                        ))
                    } else {
                        Ok(())
                    }
                }
                AggregateExpression::Max { inner, dtype } => {
                    todo!("Max {:?}", (inner, dtype))
                }
                AggregateExpression::Literal(_) => Ok(()),
            },
        }
    }
}
