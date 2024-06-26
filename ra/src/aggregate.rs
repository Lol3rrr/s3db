use std::collections::HashMap;

use crate::RaValueExpression;

use sql::{DataType, Literal, ValueExpression};
use storage::Schemas;

use super::{Attribute, AttributeId, ParseSelectError, RaExpression, RaFunction, Scope};

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
    Expression(Box<RaValueExpression>),
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
        ra_expr: &mut RaExpression,
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
                                    .map(|(n, _, _)| (String::new(), n.clone()))
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
                sql::AggregateExpression::AnyValue(_val) => Err(ParseSelectError::NotImplemented(
                    "Parsing AnyValue Aggregate",
                )),
                sql::AggregateExpression::Sum(val) => {
                    Err(ParseSelectError::NotImplemented("Parsing SUM Aggregate"))
                }
                sql::AggregateExpression::Avg(val) => {
                    Err(ParseSelectError::NotImplemented("Parsing Avg Aggregate"))
                }
                sql::AggregateExpression::Max(val) => {
                    let inner = RaValueExpression::parse_internal(
                        scope,
                        val,
                        placeholders,
                        ra_expr,
                        &mut Vec::new(),
                    )?;

                    let inner_ty = inner.possible_type(scope).map_err(|_| {
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
                sql::AggregateExpression::Min(v) => {
                    Err(ParseSelectError::NotImplemented("Parsing Min Aggregate"))
                }
                sql::AggregateExpression::ArrayAgg(v) => Err(ParseSelectError::NotImplemented(
                    "Parsing ArrayAgg Aggregate",
                )),
                sql::AggregateExpression::BitAnd(v) => {
                    Err(ParseSelectError::NotImplemented("Parsing BitAnd Aggregate"))
                }
                sql::AggregateExpression::BitXor(v) => {
                    Err(ParseSelectError::NotImplemented("Parsing BitXor Aggregate"))
                }
                sql::AggregateExpression::BitOr(v) => {
                    Err(ParseSelectError::NotImplemented("Parsing BitOr Aggregate"))
                }
                sql::AggregateExpression::BoolAnd(v) => Err(ParseSelectError::NotImplemented(
                    "Parsing BoolAnd Aggregate",
                )),
                sql::AggregateExpression::BoolOr(v) => {
                    Err(ParseSelectError::NotImplemented("Parsing BoolOr Aggregate"))
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
                        available: previous_columns
                            .iter()
                            .map(|(n, _, _)| (String::new(), n.clone()))
                            .collect(),
                        context: "Parse Aggregate Expression".into(),
                    })?;

                Ok(Attribute {
                    id: scope.attribute_id(),
                    name: cr.column.0.to_string(),
                    value: AggregateExpression::Column {
                        name: cr.column.0.to_string(),
                        dtype: ty.clone(),
                        a_id: *column,
                    },
                })
            }
            ValueExpression::Literal(_lit) => todo!(),
            ValueExpression::Renamed { inner, name } => {
                let inner = Self::parse(inner, scope, previous_columns, placeholders, ra_expr)?;

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
                let parsed = RaValueExpression::parse_internal(
                    scope,
                    other,
                    placeholders,
                    ra_expr,
                    &mut Vec::new(),
                )?;

                Ok(Attribute {
                    id: scope.attribute_id(),
                    name: "".into(),
                    value: AggregateExpression::Expression(Box::new(parsed)),
                })
            }
        }
    }

    pub fn return_ty(&self) -> DataType {
        match self {
            Self::Renamed { inner, .. } => inner.return_ty(),
            Self::CountRows => DataType::BigInteger,
            Self::Count { .. } => DataType::BigInteger,
            Self::Column { dtype, .. } => dtype.clone(),
            Self::Literal(lit) => lit.datatype().unwrap(),
            Self::Max { dtype, .. } => dtype.clone(),
            Self::Expression(v) => v.datatype().unwrap(),
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
            AggregationCondition::GroupBy { fields } => {
                match self {
                    AggregateExpression::CountRows => Ok(()),
                    AggregateExpression::Renamed { inner, .. } => inner.check(cond, schemas),
                    AggregateExpression::Count { .. } => Ok(()),
                    AggregateExpression::Column { a_id, name, .. } => {
                        if !fields.iter().any(|(_, id)| a_id == id) {
                            Err(ParseSelectError::AggregateExpressionOutsideOfAggregate {
                                a_id: *a_id,
                                name: Some(name.clone()),
                                aggregated_fields: fields.clone(),
                            })
                        } else {
                            Ok(())
                        }
                    }
                    AggregateExpression::Max { inner, dtype } => {
                        todo!("Max {:?}", (inner, dtype))
                    }
                    AggregateExpression::Literal(_) => Ok(()),
                    AggregateExpression::Expression(expr) => {
                        let mut pending: Vec<&RaValueExpression> = vec![&expr];

                        while let Some(expr) = pending.pop() {
                            match expr {
                                RaValueExpression::Renamed { value, .. } => {
                                    pending.push(value);
                                }
                                RaValueExpression::Literal(_) => {}
                                RaValueExpression::Attribute { a_id, name, .. } => {
                                    if !fields.iter().any(|(_, id)| a_id == id) {
                                        return Err(ParseSelectError::AggregateExpressionOutsideOfAggregate { a_id: *a_id, name: Some(name.clone()), aggregated_fields: fields.clone() });
                                    }
                                }
                                RaValueExpression::Function(func) => match func {
                                    RaFunction::Coalesce(parts) => {
                                        pending.extend(parts.iter());
                                    }
                                    RaFunction::LeftPad { base, .. } => {
                                        pending.push(base);
                                    }
                                    RaFunction::SetValue { value, .. } => {
                                        pending.push(value);
                                    }
                                    RaFunction::Lower(parts) => {
                                        pending.push(parts);
                                    }
                                    RaFunction::Substr {
                                        str_value,
                                        start,
                                        count,
                                    } => {
                                        pending.push(str_value);
                                        pending.push(start);
                                        if let Some(c) = count {
                                            pending.push(c);
                                        }
                                    }
                                    RaFunction::CurrentSchemas { .. } => {
                                        todo!("CurrentSchemas");
                                    }
                                    RaFunction::ArrayPosition { .. } => {
                                        todo!("ArrayPosition")
                                    }
                                },
                                other => {
                                    dbg!(other);
                                    return Err(ParseSelectError::NotImplemented(
                                        "Checking different Value Expression",
                                    ));
                                }
                            };
                        }

                        Ok(())
                    }
                }
            }
        }
    }
}
