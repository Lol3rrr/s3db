use std::collections::HashMap;

use crate::{ra::RaExpression, storage::Schemas};
use sql::{BinaryOperator, Condition, DataType, FunctionCall, Literal, ValueExpression};

use super::{
    error_context, AttributeId, ParseSelectError, RaComparisonOperator, RaValueExpression, Scope,
};

#[derive(Debug, PartialEq, Clone)]
pub enum RaCondition {
    And(Vec<RaCondition>),
    Or(Vec<RaCondition>),
    Value(Box<RaConditionValue>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct RaOrCondition {
    pub conditions: Vec<RaAndCondition>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RaAndCondition {
    pub conditions: Vec<RaCondition>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum RaConditionValue {
    Attribute {
        name: String,
        ty: DataType,
        a_id: AttributeId,
    },
    Comparison {
        first: RaValueExpression,
        second: RaValueExpression,
        comparison: RaComparisonOperator,
    },
    Negation {
        inner: Box<RaConditionValue>,
    },
    Exists {
        query: Box<RaExpression>,
    },
    Constant {
        value: bool,
    },
}

impl RaCondition {
    pub fn parse(
        condition: &Condition<'_, '_>,
        schemas: &Schemas,
    ) -> Result<(Self, HashMap<usize, DataType>), ParseSelectError> {
        let mut scope = Scope::new(schemas);

        let mut placeholders = HashMap::new();

        let cond = Self::parse_internal(
            &mut scope,
            condition,
            &mut placeholders,
            &mut RaExpression::EmptyRelation,
            &mut Vec::new(),
        )?;

        Ok((cond, placeholders))
    }

    pub(super) fn parse_internal(
        scope: &mut Scope<'_>,
        condition: &Condition<'_, '_>,
        placeholders: &mut HashMap<usize, DataType>,
        ra_expr: &mut RaExpression,
        outer: &mut Vec<RaExpression>,
    ) -> Result<Self, ParseSelectError> {
        match condition {
            Condition::And(p) => {
                let parts: Vec<_> = p
                    .iter()
                    .map(|c| Self::parse_internal(scope, c, placeholders, ra_expr, outer))
                    .collect::<Result<_, _>>()?;
                Ok(RaCondition::And(parts))
            }
            Condition::Or(p) => {
                let parts: Vec<_> = p
                    .iter()
                    .map(|c| Self::parse_internal(scope, c, placeholders, ra_expr, outer))
                    .collect::<Result<_, _>>()?;
                Ok(RaCondition::Or(parts))
            }
            Condition::Value(v) => {
                let value =
                    RaConditionValue::parse_internal(scope, v, placeholders, ra_expr, outer)?;
                Ok(RaCondition::Value(Box::new(value)))
            }
        }
    }
}

impl RaConditionValue {
    fn parse_internal(
        scope: &mut Scope<'_>,
        condition: &ValueExpression<'_, '_>,
        placeholders: &mut HashMap<usize, DataType>,
        ra_expr: &mut RaExpression,
        outer: &mut Vec<RaExpression>,
    ) -> Result<Self, ParseSelectError> {
        match condition {
            ValueExpression::ColumnReference(cr) => {
                let name = cr.column.0.to_string();
                let (dtype, attr_id) = ra_expr
                    .get_columns()
                    .into_iter()
                    .find(|(t, n, _, _)| {
                        n == &name
                            && cr
                                .relation
                                .as_ref()
                                .map(|r| r.0.as_ref() == t.as_str())
                                .unwrap_or(true)
                    })
                    .map(|(_, _, ty, aid)| (ty, aid))
                    .ok_or_else(|| ParseSelectError::UnknownAttribute {
                        attr: cr.to_static(),
                        available: ra_expr
                            .get_columns()
                            .into_iter()
                            .map(|(r, n, _, _)| (r, n))
                            .collect(),
                        context: error_context!("ValueExpression Parsing"),
                    })?;

                Ok(Self::Attribute {
                    ty: dtype,
                    name,
                    a_id: attr_id,
                })
            }
            ValueExpression::Operator {
                first,
                second,
                operator,
            } => {
                let ra_first = RaValueExpression::parse_internal(
                    scope,
                    first.as_ref(),
                    placeholders,
                    ra_expr,
                    outer,
                )?;

                let ra_second = RaValueExpression::parse_internal(
                    scope,
                    second.as_ref(),
                    placeholders,
                    ra_expr,
                    outer,
                )?;

                let first_types = ra_first.possible_type(scope).map_err(|_| {
                    ParseSelectError::DeterminePossibleTypes {
                        expr: ra_first.clone(),
                    }
                })?;
                let second_types = ra_second.possible_type(scope).map_err(|_| {
                    ParseSelectError::DeterminePossibleTypes {
                        expr: ra_second.clone(),
                    }
                })?;

                let op = match operator {
                    BinaryOperator::Equal => RaComparisonOperator::Equals,
                    BinaryOperator::NotEqual => RaComparisonOperator::NotEquals,
                    BinaryOperator::Greater => RaComparisonOperator::Greater,
                    BinaryOperator::GreaterEqual => RaComparisonOperator::GreaterEqual,
                    BinaryOperator::In => RaComparisonOperator::In,
                    BinaryOperator::NotIn => RaComparisonOperator::NotIn,
                    BinaryOperator::Like => RaComparisonOperator::Like,
                    BinaryOperator::ILike => RaComparisonOperator::ILike,
                    BinaryOperator::Is => RaComparisonOperator::Is,
                    BinaryOperator::IsNot => RaComparisonOperator::IsNot,
                    BinaryOperator::Less => RaComparisonOperator::Less,
                    BinaryOperator::LessEqual => RaComparisonOperator::LessEqual,
                    other => {
                        dbg!(other);

                        return Err(ParseSelectError::NotImplemented("Unknown Operator"));
                    }
                };

                let (ra_first, ra_second) = match op {
                    RaComparisonOperator::In | RaComparisonOperator::NotIn => {
                        let compatible_types = first_types.compatible(&second_types);

                        let resolved_type = compatible_types.resolve().ok_or_else(|| {
                            ParseSelectError::IncompatibleTypes {
                                mismatching_types: vec![first_types, second_types],
                            }
                        })?;

                        let ra_first = ra_first
                            .enforce_type(resolved_type.clone(), placeholders)
                            .unwrap();
                        let ra_second = ra_second
                            .enforce_type(resolved_type.clone(), placeholders)
                            .unwrap();

                        (ra_first, ra_second)
                    }
                    _ => {
                        let compatible_types = first_types.compatible(&second_types);

                        let resolved_type = compatible_types.resolve().ok_or_else(|| {
                            ParseSelectError::IncompatibleTypes {
                                mismatching_types: vec![first_types, second_types],
                            }
                        })?;

                        let ra_first = ra_first
                            .enforce_type(resolved_type.clone(), placeholders)
                            .unwrap();
                        let ra_second = ra_second
                            .enforce_type(resolved_type.clone(), placeholders)
                            .unwrap();

                        (ra_first, ra_second)
                    }
                };

                Ok(RaConditionValue::Comparison {
                    first: ra_first,
                    second: ra_second,
                    comparison: op,
                })
            }
            ValueExpression::Not(inner) => {
                let inner =
                    RaConditionValue::parse_internal(scope, inner, placeholders, ra_expr, outer)?;

                Ok(RaConditionValue::Negation {
                    inner: Box::new(inner),
                })
            }
            ValueExpression::FunctionCall(fc) => match fc {
                FunctionCall::Exists { query } => {
                    outer.push(ra_expr.clone());
                    let ra_expression = RaExpression::parse_s(query, scope, placeholders, outer)?;
                    outer.pop();

                    Ok(RaConditionValue::Exists {
                        query: Box::new(ra_expression),
                    })
                }
                other => {
                    dbg!(&other);

                    Err(ParseSelectError::NotImplemented(
                        "Parsing unknown Function Call",
                    ))
                }
            },
            ValueExpression::Literal(lit) => match lit {
                Literal::Bool(v) => Ok(RaConditionValue::Constant { value: *v }),
                other => {
                    dbg!(&other);
                    Err(ParseSelectError::Other("Unexpected Literal"))
                }
            },
            other => {
                dbg!(other);
                Err(ParseSelectError::NotImplemented(
                    "Other Value Expression (Condition)",
                ))
            }
        }
    }
}
