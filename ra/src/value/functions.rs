use std::collections::HashMap;

use sql::{DataType, FunctionCall, Literal};

use crate::{types, ParseRaError, RaExpression, RaValueExpression, Scope};

#[derive(Debug, PartialEq, Clone)]
pub enum RaFunction {
    Coalesce(Vec<RaValueExpression>),
    LeftPad {
        base: Box<RaValueExpression>,
        length: i64,
        padding: String,
    },
    SetValue {
        name: String,
        value: Box<RaValueExpression>,
        is_called: bool,
    },
    Lower(Box<RaValueExpression>),
    Substr {
        str_value: Box<RaValueExpression>,
        start: Box<RaValueExpression>,
        count: Option<Box<RaValueExpression>>,
    },
    CurrentSchemas {
        implicit: bool,
    },
    ArrayPosition {
        array: Box<RaValueExpression>,
        target: Box<RaValueExpression>,
    },
}

pub fn parse_fc(
    scope: &mut Scope<'_>,
    fc: &FunctionCall,
    placeholders: &mut HashMap<usize, DataType>,
    ra_expr: &mut RaExpression,
    outer: &mut Vec<RaExpression>,
) -> Result<RaFunction, ParseRaError> {
    match fc {
        FunctionCall::LPad {
            base,
            length,
            padding,
        } => {
            let ra_base =
                RaValueExpression::parse_internal(scope, base, placeholders, ra_expr, outer)?;

            let length = match length {
                Literal::SmallInteger(v) => *v as i64,
                Literal::Integer(v) => *v as i64,
                Literal::BigInteger(v) => *v,
                other => {
                    let other_type = other
                        .datatype()
                        .expect("The type of literals is always known");

                    return Err(ParseRaError::IncompatibleTypes {
                        mismatching_types: vec![
                            types::PossibleTypes::fixed_with_conversions(DataType::BigInteger),
                            types::PossibleTypes::fixed_with_conversions(other_type),
                        ],
                    });
                }
            };

            let padding = match padding {
                Literal::Str(v) => v,
                other => {
                    let other_type = other
                        .datatype()
                        .expect("The type of literals is always known");

                    return Err(ParseRaError::IncompatibleTypes {
                        mismatching_types: vec![
                            types::PossibleTypes::fixed_with_conversions(DataType::Text),
                            types::PossibleTypes::fixed_with_conversions(other_type),
                        ],
                    });
                }
            };

            Ok(RaFunction::LeftPad {
                base: Box::new(ra_base),
                length,
                padding: padding.to_string(),
            })
        }
        FunctionCall::Coalesce { values } => {
            let ra_values: Vec<_> = values
                .iter()
                .map(|v| RaValueExpression::parse_internal(scope, v, placeholders, ra_expr, outer))
                .collect::<Result<_, _>>()?;

            let possible_types: Vec<_> = ra_values
                .iter()
                .map(|v| {
                    v.possible_type(scope)
                        .map_err(|_| ParseRaError::DeterminePossibleTypes { expr: v.clone() })
                })
                .collect::<Result<_, _>>()?;

            let mut fitting_types = possible_types.first().cloned().unwrap();

            for possible in possible_types.iter() {
                fitting_types = fitting_types.compatible(possible);
            }

            let resolved_type = fitting_types
                .resolve()
                .ok_or(ParseRaError::IncompatibleTypes {
                    mismatching_types: possible_types,
                })?;

            let ra_values: Vec<RaValueExpression> = ra_values
                .into_iter()
                .map(|rav| {
                    if rav
                        .datatype()
                        .map(|rtype| rtype == resolved_type)
                        .unwrap_or(false)
                    {
                        rav
                    } else {
                        RaValueExpression::Cast {
                            inner: Box::new(rav),
                            target: resolved_type.clone(),
                        }
                    }
                })
                .collect();

            Ok(RaFunction::Coalesce(ra_values))
        }
        FunctionCall::Exists { query } => {
            dbg!(query);
            Err(ParseRaError::NotImplemented("Parse Exists"))
        }
        FunctionCall::NextValue { sequence_name } => {
            dbg!(sequence_name);
            Err(ParseRaError::NotImplemented("Parsing NextValue"))
        }
        FunctionCall::SetValue {
            sequence_name,
            value,
            is_called,
        } => {
            let ra_value =
                RaValueExpression::parse_internal(scope, value, placeholders, ra_expr, outer)?;

            let ra_value = ra_value
                .enforce_type(DataType::BigInteger, placeholders)
                .ok_or_else(|| ParseRaError::Other("Enforcing Type"))?;

            Ok(RaFunction::SetValue {
                name: sequence_name.0.to_string(),
                value: Box::new(ra_value),
                is_called: *is_called,
            })
        }
        FunctionCall::CurrentValue { sequence_name } => {
            dbg!(sequence_name);
            Err(ParseRaError::NotImplemented("Parsing CurrentValue"))
        }
        FunctionCall::LastValue {} => Err(ParseRaError::NotImplemented("Parsing LastValue")),
        FunctionCall::Lower { value } => {
            let raw_ra_value =
                RaValueExpression::parse_internal(scope, value, placeholders, ra_expr, outer)?;

            let ra_value = raw_ra_value
                .enforce_type(DataType::Text, placeholders)
                .ok_or_else(|| ParseRaError::Other("Enforcing Type"))?;

            Ok(RaFunction::Lower(Box::new(ra_value)))
        }
        FunctionCall::Substr {
            value,
            start,
            count,
        } => {
            let str_value =
                RaValueExpression::parse_internal(scope, value, placeholders, ra_expr, outer)?;
            let start =
                RaValueExpression::parse_internal(scope, start, placeholders, ra_expr, outer)?;
            let count = match count.as_ref() {
                Some(c) => {
                    let val =
                        RaValueExpression::parse_internal(scope, c, placeholders, ra_expr, outer)?;
                    Some(Box::new(val))
                }
                None => None,
            };

            Ok(RaFunction::Substr {
                str_value: Box::new(str_value),
                start: Box::new(start),
                count,
            })
        }
        FunctionCall::CurrentTimestamp => {
            // TODO

            Err(ParseRaError::NotImplemented(
                "Parse CurrentTimestamp Function",
            ))
        }
        FunctionCall::CurrentSchemas { implicit } => Ok(RaFunction::CurrentSchemas {
            implicit: *implicit,
        }),
        FunctionCall::ArrayPosition { array, target } => {
            let array_value =
                RaValueExpression::parse_internal(scope, &array, placeholders, ra_expr, outer)?;
            let target_value =
                RaValueExpression::parse_internal(scope, &target, placeholders, ra_expr, outer)?;

            // TODO
            // Type checking

            Ok(RaFunction::ArrayPosition {
                array: Box::new(array_value),
                target: Box::new(target_value),
            })
        }
    }
}
