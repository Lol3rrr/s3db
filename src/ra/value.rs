use std::collections::HashMap;

use crate::{
    ra::RaExpression,
    sql::{self, BinaryOperator, DataType, FunctionCall, Literal, ValueExpression},
};

use super::{error_context, types, AttributeId, ParseSelectError, Scope};

#[derive(Debug, PartialEq, Clone)]
pub enum RaValueExpression {
    Attribute {
        name: String,
        ty: DataType,
        a_id: AttributeId,
    },
    Placeholder(usize),
    Literal(Literal<'static>),
    List(Vec<RaValueExpression>),
    SubQuery {
        query: RaExpression,
    },
    Cast {
        inner: Box<RaValueExpression>,
        target: sql::DataType,
    },
    BinaryOperation {
        first: Box<Self>,
        second: Box<Self>,
        operator: BinaryOperator,
    },
    Function(RaFunction),
    Renamed {
        name: String,
        value: Box<Self>,
    },
}

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
    },
}

impl RaValueExpression {
    pub(super) fn parse_internal(
        scope: &mut Scope<'_>,
        expr: &ValueExpression<'_>,
        placeholders: &mut HashMap<usize, DataType>,
        ra_expr: &RaExpression,
    ) -> Result<Self, ParseSelectError> {
        match expr {
            ValueExpression::All => Err(ParseSelectError::NotImplemented(
                "Parsing All Value Expression",
            )),
            ValueExpression::Literal(lit) => Ok(Self::Literal(lit.to_static())),
            ValueExpression::Placeholder(p) => Ok(Self::Placeholder(*p)),
            ValueExpression::ColumnReference(cr) => {
                let name = cr.column.0.to_string();
                let dtype = ra_expr.get_column(&name).ok_or_else(|| {
                    ParseSelectError::UnknownAttribute {
                        attr: cr.to_static(),
                        available: ra_expr
                            .get_columns()
                            .into_iter()
                            .map(|(n, _, _)| n)
                            .collect(),
                        context: error_context!("ValueExpression Parsing"),
                    }
                })?;
                let attr_id = ra_expr
                    .get_columns()
                    .into_iter()
                    .find(|(n, _, _)| n == &name)
                    .map(|(_, _, aid)| aid)
                    .ok_or_else(|| ParseSelectError::UnknownAttribute {
                        attr: cr.to_static(),
                        available: ra_expr
                            .get_columns()
                            .into_iter()
                            .map(|(n, _, _)| n)
                            .collect(),
                        context: error_context!("ValueExpression Parsing"),
                    })?;

                Ok(Self::Attribute {
                    ty: dtype,
                    name,
                    a_id: attr_id,
                })
            }
            ValueExpression::SubQuery(select) => {
                let s = RaExpression::parse_s(select, scope, placeholders, ra_expr)?;
                Ok(Self::SubQuery { query: s })
            }
            ValueExpression::All => {
                // TODO

                Err(ParseSelectError::NotImplemented(
                    "Parse ALL(*) Value Expression",
                ))
            }
            ValueExpression::List(elems) => {
                let ra_elems: Vec<_> = elems
                    .iter()
                    .map(|e| RaValueExpression::parse_internal(scope, e, placeholders, ra_expr))
                    .collect::<Result<_, _>>()?;

                Ok(RaValueExpression::List(ra_elems))
            }
            ValueExpression::FunctionCall(fc) => match fc {
                FunctionCall::LPad {
                    base,
                    length,
                    padding,
                } => {
                    let ra_base = Self::parse_internal(scope, &base, placeholders, ra_expr)?;

                    let length = match length {
                        Literal::SmallInteger(v) => *v as i64,
                        Literal::Integer(v) => *v as i64,
                        Literal::BigInteger(v) => *v,
                        other => {
                            dbg!(&other);
                            return Err(ParseSelectError::Other);
                        }
                    };

                    let padding = match padding {
                        Literal::Str(v) => v,
                        other => {
                            dbg!(other);
                            return Err(ParseSelectError::Other);
                        }
                    };

                    Ok(RaValueExpression::Function(RaFunction::LeftPad {
                        base: Box::new(ra_base),
                        length,
                        padding: padding.to_string(),
                    }))
                }
                FunctionCall::Coalesce { values } => {
                    let ra_values: Vec<_> = values
                        .iter()
                        .map(|v| RaValueExpression::parse_internal(scope, v, placeholders, ra_expr))
                        .collect::<Result<_, _>>()?;

                    let possible_types: Vec<_> = ra_values
                        .iter()
                        .map(|v| {
                            v.possible_type(&scope).map_err(|_| {
                                ParseSelectError::DeterminePossibleTypes { expr: v.clone() }
                            })
                        })
                        .collect::<Result<_, _>>()?;

                    let mut fitting_types = possible_types.first().cloned().unwrap();

                    for possible in possible_types.iter() {
                        fitting_types = fitting_types.compatible(possible);
                    }

                    let resolved_type =
                        fitting_types
                            .resolve()
                            .ok_or(ParseSelectError::IncompatibleTypes {
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

                    Ok(Self::Function(RaFunction::Coalesce(ra_values)))
                }
                FunctionCall::Exists { query } => {
                    dbg!(query);
                    Err(ParseSelectError::NotImplemented("Parse Exists"))
                }
                FunctionCall::SetValue {
                    sequence_name,
                    value,
                    is_called,
                } => {
                    let ra_value =
                        RaValueExpression::parse_internal(scope, &value, placeholders, ra_expr)?;

                    let ra_value = ra_value
                        .enforce_type(DataType::BigInteger, placeholders)
                        .ok_or_else(|| ParseSelectError::Other)?;

                    Ok(Self::Function(RaFunction::SetValue {
                        name: sequence_name.0.to_string(),
                        value: Box::new(ra_value),
                        is_called: *is_called,
                    }))
                }
                FunctionCall::Lower { value } => {
                    let raw_ra_value =
                        RaValueExpression::parse_internal(scope, &value, placeholders, ra_expr)?;

                    let ra_value = raw_ra_value
                        .enforce_type(DataType::Text, placeholders)
                        .ok_or_else(|| ParseSelectError::Other)?;

                    Ok(Self::Function(RaFunction::Lower(Box::new(ra_value))))
                }
                FunctionCall::Substr { value, start } => {
                    let str_value =
                        RaValueExpression::parse_internal(scope, &value, placeholders, ra_expr)?;
                    let start =
                        RaValueExpression::parse_internal(scope, &start, placeholders, ra_expr)?;

                    Ok(Self::Function(RaFunction::Substr {
                        str_value: Box::new(str_value),
                        start: Box::new(start),
                    }))
                }
            },
            ValueExpression::Operator {
                first,
                second,
                operator,
            } => {
                let ra_first = Self::parse_internal(scope, &first, placeholders, ra_expr)?;
                let ra_second = Self::parse_internal(scope, &second, placeholders, ra_expr)?;

                match operator {
                    BinaryOperator::Concat => {
                        let ra_first = ra_first
                            .enforce_type(DataType::Text, placeholders)
                            .ok_or(ParseSelectError::Other)?;

                        let ra_second = ra_second
                            .enforce_type(DataType::Text, placeholders)
                            .ok_or(ParseSelectError::Other)?;

                        Ok(RaValueExpression::BinaryOperation {
                            first: Box::new(ra_first),
                            second: Box::new(ra_second),
                            operator: operator.clone(),
                        })
                    }
                    BinaryOperator::Multiply => {
                        let numeric_types = types::PossibleTypes::specific([
                            DataType::SmallInteger,
                            DataType::Integer,
                            DataType::BigInteger,
                            DataType::DoublePrecision,
                            DataType::Real,
                        ]);

                        let first_possible_types = ra_first
                            .possible_type(scope)
                            .map_err(|e| ParseSelectError::Other)?
                            .compatible(&numeric_types);
                        let second_possible_types = ra_second
                            .possible_type(scope)
                            .map_err(|e| ParseSelectError::Other)?
                            .compatible(&numeric_types);

                        let resolved_type = first_possible_types
                            .compatible(&second_possible_types)
                            .resolve()
                            .ok_or_else(|| ParseSelectError::IncompatibleTypes {
                                mismatching_types: vec![
                                    first_possible_types,
                                    second_possible_types,
                                ],
                            })?;

                        let ra_first = ra_first
                            .enforce_type(resolved_type.clone(), placeholders)
                            .ok_or(ParseSelectError::Other)?;
                        let ra_second = ra_second
                            .enforce_type(resolved_type.clone(), placeholders)
                            .ok_or(ParseSelectError::Other)?;

                        Ok(RaValueExpression::BinaryOperation {
                            first: Box::new(ra_first),
                            second: Box::new(ra_second),
                            operator: operator.clone(),
                        })
                    }
                    BinaryOperator::Add => {
                        // TODO

                        Ok(RaValueExpression::BinaryOperation {
                            first: Box::new(ra_first),
                            second: Box::new(ra_second),
                            operator: operator.clone(),
                        })
                    }
                    other => {
                        dbg!(other);
                        Err(ParseSelectError::NotImplemented("Parsing Unknown Operator"))
                    }
                }
            }
            ValueExpression::TypeCast { base, target_ty } => {
                dbg!(&base, &target_ty);
                let ra_base = Self::parse_internal(scope, &base, placeholders, ra_expr)?;

                let base_types = ra_base
                    .possible_type(scope)
                    .map_err(|e| ParseSelectError::Other)?;
                dbg!(&base_types);

                let compatible_types =
                    base_types.compatible(&types::PossibleTypes::fixed(target_ty.clone()));
                dbg!(&compatible_types);

                compatible_types
                    .resolve()
                    .ok_or_else(|| ParseSelectError::IncompatibleTypes {
                        mismatching_types: vec![
                            base_types,
                            types::PossibleTypes::fixed(target_ty.clone()),
                        ],
                    })?;

                Ok(Self::Cast {
                    inner: Box::new(ra_base),
                    target: target_ty.clone(),
                })
            }
            ValueExpression::Null => Ok(Self::Literal(Literal::Null)),
            ValueExpression::Renamed { inner, name } => {
                let inner_ra = Self::parse_internal(scope, &inner, placeholders, ra_expr)?;

                Ok(Self::Renamed {
                    name: name.0.to_string(),
                    value: Box::new(inner_ra),
                })
            }
            other => {
                dbg!(other);

                Err(ParseSelectError::NotImplemented(
                    "Parse Other ValueExpression",
                ))
            }
        }
    }

    pub(super) fn possible_type(&self, scope: &Scope<'_>) -> Result<types::PossibleTypes, ()> {
        match self {
            Self::Attribute { ty, .. } => {
                Ok(types::PossibleTypes::fixed_with_conversions(ty.clone()))
            }
            Self::Placeholder(_) => Ok(types::PossibleTypes::all()),
            Self::Literal(lit) => match lit.datatype() {
                Some(ty) => Ok(types::PossibleTypes::fixed_with_conversions(ty)),
                None => Ok(types::PossibleTypes::all()),
            },
            Self::List(elems) => {
                let mut iter = elems.iter().map(|e| e.possible_type(scope));

                let mut first = iter.next().ok_or(())??;
                for part in iter {
                    let part = part?;
                    first = first.compatible(&part);
                }

                Ok(first)
            }
            Self::SubQuery { query } => {
                let mut columns = query.get_columns();

                if columns.len() != 1 {
                    dbg!(query, columns);
                    return Err(());
                }

                let (_, ty, _) = columns.remove(0);

                Ok(types::PossibleTypes::fixed_with_conversions(ty))
            }
            Self::Cast { target, .. } => {
                Ok(types::PossibleTypes::fixed_with_conversions(target.clone()))
            }
            Self::BinaryOperation {
                first,
                second,
                operator,
            } => match operator {
                BinaryOperator::Concat => Ok(types::PossibleTypes::fixed(DataType::Text)),
                other => {
                    dbg!(&other);
                    Err(())
                }
            },
            Self::Function(fc) => match fc {
                RaFunction::LeftPad {
                    base,
                    length,
                    padding,
                } => Ok(types::PossibleTypes::fixed(DataType::Text)),
                RaFunction::Coalesce(tmp) => tmp.first().unwrap().possible_type(scope),
                RaFunction::SetValue {
                    name,
                    value,
                    is_called,
                } => Ok(types::PossibleTypes::fixed(DataType::BigInteger)),
                RaFunction::Lower(val) => Ok(types::PossibleTypes::fixed(DataType::Text)),
                RaFunction::Substr { str_value, start } => {
                    Ok(types::PossibleTypes::fixed(DataType::Text))
                }
            },
            Self::Renamed { name, value } => value.possible_type(scope),
        }
    }

    pub(crate) fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Attribute { ty, .. } => Some(ty.clone()),
            Self::Placeholder(_) => None,
            Self::Literal(lit) => lit.datatype(),
            Self::List(elems) => todo!(),
            Self::SubQuery { query } => {
                let mut columns = query.get_columns();

                if columns.len() != 1 {
                    dbg!(query, columns);
                    return None;
                }

                let (_, ty, _) = columns.remove(0);

                Some(ty)
            }
            Self::Cast { target, .. } => Some(target.clone()),
            Self::BinaryOperation {
                first,
                second,
                operator,
            } => match operator {
                BinaryOperator::Concat => Some(DataType::Text),
                BinaryOperator::Add => first.datatype(),
                other => {
                    dbg!(other);
                    None
                }
            },
            Self::Function(fc) => match fc {
                RaFunction::LeftPad {
                    base,
                    length,
                    padding,
                } => Some(DataType::Text),
                RaFunction::Coalesce(tmp) => tmp.first().unwrap().datatype(),
                RaFunction::SetValue {
                    name,
                    value,
                    is_called,
                } => Some(DataType::BigInteger),
                RaFunction::Lower(_) => Some(DataType::Text),
                RaFunction::Substr { str_value, start } => Some(DataType::Text),
            },
            Self::Renamed { name, value } => value.datatype(),
        }
    }

    pub(super) fn enforce_type(
        self,
        target: DataType,
        placeholders: &mut HashMap<usize, DataType>,
    ) -> Option<Self> {
        match self {
            Self::List(elems) => {
                let parts = elems
                    .into_iter()
                    .map(|e| e.enforce_type(target.clone(), placeholders))
                    .collect::<Option<_>>()?;
                Some(Self::List(parts))
            }
            Self::Placeholder(p) => {
                let previous = placeholders.insert(p, target);
                assert_eq!(None, previous);
                Some(Self::Placeholder(p))
            }
            Self::Attribute { ty, name, a_id } if ty == target => {
                Some(Self::Attribute { ty, name, a_id })
            }
            Self::SubQuery { query } => {
                // TODO
                dbg!(&query);

                Some(Self::SubQuery { query })
            }
            other if other.datatype().map(|t| t == target).unwrap_or(false) => Some(other),
            other => Some(Self::Cast {
                inner: Box::new(other),
                target,
            }),
        }
    }
}
