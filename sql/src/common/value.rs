use crate::{Literal, ColumnReference, BinaryOperator, FunctionCall, AggregateExpression, DataType, select, Identifier, CompatibleParser, Parser as _};
use super::aggregate;

use nom::{Parser, IResult};

/// [Reference](https://www.postgresql.org/docs/current/sql-expressions.html)
#[derive(Debug, PartialEq, Clone)]
pub enum ValueExpression<'s> {
    Literal(Literal<'s>),
    All,
    AllFromRelation {
        relation: Identifier<'s>,
    },
    Null,
    ColumnReference(ColumnReference<'s>),
    Renamed {
        inner: Box<ValueExpression<'s>>,
        name: Identifier<'s>,
    },
    PositionalParameterReference,
    SubscriptedExpression,
    FieldSelectionExpression,
    Operator {
        first: Box<ValueExpression<'static>>,
        second: Box<ValueExpression<'static>>,
        operator: BinaryOperator,
    },
    FunctionCall(FunctionCall<'s>),
    AggregateExpression(AggregateExpression),
    WindowFunctionCall,
    TypeCast {
        base: Box<ValueExpression<'s>>,
        target_ty: DataType,
    },
    CollationExpression,
    ScalarSubquery,
    ArrayConstructor,
    RowConstructor,
    ParenedExpression,
    Placeholder(usize),
    List(Vec<ValueExpression<'s>>),
    SubQuery(select::Select<'s>),
    Not(Box<ValueExpression<'s>>),
    Case {
        matched_value: Box<ValueExpression<'s>>,
        cases: Vec<(Vec<ValueExpression<'s>>, ValueExpression<'s>)>,
        else_case: Option<Box<ValueExpression<'s>>>,
    },
}

impl<'s> ValueExpression<'s> {
    pub fn is_aggregate(&self) -> bool {
        match self {
            Self::Literal(_) => false,
            Self::All => false,
            Self::AllFromRelation { .. } => false,
            Self::Null => false,
            Self::ColumnReference(_) => false,
            Self::Renamed { inner, .. } => inner.is_aggregate(),
            Self::PositionalParameterReference => false,
            Self::SubscriptedExpression => false,
            Self::FieldSelectionExpression => false,
            // TODO Is this actually true?
            Self::Operator { .. } => false,
            // TODO Check this
            Self::FunctionCall(_) => false,
            Self::AggregateExpression(_) => true,
            Self::WindowFunctionCall => false,
            Self::TypeCast { base, .. } => base.is_aggregate(),
            Self::CollationExpression => false,
            Self::ScalarSubquery => false,
            Self::ArrayConstructor => false,
            Self::RowConstructor => false,
            Self::ParenedExpression => false,
            Self::Placeholder(_) => false,
            Self::List(_) => false,
            Self::SubQuery(_) => false,
            Self::Not(v) => v.is_aggregate(),
            Self::Case { .. } => false, // TODO is this actually correct?
        }
    }

    pub fn to_static(&self) -> ValueExpression<'static> {
        match self {
            Self::Literal(lit) => ValueExpression::Literal(lit.to_static()),
            Self::All => ValueExpression::All,
            Self::AllFromRelation { relation } => ValueExpression::AllFromRelation {
                relation: relation.to_static(),
            },
            Self::Null => ValueExpression::Null,
            Self::ColumnReference(cr) => ValueExpression::ColumnReference(cr.to_static()),
            Self::Renamed { inner, name } => ValueExpression::Renamed {
                inner: Box::new(inner.to_static()),
                name: name.to_static(),
            },
            Self::PositionalParameterReference => ValueExpression::PositionalParameterReference,
            Self::SubscriptedExpression => ValueExpression::SubscriptedExpression,
            Self::FieldSelectionExpression => ValueExpression::FieldSelectionExpression,
            Self::Operator {
                first,
                second,
                operator,
            } => ValueExpression::Operator {
                first: first.clone(),
                second: second.clone(),
                operator: operator.clone(),
            },
            Self::FunctionCall(fc) => ValueExpression::FunctionCall(fc.to_static()),
            Self::AggregateExpression(ae) => ValueExpression::AggregateExpression(ae.clone()),
            Self::WindowFunctionCall => ValueExpression::WindowFunctionCall,
            Self::TypeCast { base, target_ty } => ValueExpression::TypeCast {
                base: Box::new(base.to_static()),
                target_ty: target_ty.clone(),
            },
            Self::CollationExpression => ValueExpression::CollationExpression,
            Self::ScalarSubquery => ValueExpression::ScalarSubquery,
            Self::ArrayConstructor => ValueExpression::ArrayConstructor,
            Self::RowConstructor => ValueExpression::RowConstructor,
            Self::ParenedExpression => ValueExpression::ParenedExpression,
            Self::Placeholder(p) => ValueExpression::Placeholder(*p),
            Self::List(vals) => ValueExpression::List(vals.iter().map(|v| v.to_static()).collect()),
            Self::SubQuery(s) => ValueExpression::SubQuery(s.to_static()),
            Self::Not(v) => ValueExpression::Not(Box::new(v.to_static())),
            Self::Case {
                matched_value,
                cases,
                else_case,
            } => ValueExpression::Case {
                matched_value: Box::new(matched_value.to_static()),
                cases: cases
                    .iter()
                    .map(|(matching, value)| {
                        (
                            matching.iter().map(|v| v.to_static()).collect(),
                            value.to_static(),
                        )
                    })
                    .collect(),
                else_case: else_case.as_ref().map(|c| Box::new(c.to_static())),
            },
        }
    }

    pub fn max_parameter(&self) -> usize {
        match self {
            Self::Literal(_) => 0,
            Self::All => 0,
            Self::AllFromRelation { .. } => 0,
            Self::Null => 0,
            Self::ColumnReference(_) => 0,
            Self::Renamed { inner, .. } => inner.max_parameter(),
            Self::PositionalParameterReference => 0,
            Self::SubscriptedExpression => 0,
            Self::FieldSelectionExpression => 0,
            Self::Operator { first, second, .. } => {
                core::cmp::max(first.max_parameter(), second.max_parameter())
            }
            Self::FunctionCall(fc) => fc.max_parameter(),
            Self::AggregateExpression(ae) => match ae {
                AggregateExpression::Count(c) => c.max_parameter(),
                AggregateExpression::Sum(s) => s.max_parameter(),
                AggregateExpression::AnyValue(av) => av.max_parameter(),
                AggregateExpression::Max(v) => v.max_parameter(),
            },
            Self::WindowFunctionCall => 0,
            Self::TypeCast { base, .. } => base.max_parameter(),
            Self::CollationExpression => 0,
            Self::ScalarSubquery => 0,
            Self::ArrayConstructor => 0,
            Self::RowConstructor => 0,
            Self::ParenedExpression => 0,
            Self::Placeholder(p) => *p,
            Self::List(parts) => parts.iter().map(|p| p.max_parameter()).max().unwrap_or(0),
            Self::SubQuery(s) => s.max_parameter(),
            Self::Not(v) => v.max_parameter(),
            Self::Case {
                matched_value,
                cases,
                else_case,
            } => core::iter::once(matched_value.max_parameter())
                .chain(cases.iter().flat_map(|(targets, value)| {
                    targets
                        .iter()
                        .map(|t| t.max_parameter())
                        .chain(core::iter::once(value.max_parameter()))
                }))
                .chain(else_case.iter().map(|c| c.max_parameter()))
                .max()
                .unwrap_or(0),
        }
    }
}

impl<'i, 's> crate::Parser<'i> for ValueExpression<'s> where 'i: 's {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            value_expression(i)
        }
    }
}


impl<'i, 's> crate::Parser<'i> for Vec<ValueExpression<'s>> where 'i: 's {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            nom::multi::separated_list1(
                nom::bytes::complete::tag(","),
                nom::sequence::delimited(
                    nom::character::complete::multispace0,
                    ValueExpression::parse(),
                    nom::character::complete::multispace0,
                ),
            )(i)
        }
    }
}

#[deprecated]
pub fn value_expression(
    i: &[u8],
) -> IResult<&[u8], ValueExpression<'_>, nom::error::VerboseError<&[u8]>> {
    let (remaining, parsed) = nom::branch::alt((
        nom::bytes::complete::tag("*").map(|_| ValueExpression::All),
        nom::bytes::complete::tag_no_case("NULL").map(|_| ValueExpression::Null),
        FunctionCall::parse().map(ValueExpression::FunctionCall),
        aggregate.map(ValueExpression::AggregateExpression),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("NOT"),
            nom::character::complete::multispace1,
            value_expression,
        ))
        .map(|(_, _, value)| ValueExpression::Not(Box::new(value))),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("CASE"),
            nom::character::complete::multispace1,
            value_expression,
            nom::multi::many1(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("WHEN"),
                    nom::character::complete::multispace1,
                    nom::multi::separated_list1(
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag(","),
                            nom::character::complete::multispace0,
                        )),
                        value_expression,
                    ),
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("THEN"),
                    nom::character::complete::multispace1,
                    value_expression,
                ))
                .map(|(_, _, _, target_expr, _, _, _, value)| (target_expr, value)),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("ELSE"),
                    nom::character::complete::multispace1,
                    value_expression,
                ))
                .map(|(_, _, _, v)| v),
            ),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag("END"),
        ))
        .map(
            |(_, _, matched_value, cases, else_case, _, _)| ValueExpression::Case {
                matched_value: Box::new(matched_value),
                cases,
                else_case: else_case.map(Box::new),
            },
        ),
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag("("),
                nom::character::complete::multispace0,
                select::select,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            )),
            |(_, _, s, _, _)| ValueExpression::SubQuery(s),
        ),
        nom::combinator::map(
            nom::sequence::tuple((
                Identifier::parse(),
                nom::bytes::complete::tag("."),
                nom::bytes::complete::tag("*"),
            )),
            |(relation, _, _)| ValueExpression::AllFromRelation { relation },
        ),
        nom::combinator::map(
            nom::sequence::tuple((ColumnReference::parse(), nom::bytes::complete::tag("::"), DataType::parse())),
            |(cr, _, ty)| ValueExpression::TypeCast {
                base: Box::new(ValueExpression::ColumnReference(cr)),
                target_ty: ty,
            },
        ),
        Literal::parse().map(ValueExpression::Literal),
        nom::combinator::map(ColumnReference::parse(), ValueExpression::ColumnReference),
        nom::sequence::tuple((
            nom::bytes::complete::tag("$"),
            nom::character::complete::digit1,
        ))
        .map(|(_, d)| {
            let raw = core::str::from_utf8(d).unwrap();
            let val: usize = raw.parse().unwrap();
            ValueExpression::Placeholder(val)
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag("("),
            nom::character::complete::multispace0,
            value_expression,
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(")"),
        ))
        .map(|(_, _, vexp, _, _)| vexp),
    ))(i)?;

    let (remaining, parsed) = match nom::sequence::tuple((
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("AS"),
        nom::character::complete::multispace1,
        Identifier::parse(),
    ))(remaining)
    {
        Ok((remaining, (_, _, _, name))) => (
            remaining,
            ValueExpression::Renamed {
                inner: Box::new(parsed),
                name,
            },
        ),
        _ => (remaining, parsed),
    };

    let tmp: IResult<&[u8], _, nom::error::VerboseError<&[u8]>> = nom::sequence::tuple((
        nom::character::complete::multispace0,
        nom::combinator::not(nom::combinator::peek(nom::branch::alt((
            nom::bytes::complete::tag_no_case("INNER"),
        )))),
        BinaryOperator::parse(), 
        nom::character::complete::multispace0,
    ))(remaining);
    let (remaining, (_, _, operator, _)) = match tmp {
        Ok(r) => r,
        Err(_) => {
            return Ok((remaining, parsed));
        }
    };

    let (remaining, second) = if matches!(operator, BinaryOperator::In | BinaryOperator::NotIn) {
        nom::branch::alt((
            nom::combinator::map(
                nom::sequence::tuple((
                    nom::bytes::complete::tag("("),
                    nom::multi::separated_list1(
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag(","),
                            nom::character::complete::multispace0,
                        )),
                        value_expression,
                    ),
                    nom::bytes::complete::tag(")"),
                )),
                |(_, parts, _)| ValueExpression::List(parts),
            ),
            nom::combinator::map(
                nom::sequence::tuple((
                    nom::bytes::complete::tag("("),
                    nom::character::complete::multispace0,
                    select::select,
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
                |(_, _, subquery, _, _)| ValueExpression::SubQuery(subquery),
            ),
        ))(remaining)?
    } else {
        value_expression(remaining)?
    };

    let result = match second {
        ValueExpression::Renamed { inner, name } => ValueExpression::Renamed {
            inner: Box::new(ValueExpression::Operator {
                first: Box::new(parsed.to_static()),
                second: Box::new((*inner).to_static()),
                operator,
            }),
            name,
        },
        other => ValueExpression::Operator {
            first: Box::new(parsed.to_static()),
            second: Box::new(other.to_static()),
            operator,
        },
    };

    Ok((remaining, result))
}
