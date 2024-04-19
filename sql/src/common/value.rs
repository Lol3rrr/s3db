use crate::{
    select, AggregateExpression, BinaryOperator, ColumnReference, CompatibleParser, DataType,
    FunctionCall, Identifier, Literal, Parser as _, ArenaParser
};

use nom::{IResult, Parser};

/// [Reference](https://www.postgresql.org/docs/current/sql-expressions.html)
#[derive(Debug, PartialEq)]
pub enum ValueExpression<'s, 'a> {
    Literal(Literal<'s>),
    All,
    AllFromRelation {
        relation: Identifier<'s>,
    },
    Null,
    ColumnReference(ColumnReference<'s>),
    Renamed {
        inner: Box<ValueExpression<'s, 'a>>,
        name: Identifier<'s>,
    },
    PositionalParameterReference,
    SubscriptedExpression,
    FieldSelectionExpression,
    Operator {
        first: Box<ValueExpression<'s, 'a>>,
        second: Box<ValueExpression<'s, 'a>>,
        operator: BinaryOperator,
    },
    FunctionCall(FunctionCall<'s, 'a>),
    AggregateExpression(AggregateExpression),
    WindowFunctionCall,
    TypeCast {
        base: Box<ValueExpression<'s, 'a>>,
        target_ty: DataType,
    },
    CollationExpression,
    ScalarSubquery,
    ArrayConstructor,
    RowConstructor,
    ParenedExpression,
    Placeholder(usize),
    List(Vec<ValueExpression<'s, 'a>>),
    SubQuery(select::Select<'s, 'a>),
    Not(Box<ValueExpression<'s, 'a>>),
    Case {
        matched_value: Box<ValueExpression<'s,'a>>,
        cases: Vec<(Vec<ValueExpression<'s, 'a>>, ValueExpression<'s, 'a>)>,
        else_case: Option<Box<ValueExpression<'s, 'a>>>,
    },
}

impl<'s, 'a> ValueExpression<'s, 'a> {
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
}

impl<'i, 'a> CompatibleParser for Vec<ValueExpression<'i, 'a>> {
    type StaticVersion = Vec<ValueExpression<'static, 'static>>;

    fn to_static(&self) -> Self::StaticVersion {
        self.iter().map(|p| p.to_static()).collect()
    }

    fn parameter_count(&self) -> usize {
        self.iter().map(|p| p.parameter_count()).max().unwrap_or(0)
    }
}

impl<'i, 'a> CompatibleParser for crate::arenas::Vec<'a, ValueExpression<'i, 'a>> {
    type StaticVersion = crate::arenas::Vec<'static,ValueExpression<'static, 'static>>;

    fn to_static(&self) -> Self::StaticVersion {
        crate::arenas::Vec::Heap(self.iter().map(|p| p.to_static()).collect())
    }

    fn parameter_count(&self) -> usize {
        self.iter().map(|p| p.parameter_count()).max().unwrap_or(0)
    }
}

impl<'i, 'a> CompatibleParser for ValueExpression<'i, 'a> {
    type StaticVersion = ValueExpression<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
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
                first: Box::new(first.to_static()),
                second: Box::new(second.to_static()),
                operator: operator.clone(),
            },
            Self::FunctionCall(fc) => ValueExpression::FunctionCall(fc.to_static()),
            Self::AggregateExpression(ae) => ValueExpression::AggregateExpression(ae.to_static()),
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

    fn parameter_count(&self) -> usize {
        match self {
            Self::Literal(_) => 0,
            Self::All => 0,
            Self::AllFromRelation { .. } => 0,
            Self::Null => 0,
            Self::ColumnReference(_) => 0,
            Self::Renamed { inner, .. } => inner.parameter_count(),
            Self::PositionalParameterReference => 0,
            Self::SubscriptedExpression => 0,
            Self::FieldSelectionExpression => 0,
            Self::Operator { first, second, .. } => {
                core::cmp::max(first.parameter_count(), second.parameter_count())
            }
            Self::FunctionCall(fc) => fc.parameter_count(),
            Self::AggregateExpression(ae) => match ae {
                AggregateExpression::Count(c) => c.parameter_count(),
                AggregateExpression::Sum(s) => s.parameter_count(),
                AggregateExpression::AnyValue(av) => av.parameter_count(),
                AggregateExpression::Max(v) => v.parameter_count(),
            },
            Self::WindowFunctionCall => 0,
            Self::TypeCast { base, .. } => base.parameter_count(),
            Self::CollationExpression => 0,
            Self::ScalarSubquery => 0,
            Self::ArrayConstructor => 0,
            Self::RowConstructor => 0,
            Self::ParenedExpression => 0,
            Self::Placeholder(p) => *p,
            Self::List(parts) => parts.iter().map(|p| p.parameter_count()).max().unwrap_or(0),
            Self::SubQuery(s) => s.parameter_count(),
            Self::Not(v) => v.parameter_count(),
            Self::Case {
                matched_value,
                cases,
                else_case,
            } => core::iter::once(matched_value.parameter_count())
                .chain(cases.iter().flat_map(|(targets, value)| {
                    targets
                        .iter()
                        .map(|t| t.parameter_count())
                        .chain(core::iter::once(value.parameter_count()))
                }))
                .chain(else_case.iter().map(|c| c.parameter_count()))
                .max()
                .unwrap_or(0),
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for ValueExpression<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            value_expression(i, a)
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for Vec<ValueExpression<'i, 'a>> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            nom::multi::separated_list1(
                nom::bytes::complete::tag(","),
                nom::sequence::delimited(
                    nom::character::complete::multispace0,
                    ValueExpression::parse_arena(a),
                    nom::character::complete::multispace0,
                ),
            )(i)
        }
    }
}


impl<'i, 'a> ArenaParser<'i, 'a> for crate::arenas::Vec<'a, ValueExpression<'i, 'a>> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            crate::nom_util::bump_separated_list1(
                a,
                nom::bytes::complete::tag(","),
                nom::sequence::delimited(
                    nom::character::complete::multispace0,
                    ValueExpression::parse_arena(a),
                    nom::character::complete::multispace0,
                ),
            )(i)
        }
    }
}

#[deprecated]
pub fn value_expression<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], ValueExpression<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    let (remaining, parsed) = nom::branch::alt((
        nom::bytes::complete::tag("*").map(|_| ValueExpression::All),
        nom::bytes::complete::tag_no_case("NULL").map(|_| ValueExpression::Null),
        FunctionCall::parse_arena(arena).map(ValueExpression::FunctionCall),
        AggregateExpression::parse_arena(arena).map(ValueExpression::AggregateExpression),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("NOT"),
            nom::character::complete::multispace1,
            ValueExpression::parse_arena(arena),
        ))
        .map(|(_, _, value)| ValueExpression::Not(Box::new(value))),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("CASE"),
            nom::character::complete::multispace1,
            ValueExpression::parse_arena(arena),
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
                        ValueExpression::parse_arena(arena),
                    ),
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("THEN"),
                    nom::character::complete::multispace1,
                    ValueExpression::parse_arena(arena),
                ))
                .map(|(_, _, _, target_expr, _, _, _, value)| (target_expr, value)),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("ELSE"),
                    nom::character::complete::multispace1,
                    ValueExpression::parse_arena(arena),
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
                select::Select::parse_arena(arena),
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
            nom::sequence::tuple((
                ColumnReference::parse(),
                nom::bytes::complete::tag("::"),
                DataType::parse(),
            )),
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
            ValueExpression::parse_arena(arena),
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
                        ValueExpression::parse_arena(arena),
                    ),
                    nom::bytes::complete::tag(")"),
                )),
                |(_, parts, _)| ValueExpression::List(parts),
            ),
            nom::combinator::map(
                nom::sequence::tuple((
                    nom::bytes::complete::tag("("),
                    nom::character::complete::multispace0,
                    select::Select::parse_arena(arena),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
                |(_, _, subquery, _, _)| ValueExpression::SubQuery(subquery),
            ),
        ))(remaining)?
    } else {
        ValueExpression::parse_arena(arena)(remaining)?
    };

    let result = match second {
        ValueExpression::Renamed { inner, name } => ValueExpression::Renamed {
            inner: Box::new(ValueExpression::Operator {
                first: Box::new(parsed),
                second: Box::new(*inner),
                operator,
            }),
            name,
        },
        other => ValueExpression::Operator {
            first: Box::new(parsed),
            second: Box::new(other),
            operator,
        },
    };

    Ok((remaining, result))
}
