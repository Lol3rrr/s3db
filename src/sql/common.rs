use std::borrow::Cow;

use nom::{IResult, Parser};

mod functions;
pub use functions::FunctionCall;

mod literal;
pub use literal::{literal, Literal};

mod datatype;
pub use datatype::{data_type, DataType};

use super::select;

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

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOperator {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Is,
    IsNot,
    In,
    NotIn,
    Concat,
    Multiply,
    Divide,
    Like,
    ILike,
    Add,
    Subtract,
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
            Self::Case {
                matched_value,
                cases,
                else_case,
            } => false,
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

pub fn value_expressions(
    i: &[u8],
) -> IResult<&[u8], Vec<ValueExpression<'_>>, nom::error::VerboseError<&[u8]>> {
    nom::multi::separated_list1(
        nom::bytes::complete::tag(","),
        nom::sequence::delimited(
            nom::character::complete::multispace0,
            value_expression,
            nom::character::complete::multispace0,
        ),
    )(i)
}

pub fn value_expression(
    i: &[u8],
) -> IResult<&[u8], ValueExpression<'_>, nom::error::VerboseError<&[u8]>> {
    let (remaining, parsed) = nom::branch::alt((
        nom::bytes::complete::tag("*").map(|_| ValueExpression::All),
        nom::bytes::complete::tag_no_case("NULL").map(|_| ValueExpression::Null),
        functions::function_call.map(|f| ValueExpression::FunctionCall(f)),
        aggregate.map(|t| ValueExpression::AggregateExpression(t)),
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
                cases: cases,
                else_case: else_case.map(|v| Box::new(v)),
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
                identifier,
                nom::bytes::complete::tag("."),
                nom::bytes::complete::tag("*"),
            )),
            |(relation, _, _)| ValueExpression::AllFromRelation { relation },
        ),
        nom::combinator::map(
            nom::sequence::tuple((column_reference, nom::bytes::complete::tag("::"), data_type)),
            |(cr, _, ty)| ValueExpression::TypeCast {
                base: Box::new(ValueExpression::ColumnReference(cr)),
                target_ty: ty,
            },
        ),
        literal.map(|lit| ValueExpression::Literal(lit)),
        nom::combinator::map(column_reference, |t| ValueExpression::ColumnReference(t)),
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
        identifier,
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
        nom::branch::alt((
            nom::bytes::complete::tag("=").map(|_| BinaryOperator::Equal),
            nom::bytes::complete::tag("<>").map(|_| BinaryOperator::NotEqual),
            nom::bytes::complete::tag("!=").map(|_| BinaryOperator::NotEqual),
            nom::bytes::complete::tag(">=").map(|_| BinaryOperator::GreaterEqual),
            nom::bytes::complete::tag(">").map(|_| BinaryOperator::Greater),
            nom::bytes::complete::tag("<=").map(|_| BinaryOperator::LessEqual),
            nom::bytes::complete::tag("<").map(|_| BinaryOperator::Less),
            nom::bytes::complete::tag_no_case("in").map(|_| BinaryOperator::In),
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("not"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("in"),
            ))
            .map(|_| BinaryOperator::NotIn),
            nom::bytes::complete::tag("||").map(|_| BinaryOperator::Concat),
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("IS"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("NOT"),
            ))
            .map(|_| BinaryOperator::IsNot),
            nom::bytes::complete::tag_no_case("IS").map(|_| BinaryOperator::Is),
            nom::bytes::complete::tag("*").map(|_| BinaryOperator::Multiply),
            nom::bytes::complete::tag("/").map(|_| BinaryOperator::Divide),
            nom::bytes::complete::tag("+").map(|_| BinaryOperator::Add),
            nom::bytes::complete::tag("-").map(|_| BinaryOperator::Subtract),
            nom::bytes::complete::tag_no_case("LIKE").map(|_| BinaryOperator::Like),
            nom::bytes::complete::tag_no_case("ILIKE").map(|_| BinaryOperator::ILike),
        )),
        nom::character::complete::multispace0,
    ))(remaining);
    let (remaining, (_, _, operator, _)) = match tmp {
        Ok(r) => r,
        Err(e) => {
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

    Ok((
        remaining,
        ValueExpression::Operator {
            first: Box::new(parsed.to_static()),
            second: Box::new(second.to_static()),
            operator,
        },
    ))
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Identifier<'s>(pub Cow<'s, str>);

impl<'s> Identifier<'s> {
    pub fn to_static(&self) -> Identifier<'static> {
        Identifier(match &self.0 {
            Cow::Owned(o) => Cow::Owned(o.clone()),
            Cow::Borrowed(b) => Cow::Owned((*b).to_owned()),
        })
    }
}

impl<'s> From<&'s str> for Identifier<'s> {
    fn from(value: &'s str) -> Self {
        Self(Cow::Borrowed(value))
    }
}

pub fn identifier(i: &[u8]) -> IResult<&[u8], Identifier<'_>, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag("\""),
                nom::sequence::tuple((
                    nom::bytes::complete::take_while1(|b| (b as char).is_alphabetic() || b == b'_'),
                    nom::bytes::complete::take_while(|b| {
                        (b as char).is_alphanumeric() || b == b'_'
                    }),
                ))
                .map(|(first, second)| {
                    let tmp = [first, second].concat();
                    Identifier(String::from_utf8(tmp).unwrap().into())
                }),
                nom::bytes::complete::tag("\""),
            )),
            |(_, tmp, _)| tmp,
        ),
        nom::sequence::tuple((
            nom::bytes::complete::take_while1(|b| (b as char).is_alphabetic() || b == b'_'),
            nom::bytes::complete::take_while(|b| (b as char).is_alphanumeric() || b == b'_'),
        ))
        .map(|(first, second)| {
            let tmp = [first, second].concat();
            Identifier(String::from_utf8(tmp).unwrap().into())
        }),
    ))(i)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ColumnReference<'s> {
    pub relation: Option<Identifier<'s>>,
    pub column: Identifier<'s>,
}

impl<'s> ColumnReference<'s> {
    pub fn to_static(&self) -> ColumnReference<'static> {
        ColumnReference {
            relation: self.relation.as_ref().map(|r| r.to_static()),
            column: self.column.to_static(),
        }
    }
}

pub fn column_reference(
    i: &[u8],
) -> IResult<&[u8], ColumnReference<'_>, nom::error::VerboseError<&[u8]>> {
    nom::combinator::map(
        nom::sequence::tuple((
            nom::combinator::opt(nom::sequence::tuple((
                identifier,
                nom::bytes::complete::tag("."),
            ))),
            identifier,
        )),
        |(first, second)| ColumnReference {
            relation: first.map(|(n, _)| n),
            column: second,
        },
    )(i)
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateExpression {
    Count(Box<ValueExpression<'static>>),
    Sum(Box<ValueExpression<'static>>),
    AnyValue(Box<ValueExpression<'static>>),
    Max(Box<ValueExpression<'static>>),
}

fn aggregate(i: &[u8]) -> IResult<&[u8], AggregateExpression, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("count"),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                value_expression,
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
            |exp| AggregateExpression::Count(Box::new(exp.to_static())),
        ),
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("sum"),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                value_expression,
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
            |exp| AggregateExpression::Sum(Box::new(exp.to_static())),
        ),
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("any_value"),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                value_expression,
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
            |exp| AggregateExpression::AnyValue(Box::new(exp.to_static())),
        ),
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("max"),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                value_expression,
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
            |exp| AggregateExpression::Max(Box::new(exp.to_static())),
        ),
    ))(i)
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypeModifier {
    NotNull,
    PrimaryKey,
    Null,
    DefaultValue { value: Option<Literal<'static>> },
    Collate { collation: String },
}

pub fn type_modifier(i: &[u8]) -> IResult<&[u8], TypeModifier, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("PRIMARY"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("KEY"),
        ))
        .map(|_| TypeModifier::PrimaryKey),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("NOT"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("NULL"),
        ))
        .map(|_| TypeModifier::NotNull),
        nom::bytes::complete::tag_no_case("NULL").map(|_| TypeModifier::Null),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("DEFAULT"),
            nom::character::complete::multispace1,
            nom::combinator::opt(literal),
        ))
        .map(|(_, _, value)| TypeModifier::DefaultValue {
            value: value.map(|v| v.to_static()),
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("COLLATE"),
            nom::character::complete::multispace1,
            identifier,
        ))
        .map(|(_, _, ident)| TypeModifier::Collate {
            collation: ident.0.to_string(),
        }),
    ))(i)
}

#[cfg(test)]
mod tests {
    use select::{Select, TableExpression};

    use crate::sql::Condition;

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn test_identifier() {
        let (_, name) = identifier("testing".as_bytes()).unwrap();
        assert_eq!("testing", name.0);

        let (_, name) = identifier("\"testing\"".as_bytes()).unwrap();
        assert_eq!("testing", name.0);
    }

    #[test]
    fn test_column_reference() {
        let (_, table_column) = column_reference("table.column".as_bytes()).unwrap();
        let (_, column) = column_reference("column".as_bytes()).unwrap();

        assert_eq!(
            ColumnReference {
                relation: Some(Identifier(Cow::Borrowed("table"))),
                column: Identifier(Cow::Borrowed("column")),
            },
            table_column
        );
        assert_eq!(
            ColumnReference {
                relation: None,
                column: Identifier(Cow::Borrowed("column")),
            },
            column
        );
    }

    #[test]
    fn test_expression() {
        let (_, column) = value_expression("testing".as_bytes()).unwrap();
        matches!(column, ValueExpression::ColumnReference(_));
    }

    #[test]
    fn value_in_list() {
        let (remaining, expr) = value_expression("name in ('first', 'second')".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("name".into())
                })),
                second: Box::new(ValueExpression::List(vec![
                    ValueExpression::Literal(Literal::Str("first".into())),
                    ValueExpression::Literal(Literal::Str("second".into()))
                ])),
                operator: BinaryOperator::In
            },
            expr
        );
    }

    #[test]
    fn lpad_expression() {
        let (remaining, expr) = value_expression("lpad('123', 5, '0')".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::FunctionCall(FunctionCall::LPad {
                base: Box::new(ValueExpression::Literal(Literal::Str("123".into()))),
                length: Literal::SmallInteger(5),
                padding: Literal::Str("0".into())
            }),
            expr
        );
    }

    #[test]
    fn type_cast() {
        let (remaining, expr) = value_expression("id::text".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::TypeCast {
                base: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                })),
                target_ty: DataType::Text
            },
            expr
        );
    }

    #[test]
    fn concat_operator() {
        let (remaining, expr) = value_expression("'first' || 'second'".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::Literal(Literal::Str("first".into()))),
                second: Box::new(ValueExpression::Literal(Literal::Str("second".into()))),
                operator: BinaryOperator::Concat
            },
            expr
        );
    }

    #[test]
    fn greater_operator() {
        let (remaining, expr) = value_expression("test > 0".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("test".into())
                })),
                second: Box::new(ValueExpression::Literal(Literal::SmallInteger(0))),
                operator: BinaryOperator::Greater
            },
            expr
        );
    }

    #[test]
    fn operation_in_parenthesis() {
        let (remaining, expr) = value_expression("(epoch * 1000)".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("epoch".into())
                })),
                second: Box::new(ValueExpression::Literal(Literal::SmallInteger(1000))),
                operator: BinaryOperator::Multiply
            },
            expr
        );
    }

    #[test]
    fn type_modifier_testing() {
        let (remaining, collation) = type_modifier("COLLATE \"C\"".as_bytes()).unwrap();
        assert!(
            remaining.is_empty(),
            "{:?}",
            core::str::from_utf8(remaining)
        );
    }

    #[test]
    fn max_aggregate() {
        let (remaining, max) = value_expression("max(testing)".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::AggregateExpression(AggregateExpression::Max(Box::new(
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("testing".into())
                })
            ))),
            max
        );
    }

    #[test]
    fn exists_with_query() {
        let (remaining, tmp) = value_expression(
            "(EXISTS (SELECT 1 FROM alert_rule a WHERE d.uid = a.namespace_uid))".as_bytes(),
        )
        .unwrap();

        assert_eq!(&[] as &[u8], remaining);

        assert_eq!(
            ValueExpression::FunctionCall(FunctionCall::Exists {
                query: Box::new(Select {
                    values: vec![ValueExpression::Literal(Literal::SmallInteger(1))],
                    table: Some(TableExpression::Renamed {
                        inner: Box::new(TableExpression::Relation("alert_rule".into())),
                        name: "a".into()
                    }),
                    where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("d".into()),
                                column: "uid".into()
                            })),
                            second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("a".into()),
                                column: "namespace_uid".into()
                            })),
                            operator: BinaryOperator::Equal
                        }
                    ))])),
                    order_by: None,
                    group_by: None,
                    having: None,
                    limit: None,
                    for_update: None,
                    combine: None
                })
            }),
            tmp
        );
    }

    #[test]
    fn is_not_null_operator() {
        let (remaining, tmp) =
            value_expression("service_account_id IS NOT NULL".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("service_account_id".into())
                })),
                second: Box::new(ValueExpression::Null),
                operator: BinaryOperator::IsNot
            },
            tmp
        );
    }

    #[test]
    fn less_than_equal() {
        let (remaining, tmp) = value_expression("column <= $1".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("column".into())
                })),
                second: Box::new(ValueExpression::Placeholder(1)),
                operator: BinaryOperator::LessEqual,
            },
            tmp
        );
    }

    #[test]
    fn operator_inner() {
        let (remaining, tmp) = value_expression("column = $1 INNER".as_bytes()).unwrap();

        assert_eq!(
            " INNER".as_bytes(),
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("column".into())
                })),
                second: Box::new(ValueExpression::Placeholder(1)),
                operator: BinaryOperator::Equal,
            },
            tmp
        );
    }

    #[test]
    fn something_in_subquery() {
        let (remaining, tmp) =
            value_expression("something IN ( SELECT name FROM users )".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            ValueExpression::Operator {
                first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "something".into(),
                })),
                second: Box::new(ValueExpression::SubQuery(Select {
                    values: vec![ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "name".into(),
                    })],
                    table: Some(TableExpression::Relation("users".into())),
                    where_condition: None,
                    order_by: None,
                    group_by: None,
                    having: None,
                    limit: None,
                    for_update: None,
                    combine: None
                })),
                operator: BinaryOperator::In,
            },
            tmp
        );
    }

    #[test]
    fn case_when_then() {
        let (remaining, tmp) = value_expression(
            "CASE field WHEN 'first' THEN 123 WHEN 'second' THEN 234 ELSE 0 END".as_bytes(),
        )
        .unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            ValueExpression::Case {
                matched_value: Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "field".into()
                })),
                cases: vec![
                    (
                        vec![ValueExpression::Literal(Literal::Str("first".into()))],
                        ValueExpression::Literal(Literal::SmallInteger(123))
                    ),
                    (
                        vec![ValueExpression::Literal(Literal::Str("second".into()))],
                        ValueExpression::Literal(Literal::SmallInteger(234))
                    )
                ],
                else_case: Some(Box::new(ValueExpression::Literal(Literal::SmallInteger(0)))),
            },
            tmp
        );
    }
}
