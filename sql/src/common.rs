use std::borrow::Cow;

use nom::{IResult, Parser};

mod functions;
pub use functions::FunctionCall;

mod literal;
pub use literal::Literal;

mod datatype;
pub use datatype::DataType;

mod value;
pub use value::ValueExpression;

use crate::Parser as _Parser;

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

impl<'i> crate::Parser<'i> for BinaryOperator {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
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
            ))(i)
        }
    }
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

impl<'i, 's> crate::Parser<'i> for Identifier<'s> where 'i: 's {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            nom::branch::alt((
                nom::combinator::map(
                    nom::sequence::tuple((
                        nom::bytes::complete::tag("\""),
                        nom::combinator::consumed(nom::sequence::tuple((
                            nom::bytes::complete::take_while1(|b| (b as char).is_alphabetic() || b == b'_'),
                            nom::bytes::complete::take_while(|b| {
                                (b as char).is_alphanumeric() || b == b'_'
                            }),
                        )))
                        .map(|(content, _)| {
                            Identifier(Cow::Borrowed(core::str::from_utf8(content).unwrap()))
                        }),
                        nom::bytes::complete::tag("\""),
                    )),
                    |(_, tmp, _)| tmp,
                ),
                nom::combinator::complete(nom::sequence::tuple((
                    nom::bytes::complete::take_while1(|b| (b as char).is_alphabetic() || b == b'_'),
                    nom::bytes::complete::take_while(|b| (b as char).is_alphanumeric() || b == b'_'),
                )))
                .map(|(content, _)| Identifier(Cow::Borrowed(core::str::from_utf8(content).unwrap()))),
            ))(i)
        }
    }
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

impl<'i, 's> crate::Parser<'i> for ColumnReference<'s> where 'i: 's {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            nom::combinator::map(
                nom::sequence::tuple((
                    nom::combinator::opt(nom::sequence::tuple((
                        Identifier::parse(),
                        nom::bytes::complete::tag("."),
                    ))),
                    Identifier::parse(),
                )),
                |(first, second)| ColumnReference {
                    relation: first.map(|(n, _)| n),
                    column: second,
                },
            )(i)
        }
    }
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
                    nom::branch::alt((
                        nom::bytes::complete::tag_no_case("count"),
                        nom::bytes::complete::tag_no_case("pg_catalog.count"),
                    )),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                ValueExpression::parse(),
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
                ValueExpression::parse(),
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
                ValueExpression::parse(),
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
                ValueExpression::parse(),
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

impl<'i> crate::Parser<'i> for TypeModifier {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            type_modifier(i)
        }
    }
}

#[deprecated]
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
            nom::combinator::opt(Literal::parse()),
        ))
        .map(|(_, _, value)| TypeModifier::DefaultValue {
            value: value.map(|v| v.to_static()),
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("COLLATE"),
            nom::character::complete::multispace1,
            Identifier::parse(),
        ))
        .map(|(_, _, ident)| TypeModifier::Collate {
            collation: ident.0.to_string(),
        }),
    ))(i)
}

#[cfg(test)]
mod tests {
    use crate::select::{Select, TableExpression};

    use crate::{Condition, macros::parser_parse};

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn test_identifier() {
        parser_parse!(Identifier, "testing", Identifier(Cow::Borrowed("testing")));

        parser_parse!(Identifier, "\"testing\"", Identifier(Cow::Borrowed("testing")));
    }

    #[test]
    fn test_column_reference() {
        parser_parse!(ColumnReference, "table.column", ColumnReference {
                relation: Some(Identifier(Cow::Borrowed("table"))),
                column: Identifier(Cow::Borrowed("column")),
            });

        parser_parse!(ColumnReference, "column", ColumnReference {
                relation: None,
                column: Identifier(Cow::Borrowed("column")),
            });
    }

    #[test]
    fn test_expression() {
        let (_, column) = ValueExpression::parse()("testing".as_bytes()).unwrap();
        matches!(column, ValueExpression::ColumnReference(_));
    }

    #[test]
    fn value_in_list() {
        let (remaining, expr) = ValueExpression::parse()("name in ('first', 'second')".as_bytes()).unwrap();
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
        let (remaining, expr) = ValueExpression::parse()("lpad('123', 5, '0')".as_bytes()).unwrap();

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
        let (remaining, expr) = ValueExpression::parse()("id::text".as_bytes()).unwrap();

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
        let (remaining, expr) = ValueExpression::parse()("'first' || 'second'".as_bytes()).unwrap();

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
        let (remaining, expr) = ValueExpression::parse()("test > 0".as_bytes()).unwrap();

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
        let (remaining, expr) = ValueExpression::parse()("(epoch * 1000)".as_bytes()).unwrap();

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
        parser_parse!(TypeModifier, "COLLATE \"C\"", TypeModifier::Collate { collation: "C".into() });
    }

    #[test]
    fn max_aggregate() {
        let (remaining, max) = ValueExpression::parse()("max(testing)".as_bytes()).unwrap();

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
        let (remaining, tmp) = ValueExpression::parse()(
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
                        name: "a".into(),
                        column_rename: None,
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
            ValueExpression::parse()("service_account_id IS NOT NULL".as_bytes()).unwrap();

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
        let (remaining, tmp) = ValueExpression::parse()("column <= $1".as_bytes()).unwrap();

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
        let (remaining, tmp) = ValueExpression::parse()("column = $1 INNER".as_bytes()).unwrap();

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
            ValueExpression::parse()("something IN ( SELECT name FROM users )".as_bytes()).unwrap();
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
        let (remaining, tmp) = ValueExpression::parse()(
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

    #[test]
    fn current_timestamp() {
        let (remaining, tmp) = ValueExpression::parse()("CURRENT_TIMESTAMP".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            ValueExpression::FunctionCall(FunctionCall::CurrentTimestamp),
            tmp
        );
    }

    #[test]
    fn longer_renamed_expr() {
        let (remaining, tmp) = ValueExpression::parse()("(3*4) + 5 AS testing".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            ValueExpression::Renamed {
                inner: Box::new(ValueExpression::Operator {
                    first: Box::new(ValueExpression::Operator {
                        first: Box::new(ValueExpression::Literal(Literal::SmallInteger(3))),
                        second: Box::new(ValueExpression::Literal(Literal::SmallInteger(4))),
                        operator: BinaryOperator::Multiply,
                    }),
                    second: Box::new(ValueExpression::Literal(Literal::SmallInteger(5))),
                    operator: BinaryOperator::Add
                }),
                name: "testing".into()
            },
            tmp
        );
    }
}
