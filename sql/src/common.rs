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

mod aggregates;
pub use aggregates::AggregateExpression;

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

impl<'i, 's> crate::Parser<'i> for Identifier<'s>
where
    'i: 's,
{
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            nom::branch::alt((
                nom::combinator::map(
                    nom::sequence::tuple((
                        nom::bytes::complete::tag("\""),
                        nom::combinator::consumed(nom::sequence::tuple((
                            nom::bytes::complete::take_while1(|b| {
                                (b as char).is_alphabetic() || b == b'_'
                            }),
                            nom::bytes::complete::take_while(|b| {
                                (b as char).is_alphanumeric() || b == b'_'
                            }),
                        )))
                        .map(|(content, _)| {
                            Identifier(Cow::Borrowed(core::str::from_utf8(content).expect(
                                "We know that the bytes are alphanumeric and valid charactecrs",
                            )))
                        }),
                        nom::bytes::complete::tag("\""),
                    )),
                    |(_, tmp, _)| tmp,
                ),
                nom::combinator::complete(nom::sequence::tuple((
                    nom::bytes::complete::take_while1(|b| (b as char).is_alphabetic() || b == b'_'),
                    nom::bytes::complete::take_while(|b| {
                        (b as char).is_alphanumeric() || b == b'_'
                    }),
                )))
                .map(|(content, _)| {
                    Identifier(Cow::Borrowed(core::str::from_utf8(content).expect(
                        "We know that the bytes are alphanumeric and valid charactecrs",
                    )))
                }),
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

impl<'i, 's> crate::Parser<'i> for ColumnReference<'s>
where
    'i: 's,
{
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
pub enum TypeModifier {
    NotNull,
    PrimaryKey,
    Null,
    DefaultValue { value: Option<Literal<'static>> },
    Collate { collation: String },
    Sequence { name: String, },
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
    use crate::macros::parser_parse;

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn test_identifier() {
        parser_parse!(Identifier, "testing", Identifier(Cow::Borrowed("testing")));

        parser_parse!(
            Identifier,
            "\"testing\"",
            Identifier(Cow::Borrowed("testing"))
        );
    }

    #[test]
    fn test_column_reference() {
        parser_parse!(
            ColumnReference,
            "table.column",
            ColumnReference {
                relation: Some(Identifier(Cow::Borrowed("table"))),
                column: Identifier(Cow::Borrowed("column")),
            }
        );

        parser_parse!(
            ColumnReference,
            "column",
            ColumnReference {
                relation: None,
                column: Identifier(Cow::Borrowed("column")),
            }
        );
    }

    #[test]
    fn type_modifier_testing() {
        parser_parse!(
            TypeModifier,
            "COLLATE \"C\"",
            TypeModifier::Collate {
                collation: "C".into()
            }
        );
    }
}
