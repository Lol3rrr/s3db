use nom::{IResult, Parser as _};

use crate::{Identifier, Literal, Parser};

#[derive(Debug, PartialEq, Clone)]
pub enum TypeModifier {
    NotNull,
    PrimaryKey,
    Null,
    DefaultValue { value: Option<Literal<'static>> },
    Collate { collation: String },
    Sequence { name: String },
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
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                Literal::parse(),
            ))),
        ))
        .map(|(_, value)| TypeModifier::DefaultValue {
            value: value.map(|(_, v)| v.to_static()),
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
    use super::*;

    use crate::macros::parser_parse;

    #[test]
    fn primary_key_modifier() {
        parser_parse!(TypeModifier, "Primary Key", TypeModifier::PrimaryKey);
    }

    #[test]
    fn non_null_modifier() {
        parser_parse!(TypeModifier, "Not Null", TypeModifier::NotNull);
    }

    #[test]
    fn default_modifier() {
        parser_parse!(
            TypeModifier,
            "DEFAULT",
            TypeModifier::DefaultValue { value: None }
        );
    }

    #[test]
    fn default_value_modifier() {
        parser_parse!(
            TypeModifier,
            "DEFAULT 123",
            TypeModifier::DefaultValue {
                value: Some(Literal::SmallInteger(123))
            }
        );
    }

    #[test]
    fn collate_modifier() {
        parser_parse!(
            TypeModifier,
            "COLLATE \"C\"",
            TypeModifier::Collate {
                collation: "C".into()
            }
        );
    }
}
