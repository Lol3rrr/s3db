use nom::{IResult, Parser};

use crate::{ArenaParser, CompatibleParser, DataType, Identifier, Literal, Parser as _};

/// ## References
/// * [Postgres Docs](https://www.postgresql.org/docs/16/sql-createsequence.html)
#[derive(Debug, PartialEq)]
pub struct CreateSequence<'i> {
    pub name: Identifier<'i>,
    pub if_not_exists: bool,
    pub as_type: Option<DataType>,
    pub increment: Option<Literal<'i>>,
}

impl<'i> CompatibleParser for CreateSequence<'i> {
    type StaticVersion = CreateSequence<'static>;

    fn parameter_count(&self) -> usize {
        0
    }

    fn to_static(&self) -> Self::StaticVersion {
        CreateSequence {
            name: self.name.to_static(),
            if_not_exists: self.if_not_exists,
            as_type: self.as_type.clone(),
            increment: self.increment.as_ref().map(|i| i.to_static()),
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for CreateSequence<'i> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> nom::IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| parse(i, a)
    }
}

fn parse<'i, 'a>(
    i: &'i [u8],
    _arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], CreateSequence<'i>, nom::error::VerboseError<&'i [u8]>> {
    let (i, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("CREATE"),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("SEQUENCE"),
    ))(i)?;

    let (i, if_not_exists) = nom::combinator::opt(nom::sequence::tuple((
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("IF"),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("NOT"),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("EXISTS"),
    )))(i)?;

    let (i, (_, name)) =
        nom::sequence::tuple((nom::character::complete::multispace1, Identifier::parse()))(i)?;

    let (i, as_type) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("AS"),
            nom::character::complete::multispace1,
            DataType::parse(),
        ))
        .map(|(_, _, _, ty)| ty),
    )(i)?;

    let (i, increment) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("INCREMENT"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("BY"),
            ))),
            nom::character::complete::multispace1,
            Literal::parse(),
        ))
        .map(|(_, _, _, _, inc)| inc),
    )(i)?;

    Ok((
        i,
        CreateSequence {
            name,
            if_not_exists: if_not_exists.is_some(),
            as_type,
            increment,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macros::arena_parser_parse;

    #[test]
    fn create_basic_sequence() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing",
            CreateSequence {
                name: "testing".into(),
                if_not_exists: false,
                as_type: None,
                increment: None,
            }
        );
    }

    #[test]
    fn create_sequence_ifnotexists() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE IF NOT EXISTS testing",
            CreateSequence {
                name: "testing".into(),
                if_not_exists: true,
                as_type: None,
                increment: None,
            }
        );
    }

    #[test]
    fn create_sequence_as_datatype() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing AS integer",
            CreateSequence {
                name: "testing".into(),
                if_not_exists: false,
                as_type: Some(DataType::Integer),
                increment: None
            }
        );
    }

    #[test]
    fn create_sequence_custom_increment() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing INCREMENT 2",
            CreateSequence {
                name: "testing".into(),
                if_not_exists: false,
                as_type: None,
                increment: Some(Literal::SmallInteger(2))
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing INCREMENT BY 2",
            CreateSequence {
                name: "testing".into(),
                if_not_exists: false,
                as_type: None,
                increment: Some(Literal::SmallInteger(2))
            }
        );
    }
}
