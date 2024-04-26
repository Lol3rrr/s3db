use nom::{IResult, Parser};

use crate::{
    ArenaParser, ColumnReference, CompatibleParser, DataType, Identifier, Literal, Parser as _,
};

/// ## References
/// * [Postgres Docs](https://www.postgresql.org/docs/16/sql-createsequence.html)
#[derive(Debug, PartialEq)]
pub struct CreateSequence<'i> {
    pub name: Identifier<'i>,
    pub temporary: bool,
    pub unlogged: bool,
    pub if_not_exists: bool,
    pub as_type: Option<DataType>,
    pub increment: Option<Literal<'i>>,
    pub min_value: Option<Literal<'i>>,
    pub max_value: Option<Literal<'i>>,
    pub start: Option<Literal<'i>>,
    pub cache: Option<Literal<'i>>,
    pub cycle: bool,
    pub owner: Option<ColumnReference<'i>>,
}

impl<'i> CompatibleParser for CreateSequence<'i> {
    type StaticVersion = CreateSequence<'static>;

    fn parameter_count(&self) -> usize {
        0
    }

    fn to_static(&self) -> Self::StaticVersion {
        CreateSequence {
            name: self.name.to_static(),
            temporary: self.temporary,
            unlogged: self.unlogged,
            if_not_exists: self.if_not_exists,
            as_type: self.as_type.clone(),
            increment: self.increment.as_ref().map(|i| i.to_static()),
            min_value: self.min_value.as_ref().map(|i| i.to_static()),
            max_value: self.max_value.as_ref().map(|i| i.to_static()),
            start: self.start.as_ref().map(|i| i.to_static()),
            cache: self.cache.as_ref().map(|i| i.to_static()),
            cycle: self.cycle,
            owner: self.owner.as_ref().map(|i| i.to_static()),
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

fn parse<'i>(
    i: &'i [u8],
    _arena: &'_ bumpalo::Bump,
) -> IResult<&'i [u8], CreateSequence<'i>, nom::error::VerboseError<&'i [u8]>> {
    let (i, (_, temporary, unlogged, _, _)) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("CREATE"),
        nom::combinator::opt(nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::branch::alt((
                nom::bytes::complete::tag_no_case("TEMPORARY"),
                nom::bytes::complete::tag_no_case("TEMP"),
            )),
        ))),
        nom::combinator::opt(nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("UNLOGGED"),
        ))),
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

    let (i, min_value) = nom::branch::alt((
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("NO"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("MINVALUE"),
        ))
        .map(|_| None),
        nom::combinator::opt(
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("MINVALUE"),
                nom::character::complete::multispace1,
                Literal::parse(),
            ))
            .map(|(_, _, _, lit)| lit),
        ),
    ))(i)?;

    let (i, max_value) = nom::branch::alt((
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("NO"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("MAXVALUE"),
        ))
        .map(|_| None),
        nom::combinator::opt(
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("MAXVALUE"),
                nom::character::complete::multispace1,
                Literal::parse(),
            ))
            .map(|(_, _, _, lit)| lit),
        ),
    ))(i)?;

    let (i, start) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("START"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("WITH"),
            ))),
            nom::character::complete::multispace1,
            Literal::parse(),
        ))
        .map(|(_, _, _, _, lit)| lit),
    )(i)?;

    let (i, cache) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("CACHE"),
            nom::character::complete::multispace1,
            Literal::parse(),
        ))
        .map(|(_, _, _, lit)| lit),
    )(i)?;

    let (i, cycle) = nom::combinator::opt(nom::branch::alt((
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("NO"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("CYCLE"),
        ))
        .map(|_| false),
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("CYCLE"),
        ))
        .map(|_| true),
    )))(i)?;

    let (i, owner) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("OWNED"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("BY"),
            nom::character::complete::multispace1,
            nom::branch::alt((
                nom::bytes::complete::tag_no_case("NONE").map(|_| None),
                ColumnReference::parse().map(|c| Some(c)),
            )),
        ))
        .map(|(_, _, _, _, _, owner)| owner),
    )(i)?;

    Ok((
        i,
        CreateSequence {
            name,
            unlogged: unlogged.is_some(),
            temporary: temporary.is_some(),
            if_not_exists: if_not_exists.is_some(),
            as_type,
            increment,
            min_value,
            max_value,
            start,
            cache,
            cycle: cycle.unwrap_or(false),
            owner: owner.flatten(),
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
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_unlogged_sequence() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE UNLOGGED SEQUENCE testing",
            CreateSequence {
                name: "testing".into(),
                unlogged: true,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_temporar_sequence() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE TEMPORARY SEQUENCE testing",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: true,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE TEMP SEQUENCE testing",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: true,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
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
                unlogged: false,
                temporary: false,
                if_not_exists: true,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
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
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: Some(DataType::Integer),
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
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
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: Some(Literal::SmallInteger(2)),
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing INCREMENT BY 2",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: Some(Literal::SmallInteger(2)),
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_sequence_min_value() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing MINVALUE 13",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: Some(Literal::SmallInteger(13)),
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing NO MINVALUE",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_sequence_max_value() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing MAXVALUE 13",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: Some(Literal::SmallInteger(13)),
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing NO MAXVALUE",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_sequence_start() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing START 10",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: Some(Literal::SmallInteger(10)),
                cache: None,
                cycle: false,
                owner: None,
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing START WITH 10",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: Some(Literal::SmallInteger(10)),
                cache: None,
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_sequence_cache() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing CACHE 6",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: Some(Literal::SmallInteger(6)),
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_sequence_cycle() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing CYCLE",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: true,
                owner: None,
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing NO CYCLE",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );
    }

    #[test]
    fn create_sequence_owned_by() {
        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing OWNED BY NONE",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: None,
            }
        );

        arena_parser_parse!(
            CreateSequence,
            "CREATE SEQUENCE testing OWNED BY somet.somec",
            CreateSequence {
                name: "testing".into(),
                unlogged: false,
                temporary: false,
                if_not_exists: false,
                as_type: None,
                increment: None,
                min_value: None,
                max_value: None,
                start: None,
                cache: None,
                cycle: false,
                owner: Some(ColumnReference {
                    relation: Some("somet".into()),
                    column: "somec".into(),
                }),
            }
        );
    }
}
