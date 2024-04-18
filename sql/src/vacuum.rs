use nom::IResult;

use crate::{dialects, CompatibleParser, Identifier, Parser as _};

#[derive(Debug, PartialEq)]
pub struct Vacuum {}

impl CompatibleParser<dialects::Postgres> for Vacuum {
    type StaticVersion = Vacuum;

    fn to_static(&self) -> Self::StaticVersion {
        Self {}
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

pub fn parse(i: &[u8]) -> IResult<&[u8], Vacuum, nom::error::VerboseError<&[u8]>> {
    let (rem, _) = nom::bytes::complete::tag_no_case("vacuum")(i)?;

    let (rem, _analyze) = nom::combinator::opt(nom::sequence::tuple((
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("analyze"),
    )))(rem)?;

    let (rem, _table) = nom::combinator::opt(nom::sequence::tuple((
        nom::character::complete::multispace1,
        Identifier::parse(),
    )))(rem)?;

    Ok((rem, Vacuum {}))
}
