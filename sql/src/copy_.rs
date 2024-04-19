use nom::{IResult, Parser};

use crate::{dialects, CompatibleParser, Identifier, Parser as _};

#[derive(Debug, PartialEq)]
pub struct Copy_<'s> {
    pub target: Identifier<'s>,
    pub source: CopyInputSource,
}

#[derive(Debug, PartialEq, Clone)]
pub enum CopyInputSource {
    Stdin,
}

impl<'s> CompatibleParser<dialects::Postgres> for Copy_<'s> {
    type StaticVersion = Copy_<'static>;

    fn to_static(&self) -> Self::StaticVersion {
        Copy_ {
            target: self.target.to_static(),
            source: self.source.clone(),
        }
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

impl<'i, 's> crate::Parser<'i> for Copy_<'s>
where
    'i: 's,
{
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            parse(i)
        }
    }
}

#[deprecated]
pub fn parse(i: &[u8]) -> IResult<&[u8], Copy_<'_>, nom::error::VerboseError<&[u8]>> {
    let (rem, (_, _, name, _, _, _, source)) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("copy"),
        nom::character::complete::multispace1,
        Identifier::parse(),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("from"),
        nom::character::complete::multispace1,
        nom::branch::alt((
            nom::bytes::complete::tag_no_case("stdin").map(|_| CopyInputSource::Stdin),
        )),
    ))(i)?;

    Ok((
        rem,
        Copy_ {
            target: name,
            source,
        },
    ))
}
