use nom::{IResult, Parser};

use crate::{ColumnReference, CompatibleParser, Literal, Parser as _};

#[derive(Debug, PartialEq, Clone)]
pub enum GroupAttribute<'s> {
    ColumnRef(ColumnReference<'s>),
    ColumnIndex(usize),
}

impl<'i> CompatibleParser for GroupAttribute<'i> {
    type StaticVersion = GroupAttribute<'static>;

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::ColumnRef(c) => GroupAttribute::ColumnRef(c.to_static()),
            Self::ColumnIndex(i) => GroupAttribute::ColumnIndex(*i),
        }
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

impl<'i, 'a> crate::ArenaParser<'i, 'a> for crate::arenas::Vec<'a, GroupAttribute<'i>> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            parse(i, a)
        }
    }
}

#[deprecated]
pub fn parse<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], crate::arenas::Vec<'a, GroupAttribute<'i>>, nom::error::VerboseError<&'i [u8]>>
{
    let (rem, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("GROUP"),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("BY"),
        nom::character::complete::multispace1,
    ))(i)?;

    let (rem, tmp) = crate::nom_util::bump_separated_list1(
        arena,
        nom::sequence::tuple((
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(","),
            nom::character::complete::multispace0,
        )),
        nom::branch::alt((
            ColumnReference::parse().map(GroupAttribute::ColumnRef),
            nom::combinator::map_res(Literal::parse(), |lit| match lit {
                Literal::SmallInteger(v) => Ok(GroupAttribute::ColumnIndex(v as usize)),
                Literal::Integer(v) => Ok(GroupAttribute::ColumnIndex(v as usize)),
                Literal::BigInteger(v) => Ok(GroupAttribute::ColumnIndex(v as usize)),
                _ => Err(()),
            }),
        )),
    )(rem)?;

    Ok((rem, tmp))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::macros::arena_parser_parse;

    #[test]
    fn single_attribute() {
        arena_parser_parse!(
            crate::arenas::Vec<'_, GroupAttribute<'_>>,
            "group by first",
            vec![GroupAttribute::ColumnRef(ColumnReference {
                relation: None,
                column: "first".into()
            })]
        );
    }

    #[test]
    fn column_index() {
        arena_parser_parse!(
            crate::arenas::Vec<'_, GroupAttribute<'_>>,
            "group by 1",
            vec![GroupAttribute::ColumnIndex(1)]
        );
    }
}
