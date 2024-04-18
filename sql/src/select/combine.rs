use nom::{IResult, Parser};

#[derive(Debug, PartialEq, Clone)]
pub enum Combination {
    Union,
    Intersection,
    Except,
}

impl<'i> crate::Parser<'i> for Combination {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            combine(i)
        }
    }
}

#[deprecated]
pub fn combine(i: &[u8]) -> IResult<&[u8], Combination, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("UNION"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ALL"),
        ))
        .map(|_| Combination::Union),
        nom::bytes::complete::tag_no_case("UNION").map(|_| Combination::Union),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("INTERSECT"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ALL"),
        ))
        .map(|_| Combination::Intersection),
        nom::bytes::complete::tag_no_case("INTERSECT").map(|_| Combination::Intersection),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("EXCEPT"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ALL"),
        ))
        .map(|_| Combination::Except),
        nom::bytes::complete::tag_no_case("EXCEPT").map(|_| Combination::Except),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::macros::parser_parse;

    #[test]
    fn basic_combine() {
        parser_parse!(Combination, "UNION", Combination::Union);

        parser_parse!(Combination, "UNION ALL", Combination::Union);

        parser_parse!(Combination, "INTERSECT", Combination::Intersection);

        parser_parse!(Combination, "EXCEPT", Combination::Except);
    }
}
