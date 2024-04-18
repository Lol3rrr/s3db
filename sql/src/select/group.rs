use nom::{IResult, Parser};

use crate::{
    ColumnReference, Literal,
    Parser as _
};

#[derive(Debug, PartialEq, Clone)]
pub enum GroupAttribute<'s> {
    ColumnRef(ColumnReference<'s>),
    ColumnIndex(usize),
}

impl<'s> GroupAttribute<'s> {
    pub fn to_static(&self) -> GroupAttribute<'static> {
        match self {
            Self::ColumnRef(c) => GroupAttribute::ColumnRef(c.to_static()),
            Self::ColumnIndex(i) => GroupAttribute::ColumnIndex(*i),
        }
    }
}

impl<'i, 's> crate::Parser<'i> for Vec<GroupAttribute<'s>> where 'i: 's {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            parse(i)
        }
    }
}

#[deprecated]
pub fn parse(i: &[u8]) -> IResult<&[u8], Vec<GroupAttribute<'_>>, nom::error::VerboseError<&[u8]>> {
    let (rem, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("GROUP"),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("BY"),
        nom::character::complete::multispace1,
    ))(i)?;

    let (rem, tmp) = nom::multi::separated_list1(
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

    use crate::macros::parser_parse;

    #[test]
    fn single_attribute() {
        parser_parse!(Vec<GroupAttribute<'_>>, "group by first", vec![GroupAttribute::ColumnRef(ColumnReference {
            relation: None,
            column: "first".into()
        })]);
    }

    #[test]
    fn column_index() {
        parser_parse!(Vec<GroupAttribute<'_>>, "group by 1", vec![GroupAttribute::ColumnIndex(1)]);
    }
}
