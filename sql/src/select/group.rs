use nom::{IResult, Parser};

use crate::{
    common::{column_reference, literal},
    ColumnReference, Literal,
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
            column_reference.map(GroupAttribute::ColumnRef),
            nom::combinator::map_res(literal, |lit| match lit {
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

    #[test]
    fn single_attribute() {
        let (remaining, tmp) = parse("group by first".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            vec![GroupAttribute::ColumnRef(ColumnReference {
                relation: None,
                column: "first".into()
            })],
            tmp
        );
    }

    #[test]
    fn column_index() {
        let (remaining, tmp) = parse("group by 1".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(vec![GroupAttribute::ColumnIndex(1)], tmp);
    }
}
