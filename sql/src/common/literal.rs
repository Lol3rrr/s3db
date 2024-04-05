use std::borrow::Cow;

use nom::{IResult, Parser};

use super::DataType;

#[derive(Debug, PartialEq, Clone)]
pub enum Literal<'s> {
    Null,
    Str(Cow<'s, str>),
    Name(Cow<'s, str>),
    SmallInteger(i16),
    Integer(i32),
    BigInteger(i64),
    Bool(bool),
}

impl<'s> Literal<'s> {
    pub fn to_static(&self) -> Literal<'static> {
        match self {
            Self::Null => Literal::Null,
            Self::Str(val) => Literal::Str(Cow::Owned(val.clone().into_owned())),
            Self::Name(val) => Literal::Name(Cow::Owned(val.clone().into_owned())),
            Self::SmallInteger(v) => Literal::SmallInteger(*v),
            Self::Integer(v) => Literal::Integer(*v),
            Self::BigInteger(v) => Literal::BigInteger(*v),
            Self::Bool(b) => Literal::Bool(*b),
        }
    }

    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Str(_) => Some(DataType::Text),
            Self::Name(_) => Some(DataType::Name),
            Self::SmallInteger(_) => Some(DataType::SmallInteger),
            Self::Integer(_) => Some(DataType::Integer),
            Self::BigInteger(_) => Some(DataType::BigInteger),
            Self::Bool(_) => Some(DataType::Bool),
        }
    }
}

pub fn literal(i: &[u8]) -> IResult<&[u8], Literal<'_>, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag("'"),
            nom::bytes::complete::take_until("'"),
            nom::bytes::complete::tag("'"),
        ))
        .map(|(_, val, _)| Literal::Str(Cow::Borrowed(core::str::from_utf8(val).unwrap()))),
        nom::sequence::tuple((
            nom::combinator::opt(nom::bytes::complete::tag("-")),
            nom::character::complete::digit1,
        ))
        .map(|(negative, d)| {
            let raw_digit = core::str::from_utf8(d).unwrap();

            let smallinteger = raw_digit.parse::<i16>();
            let integer = raw_digit.parse::<i32>();
            let biginteger = raw_digit.parse::<i64>();

            match (smallinteger, integer, biginteger) {
                (Ok(v), _, _) if negative.is_some() => Literal::SmallInteger(-v),
                (Ok(v), _, _) => Literal::SmallInteger(v),
                (_, Ok(v), _) if negative.is_some() => Literal::Integer(-v),
                (_, Ok(v), _) => Literal::Integer(v),
                (_, _, Ok(v)) if negative.is_some() => Literal::BigInteger(-v),
                (_, _, Ok(v)) => Literal::BigInteger(v),
                _ => todo!(),
            }
        }),
        nom::combinator::map(nom::bytes::complete::tag_no_case("false"), |_| {
            Literal::Bool(false)
        }),
        nom::combinator::map(nom::bytes::complete::tag_no_case("true"), |_| {
            Literal::Bool(true)
        }),
        nom::combinator::map(nom::bytes::complete::tag_no_case("NULL"), |_| Literal::Null),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_literal() {
        let (remaining, value) = literal("NULL".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(Literal::Null, value);
    }

    #[test]
    fn negative_number() {
        let (remaining, value) = literal("-1".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(Literal::SmallInteger(-1), value);
    }
}
