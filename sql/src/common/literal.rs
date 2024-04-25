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

impl<'i, 's> crate::Parser<'i> for Literal<'s>
where
    'i: 's,
{
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            literal(i)
        }
    }
}

#[deprecated]
pub fn literal(i: &[u8]) -> IResult<&[u8], Literal<'_>, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag("'"),
            nom::bytes::complete::take_until("'"),
            nom::bytes::complete::tag("'"),
        ))
        .map(|(_, val, _)| Literal::Str(Cow::Borrowed(core::str::from_utf8(val).expect("TODO")))),
        nom::combinator::map_res(
            nom::sequence::tuple((
                nom::combinator::opt(nom::bytes::complete::tag("-")),
                nom::character::complete::digit1,
            )),
            |(negative, d)| {
                let raw_digit = core::str::from_utf8(d).expect("We know its a valid string, because its only made up of digits which are valid characters");

                let smallinteger = raw_digit.parse::<i16>();
                let integer = raw_digit.parse::<i32>();
                let biginteger = raw_digit.parse::<i64>();

                let tmp = match (smallinteger, integer, biginteger) {
                    (Ok(v), _, _) if negative.is_some() => Literal::SmallInteger(-v),
                    (Ok(v), _, _) => Literal::SmallInteger(v),
                    (_, Ok(v), _) if negative.is_some() => Literal::Integer(-v),
                    (_, Ok(v), _) => Literal::Integer(v),
                    (_, _, Ok(v)) if negative.is_some() => Literal::BigInteger(-v),
                    (_, _, Ok(v)) => Literal::BigInteger(v),
                    other => return Err(other),
                };
                Ok(tmp)
            },
        ),
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
    use crate::macros::parser_parse;

    #[test]
    fn null_literal() {
        parser_parse!(Literal, "NULL", Literal::Null);
    }

    #[test]
    fn negative_number() {
        parser_parse!(Literal, "-1", Literal::SmallInteger(-1));
    }
}
