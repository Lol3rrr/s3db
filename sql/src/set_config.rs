use nom::{IResult, Parser};

use crate::{Parser as _, Literal};

#[derive(Debug, PartialEq, Clone)]
pub enum Configuration {
    ClientEncoding { target: Literal<'static> },
    ClientMinMessages { value: Literal<'static> },
}

pub fn parse(i: &[u8]) -> IResult<&[u8], Configuration, nom::error::VerboseError<&[u8]>> {
    let (i, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("set"),
        nom::character::complete::multispace1,
    ))(i)?;

    nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("client_encoding"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("to"),
            nom::character::complete::multispace1,
            Literal::parse(),
        ))
        .map(|(_, _, _, _, target)| Configuration::ClientEncoding {
            target: target.to_static(),
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("CLIENT_MIN_MESSAGES"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TO"),
            nom::character::complete::multispace1,
            Literal::parse(),
        ))
        .map(|(_, _, _, _, val)| Configuration::ClientMinMessages {
            value: val.to_static(),
        }),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_config {
        ($query:literal, $expected:expr) => {
            let (remaining, tmp) = parse($query.as_bytes()).unwrap();
            assert_eq!(&[] as &[u8], remaining);

            assert_eq!($expected, tmp);
        };
    }

    #[test]
    fn set_encoding() {
        test_config!(
            "set client_encoding to 'UTF8'",
            Configuration::ClientEncoding {
                target: Literal::Str("UTF8".into())
            }
        );
    }

    #[test]
    fn set_client_min_messages() {
        test_config!(
            "set CLIENT_MIN_MESSAGES TO 'ERROR'",
            Configuration::ClientMinMessages {
                value: Literal::Str("ERROR".into())
            }
        );
    }
}
