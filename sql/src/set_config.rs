use nom::{IResult, Parser};

use crate::{Literal, Parser as _};

#[derive(Debug, PartialEq, Clone)]
pub enum Configuration {
    ClientEncoding { target: Literal<'static> },
    ClientMinMessages { value: Literal<'static> },
}

impl<'i> crate::Parser<'i> for Configuration {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            parse(i)
        }
    }
}

#[deprecated]
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
    use crate::macros::parser_parse;

    #[test]
    fn set_encoding() {
        parser_parse!(
            Configuration,
            "set client_encoding to 'UTF8'",
            Configuration::ClientEncoding {
                target: Literal::Str("UTF8".into())
            }
        );
    }

    #[test]
    fn set_client_min_messages() {
        parser_parse!(
            Configuration,
            "set CLIENT_MIN_MESSAGES TO 'ERROR'",
            Configuration::ClientMinMessages {
                value: Literal::Str("ERROR".into())
            }
        );
    }
}
