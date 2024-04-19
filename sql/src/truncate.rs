use nom::IResult;

use crate::{dialects, CompatibleParser, Identifier, Parser as _};

#[derive(Debug, PartialEq)]
pub struct TruncateTable<'s> {
    pub names: Vec<Identifier<'s>>,
}

impl<'i, 's> crate::Parser<'i> for TruncateTable<'s> where 'i: 's {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            parse(i)
        }
    }
}

#[deprecated]
pub fn parse(i: &[u8]) -> IResult<&[u8], TruncateTable<'_>, nom::error::VerboseError<&[u8]>> {
    nom::combinator::map(
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("TRUNCATE"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("TABLE"),
            ))),
            nom::character::complete::multispace1,
            nom::multi::separated_list1(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(","),
                    nom::character::complete::multispace0,
                )),
                Identifier::parse(),
            ),
        )),
        |(_, _, _, tables)| TruncateTable { names: tables },
    )(i)
}

impl<'s> CompatibleParser<dialects::Postgres> for TruncateTable<'s> {
    type StaticVersion = TruncateTable<'static>;

    fn to_static(&self) -> Self::StaticVersion {
        TruncateTable {
            names: self.names.iter().map(|n| n.to_static()).collect(),
        }
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macros::parser_parse;

    #[test]
    fn truncate_single_table() {
        parser_parse!(
            TruncateTable,
            "truncate table pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers",
            TruncateTable {
                names: vec![
                    "pgbench_accounts".into(),
                    "pgbench_branches".into(),
                    "pgbench_history".into(),
                    "pgbench_tellers".into()
                ]
            }
        );
    }
}
