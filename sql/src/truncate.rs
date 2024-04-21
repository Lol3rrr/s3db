use nom::IResult;

use crate::{CompatibleParser, Identifier, Parser as _};

#[derive(Debug, PartialEq)]
pub struct TruncateTable<'s, 'a> {
    pub names: crate::arenas::Vec<'a, Identifier<'s>>,
}

impl<'i, 'a> CompatibleParser for TruncateTable<'i, 'a> {
    type StaticVersion = TruncateTable<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
        TruncateTable {
            names: crate::arenas::Vec::Heap(self.names.iter().map(|n| n.to_static()).collect()),
        }
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

impl<'i, 'a> crate::ArenaParser<'i, 'a> for TruncateTable<'i, 'a> {
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
) -> IResult<&'i [u8], TruncateTable<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    nom::combinator::map(
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("TRUNCATE"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("TABLE"),
            ))),
            nom::character::complete::multispace1,
            crate::nom_util::bump_separated_list1(
                arena,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macros::arena_parser_parse;

    #[test]
    fn truncate_single_table() {
        arena_parser_parse!(
            TruncateTable,
            "truncate table pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers",
            TruncateTable {
                names: vec![
                    "pgbench_accounts".into(),
                    "pgbench_branches".into(),
                    "pgbench_history".into(),
                    "pgbench_tellers".into()
                ]
                .into()
            }
        );
    }
}
