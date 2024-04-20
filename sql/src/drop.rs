use nom::{IResult, Parser};

use crate::{Identifier, Parser as _, CompatibleParser};

#[derive(Debug, PartialEq)]
pub struct DropIndex<'s> {
    pub name: Identifier<'s>,
    pub concurrently: bool,
    pub if_exists: bool,
    pub dependent_handling: DependentHandling,
}

#[derive(Debug, PartialEq)]
pub struct DropTable<'s, 'a> {
    pub names: crate::arenas::Vec<'a, Identifier<'s>>,
    pub if_exists: bool,
    pub dependent_handling: DependentHandling,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DependentHandling {
    Restrict,
    Cascade,
}

impl<'s> DropIndex<'s> {
    pub fn to_static(&self) -> DropIndex<'static> {
        DropIndex {
            name: self.name.to_static(),
            concurrently: self.concurrently,
            if_exists: self.if_exists,
            dependent_handling: self.dependent_handling.clone(),
        }
    }
}

impl<'s, 'a> CompatibleParser for DropTable<'s, 'a> {
    type StaticVersion = DropTable<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
        DropTable {
            names: crate::arenas::Vec::Heap(self.names.iter().map(|n| n.to_static()).collect()),
            if_exists: self.if_exists,
            dependent_handling: self.dependent_handling.clone(),
        }
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

impl<'i, 's> crate::Parser<'i> for DropIndex<'s>
where
    'i: 's,
{
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            drop_index(i)
        }
    }
}

#[deprecated]
pub fn drop_index(i: &[u8]) -> IResult<&[u8], DropIndex<'_>, nom::error::VerboseError<&[u8]>> {
    nom::combinator::map(
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("DROP"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("INDEX"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("CONCURRENTLY"),
            ))),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("IF"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("EXISTS"),
            ))),
            nom::character::complete::multispace1,
            Identifier::parse(),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::branch::alt((
                        nom::bytes::complete::tag_no_case("CASCADE")
                            .map(|_| DependentHandling::Cascade),
                        nom::bytes::complete::tag_no_case("RESTRICT")
                            .map(|_| DependentHandling::Restrict),
                    )),
                ))
                .map(|(_, d)| d),
            ),
        )),
        |(_, _, _, concurrently, if_exists, _, index_name, dependent_handling)| DropIndex {
            name: index_name,
            concurrently: concurrently.is_some(),
            if_exists: if_exists.is_some(),
            dependent_handling: dependent_handling.unwrap_or(DependentHandling::Restrict),
        },
    )(i)
}

impl<'i, 'a> crate::ArenaParser<'i, 'a> for DropTable<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            drop_table(i, a)
        }
    }
}

#[deprecated]
pub fn drop_table<'i, 'a>(i: &'i [u8], arena: &'a bumpalo::Bump) -> IResult<&'i [u8], DropTable<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    nom::combinator::map(
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("DROP"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TABLE"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("IF"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("EXISTS"),
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
            nom::combinator::opt(nom::branch::alt((
                nom::bytes::complete::tag_no_case("RESTRICT").map(|_| DependentHandling::Restrict),
                nom::bytes::complete::tag_no_case("CASCADE").map(|_| DependentHandling::Cascade),
            ))),
        )),
        |(_, _, _, raw_if_exists, _, names, dependent_handling)| DropTable {
            names,
            if_exists: raw_if_exists.is_some(),
            dependent_handling: dependent_handling.unwrap_or(DependentHandling::Restrict),
        },
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macros::{parser_parse, arena_parser_parse};

    #[test]
    fn drop_index_basic() {
        parser_parse!(
            DropIndex,
            "DROP INDEX \"UQE_user_login\" CASCADE",
            DropIndex {
                name: Identifier("UQE_user_login".into()),
                concurrently: false,
                if_exists: false,
                dependent_handling: DependentHandling::Cascade,
            }
        );
    }

    #[test]
    fn drop_table_basic() {
        arena_parser_parse!(
            DropTable,
            "DROP TABLE \"testing\"",
            DropTable {
                names: vec![Identifier("testing".into())].into(),
                if_exists: false,
                dependent_handling: DependentHandling::Restrict,
            }
        );
    }
}
