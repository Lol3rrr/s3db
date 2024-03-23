use nom::{IResult, Parser};

use super::{common::identifier, Identifier};

#[derive(Debug, PartialEq)]
pub struct DropIndex<'s> {
    pub name: Identifier<'s>,
    pub concurrently: bool,
    pub if_exists: bool,
    pub dependent_handling: DependentHandling,
}

#[derive(Debug, PartialEq)]
pub struct DropTable<'s> {
    pub name: Identifier<'s>,
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

impl<'s> DropTable<'s> {
    pub fn to_static(&self) -> DropTable<'static> {
        DropTable {
            name: self.name.to_static(),
            if_exists: self.if_exists,
            dependent_handling: self.dependent_handling.clone(),
        }
    }
}

pub fn drop_index(i: &[u8]) -> IResult<&[u8], DropIndex<'_>> {
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
            identifier,
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

pub fn drop_table(i: &[u8]) -> IResult<&[u8], DropTable<'_>> {
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
            identifier,
            nom::combinator::opt(nom::branch::alt((
                nom::bytes::complete::tag_no_case("RESTRICT").map(|_| DependentHandling::Restrict),
                nom::bytes::complete::tag_no_case("CASCADE").map(|_| DependentHandling::Cascade),
            ))),
        )),
        |(_, _, _, raw_if_exists, _, name, dependent_handling)| DropTable {
            name,
            if_exists: raw_if_exists.is_some(),
            dependent_handling: dependent_handling.unwrap_or(DependentHandling::Restrict),
        },
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_index_basic() {
        let (remaining, drop_index) =
            drop_index("DROP INDEX \"UQE_user_login\" CASCADE".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            DropIndex {
                name: Identifier("UQE_user_login".into()),
                concurrently: false,
                if_exists: false,
                dependent_handling: DependentHandling::Cascade,
            },
            drop_index
        );
    }

    #[test]
    fn drop_table_basic() {
        let (remaining, table) = drop_table("DROP TABLE \"testing\"".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            DropTable {
                name: Identifier("testing".into()),
                if_exists: false,
                dependent_handling: DependentHandling::Restrict,
            },
            table
        );
    }
}
