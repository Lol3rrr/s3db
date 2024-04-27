use nom::{IResult, Parser};

use crate::{CompatibleParser, Parser as _Parser};

use super::common::{DataType, Identifier, TypeModifier};

mod table;
pub use table::CreateTable;

mod sequence;
pub use sequence::CreateSequence;

#[derive(Debug, PartialEq)]
pub struct CreateIndex<'s, 'a> {
    pub identifier: Identifier<'s>,
    pub table: Identifier<'s>,
    pub columns: crate::arenas::Vec<'a, Identifier<'s>>,
    pub unique: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum WithOptions {
    FillFactor(usize),
}

impl<'s, 'a> CompatibleParser for CreateIndex<'s, 'a> {
    type StaticVersion = CreateIndex<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
        CreateIndex {
            identifier: self.identifier.to_static(),
            table: self.table.to_static(),
            columns: crate::arenas::Vec::Heap(self.columns.iter().map(|c| c.to_static()).collect()),
            unique: self.unique,
        }
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

enum ParsedTableField<'s, 'a> {
    Field(TableField<'s, 'a>),
    PrimaryKey(crate::arenas::Vec<'a, Identifier<'s>>),
}

impl<'i, 'a> crate::ArenaParser<'i, 'a> for CreateIndex<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            create_index(i, a)
        }
    }
}

#[deprecated]
pub fn create_index<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], CreateIndex<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    let (remaining, (_, unique, _, _, _, name, _, table, _, _, columns, _)) =
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("CREATE"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("UNIQUE"),
            ))),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("INDEX"),
            nom::character::complete::multispace1,
            Identifier::parse(),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("ON"),
                nom::character::complete::multispace1,
            )),
            Identifier::parse(),
            nom::character::complete::multispace0,
            nom::bytes::complete::tag("("),
            crate::nom_util::bump_separated_list1(
                arena,
                nom::bytes::complete::tag(","),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    Identifier::parse(),
                    nom::character::complete::multispace0,
                ))
                .map(|(_, column, _)| column),
            ),
            nom::bytes::complete::tag(")"),
        ))(i)?;

    let unique_index = unique.is_some();

    Ok((
        remaining,
        CreateIndex {
            identifier: name,
            table,
            columns,
            unique: unique_index,
        },
    ))
}

#[derive(Debug, PartialEq)]
pub struct TableField<'s, 'a> {
    pub ident: Identifier<'s>,
    pub datatype: DataType,
    pub modifiers: crate::arenas::Vec<'a, TypeModifier>,
}

impl<'i, 'a, 's> crate::ArenaParser<'i, 'a> for TableField<'s, 'a>
where
    'i: 's,
{
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| create_field(i, a)
    }
}

impl<'s, 'a> CompatibleParser for TableField<'s, 'a> {
    type StaticVersion = TableField<'static, 'static>;

    fn parameter_count(&self) -> usize {
        0
    }
    fn to_static(&self) -> Self::StaticVersion {
        TableField {
            ident: self.ident.to_static(),
            datatype: self.datatype.clone(),
            modifiers: self.modifiers.clone_to_heap(),
        }
    }
}

fn create_field<'i, 's, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], TableField<'s, 'a>, nom::error::VerboseError<&'i [u8]>>
where
    'i: 's,
{
    nom::combinator::map(
        nom::sequence::tuple((
            Identifier::parse(),
            nom::character::complete::multispace1,
            DataType::parse(),
            crate::nom_util::bump_many0(
                arena,
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    TypeModifier::parse(),
                ))
                .map(|(_, m)| m),
            ),
        )),
        |(i, _, datatype, modifiers)| TableField {
            ident: i,
            datatype,
            modifiers,
        },
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macros::arena_parser_parse;

    #[test]
    fn create_unique_index() {
        arena_parser_parse!(
            CreateIndex,
            "CREATE UNIQUE INDEX \"UQE_user_login\" ON \"user\" (\"login\");",
            CreateIndex {
                identifier: Identifier("UQE_user_login".into()),
                table: Identifier("user".into()),
                columns: vec![Identifier("login".into())].into(),
                unique: true,
            },
            &[b';']
        );
    }

    #[test]
    fn create_normal_index() {
        arena_parser_parse!(
            CreateIndex,
            "CREATE INDEX \"UQE_user_login\" ON \"user\" (\"login\");",
            CreateIndex {
                identifier: Identifier("UQE_user_login".into()),
                table: Identifier("user".into()),
                columns: vec![Identifier("login".into())].into(),
                unique: false,
            },
            &[b';']
        );
    }

    #[test]
    fn field() {
        let arena = bumpalo::Bump::new();
        arena_parser_parse!(
            TableField,
            "\"id\" SERIAL PRIMARY KEY",
            TableField {
                ident: "id".into(),
                datatype: DataType::Serial,
                modifiers: bumpalo::vec![in &arena; TypeModifier::PrimaryKey].into()
            }
        );
    }
}
