use nom::{IResult, Parser};

use crate::{ArenaParser as _, CompatibleParser, Literal, Parser as _Parser};

use super::common::{DataType, Identifier, TypeModifier};

#[derive(Debug, PartialEq)]
pub struct CreateTable<'s, 'a> {
    pub identifier: Identifier<'s>,
    pub fields: Vec<TableField<'s, 'a>>,
    pub if_not_exists: bool,
    pub primary_key: Option<Vec<Identifier<'s>>>,
    pub withs: Vec<WithOptions>,
}

#[derive(Debug, PartialEq)]
pub struct CreateIndex<'s> {
    pub identifier: Identifier<'s>,
    pub table: Identifier<'s>,
    pub columns: Vec<Identifier<'s>>,
    pub unique: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum WithOptions {
    FillFactor(usize),
}

impl<'s, 'a> CreateTable<'s, 'a> {
    pub fn to_static(&self) -> CreateTable<'static, 'static> {
        CreateTable {
            identifier: self.identifier.to_static(),
            fields: self.fields.iter().map(|f| f.to_static()).collect(),
            if_not_exists: self.if_not_exists,
            primary_key: self
                .primary_key
                .as_ref()
                .map(|parts| parts.iter().map(|p| p.to_static()).collect()),
            withs: self.withs.clone(),
        }
    }

    pub fn parameter_count(&self) -> usize {
        0
    }
}

impl<'s> CompatibleParser for CreateIndex<'s> {
    type StaticVersion = CreateIndex<'static>;

    fn to_static(&self) -> Self::StaticVersion {
        CreateIndex {
            identifier: self.identifier.to_static(),
            table: self.table.to_static(),
            columns: self.columns.iter().map(|c| c.to_static()).collect(),
            unique: self.unique,
        }
    }

    fn parameter_count(&self) -> usize {
        0
    }
}

enum ParsedTableField<'s, 'a> {
    Field(TableField<'s, 'a>),
    PrimaryKey(Vec<Identifier<'s>>),
}

impl<'i, 's, 'a> crate::ArenaParser<'i, 'a> for CreateTable<'s, 'a>
where
    'i: 's,
{
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            create_table(i, a)
        }
    }
}

impl<'s, 'a> CompatibleParser for CreateTable<'s, 'a> {
    type StaticVersion = CreateTable<'static, 'static>;

    fn parameter_count(&self) -> usize {
        0
    }

    fn to_static(&self) -> Self::StaticVersion {
        todo!()
    }
}

#[deprecated]
pub fn create_table<'i, 's, 'a>(
    i: &'i [u8],
    a: &'a bumpalo::Bump,
) -> IResult<&'i [u8], CreateTable<'s, 'a>, nom::error::VerboseError<&'i [u8]>>
where
    'i: 's,
{
    let (remaining, (_, _, _, if_not_exists, _, table_ident, _, _, _, raw_parts, _, _, with)) =
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("CREATE"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TABLE"),
            nom::combinator::opt(nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("IF NOT EXISTS"),
            ))),
            nom::character::complete::multispace1,
            Identifier::parse(),
            nom::character::complete::multispace0,
            nom::bytes::complete::tag("("),
            nom::character::complete::multispace0,
            nom::multi::separated_list1(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(","),
                    nom::character::complete::multispace0,
                )),
                nom::branch::alt((
                    TableField::parse_arena(a).map(ParsedTableField::Field),
                    nom::sequence::tuple((
                        nom::bytes::complete::tag_no_case("PRIMARY"),
                        nom::character::complete::multispace1,
                        nom::bytes::complete::tag_no_case("KEY"),
                        nom::character::complete::multispace0,
                        nom::bytes::complete::tag("("),
                        nom::character::complete::multispace0,
                        nom::multi::separated_list1(
                            nom::sequence::tuple((
                                nom::character::complete::multispace0,
                                nom::bytes::complete::tag(","),
                                nom::character::complete::multispace0,
                            )),
                            Identifier::parse(),
                        ),
                        nom::character::complete::multispace0,
                        nom::bytes::complete::tag(")"),
                    ))
                    .map(|(_, _, _, _, _, _, parts, _, _)| ParsedTableField::PrimaryKey(parts)),
                )),
            ),
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(")"),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("WITH"),
                    nom::character::complete::multispace1,
                    nom::sequence::delimited(
                        nom::bytes::complete::tag("("),
                        nom::multi::separated_list0(
                            nom::sequence::tuple((
                                nom::character::complete::multispace0,
                                nom::bytes::complete::tag(","),
                                nom::character::complete::multispace0,
                            )),
                            nom::branch::alt((nom::sequence::tuple((
                                nom::bytes::complete::tag("fillfactor"),
                                nom::bytes::complete::tag("="),
                                Literal::parse(),
                            ))
                            .map(|(_, _, val)| {
                                let value = match val {
                                    Literal::SmallInteger(v) => v as usize,
                                    Literal::Integer(v) => v as usize,
                                    Literal::BigInteger(v) => v as usize,
                                    _ => todo!(),
                                };

                                WithOptions::FillFactor(value)
                            }),)),
                        ),
                        nom::bytes::complete::tag(")"),
                    ),
                ))
                .map(|(_, _, _, parts)| parts),
            ),
        ))(i)?;

    let mut fields = Vec::new();
    let mut primary_key = None;

    for part in raw_parts {
        match part {
            ParsedTableField::Field(f) => {
                fields.push(f);
            }
            ParsedTableField::PrimaryKey(pk) => {
                let tmp = primary_key.replace(pk);

                if tmp.is_some() {
                    panic!("Multiple Primary Key definitions")
                }
            }
        };
    }

    Ok((
        remaining,
        CreateTable {
            identifier: table_ident,
            fields,
            if_not_exists: if_not_exists.is_some(),
            primary_key,
            withs: with.unwrap_or(Vec::new()),
        },
    ))
}

impl<'i, 's> crate::Parser<'i> for CreateIndex<'s>
where
    'i: 's,
{
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            create_index(i)
        }
    }
}

#[deprecated]
pub fn create_index(i: &[u8]) -> IResult<&[u8], CreateIndex<'_>, nom::error::VerboseError<&[u8]>> {
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
            nom::multi::separated_list1(
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

impl<'s, 'a> TableField<'s, 'a> {
    pub fn to_static(&self) -> TableField<'static, 'static> {
        TableField {
            ident: self.ident.to_static(),
            datatype: self.datatype.clone(),
            modifiers: self.modifiers.clone_to_heap(),
        }
    }
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
        todo!()
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
    use crate::macros::{arena_parser_parse, parser_parse};

    #[test]
    fn create_table_test() {
        let arena = bumpalo::Bump::new();
        arena_parser_parse!(
            CreateTable,
            "CREATE TABLE IF NOT EXISTS \"migration_log\" (\n\"id\" SERIAL PRIMARY KEY  NOT NULL\n, \"migration_id\" VARCHAR(255) NOT NULL\n, \"sql\" TEXT NOT NULL\n, \"success\" BOOL NOT NULL\n, \"error\" TEXT NOT NULL\n, \"timestamp\" TIMESTAMP NOT NULL\n);",
            CreateTable {
                identifier: "migration_log".into(),
                if_not_exists: true,
                fields: vec![
                    TableField {
                        ident: "id".into(),
                        datatype: DataType::Serial,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::PrimaryKey, TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "migration_id".into(),
                        datatype: DataType::VarChar { size: 255 },
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "sql".into(),
                        datatype: DataType::Text,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "success".into(),
                        datatype: DataType::Bool,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "error".into(),
                        datatype: DataType::Text,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "timestamp".into(),
                        datatype: DataType::Timestamp,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    }
                ],
                primary_key: None,
                withs: Vec::new(),
            },
            &[b';']
        );
    }

    #[test]
    fn create_unique_index() {
        parser_parse!(
            CreateIndex,
            "CREATE UNIQUE INDEX \"UQE_user_login\" ON \"user\" (\"login\");",
            CreateIndex {
                identifier: Identifier("UQE_user_login".into()),
                table: Identifier("user".into()),
                columns: vec![Identifier("login".into())],
                unique: true,
            },
            &[b';']
        );
    }

    #[test]
    fn create_normal_index() {
        parser_parse!(
            CreateIndex,
            "CREATE INDEX \"UQE_user_login\" ON \"user\" (\"login\");",
            CreateIndex {
                identifier: Identifier("UQE_user_login".into()),
                table: Identifier("user".into()),
                columns: vec![Identifier("login".into())],
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

    #[test]
    fn type_modifier_test() {
        parser_parse!(TypeModifier, "NOT NULL", TypeModifier::NotNull);

        parser_parse!(TypeModifier, "PRIMARY KEY", TypeModifier::PrimaryKey);
    }

    #[test]
    fn create_table_explicit_primary_key() {
        let arena = bumpalo::Bump::new();
        arena_parser_parse!(
            CreateTable,
            "CREATE TABLE IF NOT EXISTS \"alert_instance\" (\n\"def_org_id\" BIGINT NOT NULL\n, \"def_uid\" VARCHAR(40) NOT NULL DEFAULT 0\n, \"labels\" TEXT NOT NULL\n, \"labels_hash\" VARCHAR(190) NOT NULL\n, \"current_state\" VARCHAR(190) NOT NULL\n, \"current_state_since\" BIGINT NOT NULL\n, \"last_eval_time\" BIGINT NOT NULL\n, PRIMARY KEY ( \"def_org_id\",\"def_uid\",\"labels_hash\" ));",
            CreateTable {
                fields: vec![
                    TableField {
                        ident: "def_org_id".into(),
                        datatype: DataType::BigInteger,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "def_uid".into(),
                        datatype: DataType::VarChar {size: 40 },
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull, TypeModifier::DefaultValue { value: Some(Literal::SmallInteger(0)) }].into(),
                    },
                    TableField {
                        ident: "labels".into(),
                        datatype: DataType::Text,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "labels_hash".into(),
                        datatype: DataType::VarChar{size: 190},
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "current_state".into(),
                        datatype: DataType::VarChar{size: 190},
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "current_state_since".into(),
                        datatype: DataType::BigInteger,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                    TableField {
                        ident: "last_eval_time".into(),
                        datatype: DataType::BigInteger,
                        modifiers: bumpalo::vec![in &arena; TypeModifier::NotNull].into(),
                    },
                ],
                identifier: "alert_instance".into(),
                withs: vec![],
                primary_key: Some(vec!["def_org_id".into(), "def_uid".into(), "labels_hash".into()]),
                if_not_exists: true,
            },
            &[b';']
        );
    }

    #[test]
    fn create_with_fillfactor() {
        let arena = bumpalo::Bump::new();
        arena_parser_parse!(
            CreateTable,
            "CREATE TABLE testing (name int) with (fillfactor=100)",
            CreateTable {
                identifier: "testing".into(),
                fields: vec![TableField {
                    ident: "name".into(),
                    datatype: DataType::Integer,
                    modifiers: bumpalo::vec![in &arena; ].into()
                }],
                if_not_exists: false,
                primary_key: None,
                withs: vec![WithOptions::FillFactor(100)]
            }
        );
    }
}
