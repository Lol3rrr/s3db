use nom::{IResult, Parser};

use crate::{common::identifier, dialects, CompatibleParser, Literal};

use super::common::{data_type, literal, type_modifier, DataType, Identifier, TypeModifier};

#[derive(Debug, PartialEq)]
pub struct CreateTable<'s> {
    pub identifier: Identifier<'s>,
    pub fields: Vec<TableField<'s>>,
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

impl<'s> CompatibleParser<dialects::Postgres> for CreateTable<'s> {
    type StaticVersion = CreateTable<'static>;

    fn to_static(&self) -> Self::StaticVersion {
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

    fn parameter_count(&self) -> usize {
        0
    }
}

impl<'s> CompatibleParser<dialects::Postgres> for CreateIndex<'s> {
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

enum ParsedTableField<'s> {
    Field(TableField<'s>),
    PrimaryKey(Vec<Identifier<'s>>),
}

pub fn create_table(i: &[u8]) -> IResult<&[u8], CreateTable, nom::error::VerboseError<&[u8]>> {
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
            identifier,
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
                    create_field.map(ParsedTableField::Field),
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
                            identifier,
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
                                literal,
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
            identifier,
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("ON"),
                nom::character::complete::multispace1,
            )),
            identifier,
            nom::character::complete::multispace0,
            nom::bytes::complete::tag("("),
            nom::multi::separated_list1(
                nom::bytes::complete::tag(","),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    identifier,
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
pub struct TableField<'s> {
    pub ident: Identifier<'s>,
    pub datatype: DataType,
    pub modifiers: Vec<TypeModifier>,
}

impl<'s> TableField<'s> {
    pub fn to_static(&self) -> TableField<'static> {
        TableField {
            ident: self.ident.to_static(),
            datatype: self.datatype.clone(),
            modifiers: self.modifiers.clone(),
        }
    }
}

fn create_field(i: &[u8]) -> IResult<&[u8], TableField<'_>, nom::error::VerboseError<&[u8]>> {
    nom::combinator::map(
        nom::sequence::tuple((
            identifier,
            nom::character::complete::multispace1,
            data_type,
            nom::multi::many0(
                nom::sequence::tuple((nom::character::complete::multispace1, type_modifier))
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
    use super::super::tests::single_parse_test;
    use super::*;

    #[test]
    fn create_table_test() {
        let (remaining, query)= create_table("CREATE TABLE IF NOT EXISTS \"migration_log\" (\n\"id\" SERIAL PRIMARY KEY  NOT NULL\n, \"migration_id\" VARCHAR(255) NOT NULL\n, \"sql\" TEXT NOT NULL\n, \"success\" BOOL NOT NULL\n, \"error\" TEXT NOT NULL\n, \"timestamp\" TIMESTAMP NOT NULL\n);".as_bytes()).unwrap();

        assert_eq!(&[b';'] as &[u8], remaining);

        assert_eq!(
            CreateTable {
                identifier: "migration_log".into(),
                if_not_exists: true,
                fields: vec![
                    TableField {
                        ident: "id".into(),
                        datatype: DataType::Serial,
                        modifiers: vec![TypeModifier::PrimaryKey, TypeModifier::NotNull],
                    },
                    TableField {
                        ident: "migration_id".into(),
                        datatype: DataType::VarChar { size: 255 },
                        modifiers: vec![TypeModifier::NotNull],
                    },
                    TableField {
                        ident: "sql".into(),
                        datatype: DataType::Text,
                        modifiers: vec![TypeModifier::NotNull],
                    },
                    TableField {
                        ident: "success".into(),
                        datatype: DataType::Bool,
                        modifiers: vec![TypeModifier::NotNull],
                    },
                    TableField {
                        ident: "error".into(),
                        datatype: DataType::Text,
                        modifiers: vec![TypeModifier::NotNull],
                    },
                    TableField {
                        ident: "timestamp".into(),
                        datatype: DataType::Timestamp,
                        modifiers: vec![TypeModifier::NotNull],
                    }
                ],
                primary_key: None,
                withs: Vec::new(),
            },
            query
        );
    }

    #[test]
    fn create_unique_index() {
        let (remaining, query) = create_index(
            "CREATE UNIQUE INDEX \"UQE_user_login\" ON \"user\" (\"login\");".as_bytes(),
        )
        .unwrap();

        dbg!(&remaining, &query);

        assert_eq!(
            &[b';'] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            CreateIndex {
                identifier: Identifier("UQE_user_login".into()),
                table: Identifier("user".into()),
                columns: vec![Identifier("login".into())],
                unique: true,
            },
            query
        );
    }

    #[test]
    fn create_normal_index() {
        let (remaining, query) =
            create_index("CREATE INDEX \"UQE_user_login\" ON \"user\" (\"login\");".as_bytes())
                .unwrap();

        dbg!(&remaining, &query);

        assert_eq!(
            &[b';'] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            CreateIndex {
                identifier: Identifier("UQE_user_login".into()),
                table: Identifier("user".into()),
                columns: vec![Identifier("login".into())],
                unique: false,
            },
            query
        );
    }

    #[test]
    fn field() {
        let (remaining, field) = create_field("\"id\" SERIAL PRIMARY KEY".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(
            TableField {
                ident: "id".into(),
                datatype: DataType::Serial,
                modifiers: vec![TypeModifier::PrimaryKey]
            },
            field
        );
    }

    #[test]
    fn datatypes() {
        let (remaining, dtype) = data_type("SERIAL".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Serial, dtype);

        let (remaining, dtype) = data_type("VARCHAR(123)".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::VarChar { size: 123 }, dtype);

        let (remaining, dtype) = data_type("TEXT".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Text, dtype);

        let (remaining, dtype) = data_type("BOOL".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Bool, dtype);

        let (remaining, dtype) = data_type("TIMESTAMP".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Timestamp, dtype);
    }

    #[test]
    fn type_modifier_test() {
        let (remaining, modifier) = type_modifier("NOT NULL".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(TypeModifier::NotNull, modifier);

        let (remaining, modifier) = type_modifier("PRIMARY KEY".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(TypeModifier::PrimaryKey, modifier);
    }

    #[test]
    fn create_table_explicit_primary_key() {
        let (remaining, query) = create_table("CREATE TABLE IF NOT EXISTS \"alert_instance\" (\n\"def_org_id\" BIGINT NOT NULL\n, \"def_uid\" VARCHAR(40) NOT NULL DEFAULT 0\n, \"labels\" TEXT NOT NULL\n, \"labels_hash\" VARCHAR(190) NOT NULL\n, \"current_state\" VARCHAR(190) NOT NULL\n, \"current_state_since\" BIGINT NOT NULL\n, \"last_eval_time\" BIGINT NOT NULL\n, PRIMARY KEY ( \"def_org_id\",\"def_uid\",\"labels_hash\" ));".as_bytes()).unwrap();

        assert_eq!(&[b';'] as &[u8], remaining);

        dbg!(&query);

        assert!(query.primary_key.is_some());
    }

    #[test]
    fn create_with_fillfactor() {
        single_parse_test!(
            create_table,
            "CREATE TABLE testing (name int) with (fillfactor=100)",
            CreateTable {
                identifier: "testing".into(),
                fields: vec![TableField {
                    ident: "name".into(),
                    datatype: DataType::Integer,
                    modifiers: Vec::new()
                }],
                if_not_exists: false,
                primary_key: None,
                withs: vec![WithOptions::FillFactor(100)]
            }
        );
    }
}
