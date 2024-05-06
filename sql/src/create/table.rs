use nom::{IResult, Parser as _};

use crate::{
    create::{ParsedTableField, TableField, WithOptions},
    ArenaParser as _, CompatibleParser, Identifier, Literal, Parser as _,
};

#[derive(Debug, PartialEq)]
pub struct CreateTable<'s, 'a> {
    pub identifier: Identifier<'s>,
    pub fields: crate::arenas::Vec<'a, TableField<'s, 'a>>,
    pub if_not_exists: bool,
    pub primary_key: Option<crate::arenas::Vec<'a, Identifier<'s>>>,
    pub partitioned: Option<()>,
    pub withs: crate::arenas::Vec<'a, WithOptions>,
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
        CreateTable {
            identifier: self.identifier.to_static(),
            fields: crate::arenas::Vec::Heap(self.fields.iter().map(|f| f.to_static()).collect()),
            if_not_exists: self.if_not_exists,
            primary_key: self.primary_key.as_ref().map(|parts| {
                crate::arenas::Vec::Heap(parts.iter().map(|p| p.to_static()).collect())
            }),
            partitioned: self.partitioned,
            withs: self.withs.clone_to_heap(),
        }
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
    let (
        remaining,
        (_, _, _, if_not_exists, _, table_ident, _, _, _, raw_parts, _, _, partition, with),
    ) = nom::sequence::tuple((
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
        crate::nom_util::bump_separated_list1(
            a,
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
                    crate::nom_util::bump_separated_list1(
                        a,
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
        nom::combinator::opt(nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("PARTITION"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("BY"),
            nom::character::complete::multispace1,
            nom::branch::alt((nom::combinator::map(
                nom::bytes::complete::tag_no_case("range"),
                |_| (),
            ),)),
            nom::character::complete::multispace1,
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag("("),
                    nom::character::complete::multispace0,
                )),
                Identifier::parse(),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
        ))),
        nom::combinator::opt(
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("WITH"),
                nom::character::complete::multispace1,
                nom::sequence::delimited(
                    nom::bytes::complete::tag("("),
                    crate::nom_util::bump_separated_list0(
                        a,
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
                                other => {
                                    panic!("Fill Factor needs to be a number, but got {:?}", other)
                                }
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

    dbg!(&partition);

    let mut fields = crate::arenas::Vec::arena(a);
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
            partitioned: partition.map(|_| ()),
            withs: with.unwrap_or(crate::arenas::Vec::arena(a)),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{macros::arena_parser_parse, DataType, Literal, TypeModifier};

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
                ].into(),
                primary_key: None,
                partitioned: None,
                withs: Vec::new().into(),
            },
            &[b';']
        );
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
                ].into(),
                identifier: "alert_instance".into(),
                withs: vec![].into(),
                primary_key: Some(vec!["def_org_id".into(), "def_uid".into(), "labels_hash".into()].into()),
                partitioned: None,
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
                }]
                .into(),
                if_not_exists: false,
                primary_key: None,
                partitioned: None,
                withs: vec![WithOptions::FillFactor(100)].into()
            }
        );
    }

    #[test]
    fn create_partioned() {
        arena_parser_parse!(
            CreateTable,
            "CREATE TABLE testing (name int) partition by range (name)",
            CreateTable {
                identifier: "testing".into(),
                fields: vec![TableField {
                    ident: "name".into(),
                    datatype: DataType::Integer,
                    modifiers: Vec::new().into(),
                }]
                .into(),
                if_not_exists: false,
                primary_key: None,
                partitioned: Some(()),
                withs: Vec::new().into(),
            }
        );
    }
}
