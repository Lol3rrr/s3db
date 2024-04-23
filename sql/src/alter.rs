use nom::{IResult, Parser};

use crate::{CompatibleParser, Parser as _Parser};

use super::{DataType, Identifier, Literal, TypeModifier};

#[derive(Debug, PartialEq)]
pub enum AlterTable<'s, 'a> {
    Rename {
        from: Identifier<'s>,
        to: Identifier<'s>,
    },
    AddColumn {
        table: Identifier<'s>,
        column_name: Identifier<'s>,
        data_type: DataType,
        type_modifiers: crate::arenas::Vec<'a, TypeModifier>,
    },
    AlterColumnTypes {
        table: Identifier<'s>,
        columns: crate::arenas::Vec<'a, (Identifier<'s>, DataType)>,
    },
    AtlerColumnDropNotNull {
        table: Identifier<'s>,
        column: Identifier<'s>,
    },
    RenameColumn {
        table: Identifier<'s>,
        from: Identifier<'s>,
        to: Identifier<'s>,
    },
    SetColumnDefault {
        table: Identifier<'s>,
        column: Identifier<'s>,
        value: Literal<'s>,
    },
    AddPrimaryKey {
        table: Identifier<'s>,
        column: Identifier<'s>,
    },
}

impl<'s, 'a> CompatibleParser for AlterTable<'s, 'a> {
    type StaticVersion = AlterTable<'static, 'static>;

    fn parameter_count(&self) -> usize {
        0
    }

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::Rename { from, to } => AlterTable::Rename {
                from: from.to_static(),
                to: to.to_static(),
            },
            Self::AddColumn {
                table,
                column_name,
                data_type,
                type_modifiers,
            } => AlterTable::AddColumn {
                table: table.to_static(),
                column_name: column_name.to_static(),
                data_type: data_type.clone(),
                type_modifiers: type_modifiers.clone_to_heap(),
            },
            Self::AlterColumnTypes { table, columns } => AlterTable::AlterColumnTypes {
                table: table.to_static(),
                columns: crate::arenas::Vec::Heap(
                    columns
                        .iter()
                        .map(|(cn, ct)| (cn.to_static(), ct.clone()))
                        .collect(),
                ),
            },
            Self::AtlerColumnDropNotNull { table, column } => AlterTable::AtlerColumnDropNotNull {
                table: table.to_static(),
                column: column.to_static(),
            },
            Self::RenameColumn { table, from, to } => AlterTable::RenameColumn {
                table: table.to_static(),
                from: from.to_static(),
                to: to.to_static(),
            },
            Self::SetColumnDefault {
                table,
                column,
                value,
            } => AlterTable::SetColumnDefault {
                table: table.to_static(),
                column: column.to_static(),
                value: value.to_static(),
            },
            Self::AddPrimaryKey { table, column } => AlterTable::AddPrimaryKey {
                table: table.to_static(),
                column: column.to_static(),
            },
        }
    }
}

impl<'i, 's, 'a> crate::ArenaParser<'i, 'a> for AlterTable<'s, 'a>
where
    'i: 's,
{
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            alter_table(i, a)
        }
    }
}

#[deprecated]
pub fn alter_table<'i, 's, 'a>(
    i: &'i [u8],
    a: &'a bumpalo::Bump,
) -> IResult<&'i [u8], AlterTable<'s, 'a>, nom::error::VerboseError<&'i [u8]>>
where
    'i: 's,
{
    let (remaining, (_, _, table, _)) = nom::sequence::tuple((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("ALTER"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TABLE"),
        )),
        nom::character::complete::multispace1,
        Identifier::parse(),
        nom::character::complete::multispace1,
    ))(i)?;

    let (remaining, tmp) = nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("RENAME"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TO"),
            nom::character::complete::multispace1,
            Identifier::parse(),
        ))
        .map(|(_, _, _, _, target)| AlterTable::Rename {
            from: table.clone(),
            to: target,
        }),
        nom::sequence::tuple((
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("ADD"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("COLUMN"),
                nom::character::complete::multispace1,
            )),
            Identifier::parse(),
            nom::character::complete::multispace1,
            DataType::parse(),
            nom::character::complete::multispace1,
            crate::nom_util::bump_separated_list0(
                a,
                nom::character::complete::multispace1,
                TypeModifier::parse(),
            ),
        ))
        .map(
            |(_, column, _, data_type, _, type_modifiers)| AlterTable::AddColumn {
                table: table.clone(),
                column_name: column,
                data_type,
                type_modifiers,
            },
        ),
        crate::nom_util::bump_separated_list1(
            a,
            nom::sequence::tuple((
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
            )),
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("ALTER"),
                nom::combinator::opt(nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("COLUMN"),
                ))),
                nom::character::complete::multispace1,
                Identifier::parse(),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("TYPE"),
                nom::character::complete::multispace1,
                DataType::parse(),
                nom::combinator::opt(nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    TypeModifier::parse(),
                ))),
            ))
            .map(|(_, _, _, cname, _, _, _, dt, _tm)| (cname, dt)),
        )
        .map(|columns| AlterTable::AlterColumnTypes {
            table: table.clone(),
            columns,
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("ALTER"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("COLUMN"),
            nom::character::complete::multispace1,
            Identifier::parse(),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("DROP"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("NOT"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("NULL"),
        ))
        .map(|(_, _, _, _, column, _, _, _, _, _, _)| {
            AlterTable::AtlerColumnDropNotNull {
                table: table.clone(),
                column,
            }
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("RENAME"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("COLUMN"),
            nom::character::complete::multispace1,
            Identifier::parse(),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TO"),
            nom::character::complete::multispace1,
            Identifier::parse(),
        ))
        .map(|(_, _, _, _, from, _, _, _, to)| AlterTable::RenameColumn {
            table: table.clone(),
            from,
            to,
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("ALTER"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("COLUMN"),
            nom::character::complete::multispace1,
            Identifier::parse(),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("SET"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("DEFAULT"),
            nom::character::complete::multispace1,
            Literal::parse(),
        ))
        .map(
            |(_, _, _, _, column, _, _, _, _, _, value)| AlterTable::SetColumnDefault {
                table: table.clone(),
                column,
                value,
            },
        ),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("add"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("primary"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("key"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag("("),
            nom::character::complete::multispace0,
            Identifier::parse(),
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(")"),
        ))
        .map(
            |(_, _, _, _, _, _, _, _, column, _, _)| AlterTable::AddPrimaryKey {
                table: table.clone(),
                column,
            },
        ),
    ))(remaining)?;

    Ok((remaining, tmp))
}

#[cfg(test)]
mod tests {
    use crate::{macros::arena_parser_parse, Literal};

    use super::*;

    #[test]
    fn alter_table_rename() {
        arena_parser_parse!(
            AlterTable,
            "ALTER TABLE \"user\" RENAME TO \"user_v1\"",
            AlterTable::Rename {
                from: Identifier("user".into()),
                to: Identifier("user_v1".into())
            }
        );
    }

    #[test]
    fn alter_add_column() {
        arena_parser_parse!(
            AlterTable,
            "alter table \"user\" ADD COLUMN \"help_flags1\" BIGINT NOT NULL DEFAULT 0",
            AlterTable::AddColumn {
                table: Identifier("user".into()),
                column_name: Identifier("help_flags1".into()),
                data_type: DataType::BigInteger,
                type_modifiers: vec![
                    TypeModifier::NotNull,
                    TypeModifier::DefaultValue {
                        value: Some(Literal::SmallInteger(0))
                    }
                ]
                .into()
            }
        );
    }

    #[test]
    fn alter_column_types() {
        arena_parser_parse!(AlterTable, "ALTER TABLE \"user\" ALTER \"login\" TYPE VARCHAR(190), ALTER \"email\" TYPE VARCHAR(190), ALTER \"name\" TYPE VARCHAR(255), ALTER \"password\" TYPE VARCHAR(255), ALTER \"salt\" TYPE VARCHAR(50), ALTER \"rands\" TYPE VARCHAR(50), ALTER \"company\" TYPE VARCHAR(255), ALTER \"theme\" TYPE VARCHAR(255)", AlterTable::AlterColumnTypes {
                table: Identifier("user".into()),
                columns: vec![
                    (Identifier("login".into()), DataType::VarChar { size: 190 }),
                    (Identifier("email".into()), DataType::VarChar { size: 190 }),
                    (Identifier("name".into()), DataType::VarChar { size: 255 }),
                    (
                        Identifier("password".into()),
                        DataType::VarChar { size: 255 }
                    ),
                    (Identifier("salt".into()), DataType::VarChar { size: 50 }),
                    (Identifier("rands".into()), DataType::VarChar { size: 50 }),
                    (
                        Identifier("company".into()),
                        DataType::VarChar { size: 255 }
                    ),
                    (Identifier("theme".into()), DataType::VarChar { size: 255 })
                ].into()
            });
    }

    #[test]
    fn alter_column_drop_not_null() {
        arena_parser_parse!(
            AlterTable,
            "ALTER TABLE \"user\" ALTER COLUMN is_service_account DROP NOT NULL",
            AlterTable::AtlerColumnDropNotNull {
                table: Identifier("user".into()),
                column: Identifier("is_service_account".into())
            }
        );
    }

    #[test]
    fn alter_column_add_bytea_null() {
        arena_parser_parse!(
            AlterTable,
            "alter table \"dashboard_snapshot\" ADD COLUMN \"dashboard_encrypted\" BYTEA NULL",
            AlterTable::AddColumn {
                table: Identifier("dashboard_snapshot".into()),
                column_name: Identifier("dashboard_encrypted".into()),
                data_type: DataType::ByteA,
                type_modifiers: vec![TypeModifier::Null].into()
            }
        );
    }

    #[test]
    fn alter_column_type() {
        arena_parser_parse!(
            AlterTable,
            "ALTER TABLE annotation ALTER COLUMN tags TYPE VARCHAR(4096)",
            AlterTable::AlterColumnTypes {
                table: Identifier("annotation".into()),
                columns: vec![(Identifier("tags".into()), DataType::VarChar { size: 4096 })].into()
            }
        );
    }

    #[test]
    fn alter_column_rename() {
        arena_parser_parse!(
            AlterTable,
            "ALTER TABLE alert_instance RENAME COLUMN def_org_id TO rule_org_id",
            AlterTable::RenameColumn {
                table: Identifier("alert_instance".into()),
                from: Identifier("def_org_id".into()),
                to: Identifier("rule_org_id".into())
            }
        );
    }

    #[test]
    fn alter_set_default() {
        arena_parser_parse!(
            AlterTable,
            "ALTER TABLE alert_rule ALTER COLUMN is_paused SET DEFAULT false",
            AlterTable::SetColumnDefault {
                table: Identifier("alert_rule".into()),
                column: Identifier("is_paused".into()),
                value: Literal::Bool(false)
            }
        );
    }
}
