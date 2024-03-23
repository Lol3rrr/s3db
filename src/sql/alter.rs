use nom::{IResult, Parser};

use crate::sql::common::identifier;

use super::{
    common::{data_type, literal, type_modifier},
    DataType, Identifier, Literal, TypeModifier,
};

#[derive(Debug, PartialEq)]
pub enum AlterTable<'s> {
    Rename {
        from: Identifier<'s>,
        to: Identifier<'s>,
    },
    AddColumn {
        table: Identifier<'s>,
        column_name: Identifier<'s>,
        data_type: DataType,
        type_modifiers: Vec<TypeModifier>,
    },
    AlterColumnTypes {
        table: Identifier<'s>,
        columns: Vec<(Identifier<'s>, DataType)>,
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
}

impl<'s> AlterTable<'s> {
    pub fn to_static(&self) -> AlterTable<'static> {
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
                type_modifiers: type_modifiers.clone(),
            },
            Self::AlterColumnTypes { table, columns } => AlterTable::AlterColumnTypes {
                table: table.to_static(),
                columns: columns
                    .iter()
                    .map(|(cn, ct)| (cn.to_static(), ct.clone()))
                    .collect(),
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
        }
    }
}

pub fn alter_table(i: &[u8]) -> IResult<&[u8], AlterTable<'_>> {
    let (remaining, (_, _, table, _)) = nom::sequence::tuple((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("ALTER"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TABLE"),
        )),
        nom::character::complete::multispace1,
        identifier,
        nom::character::complete::multispace1,
    ))(i)?;

    let (remaining, tmp) = nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("RENAME"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TO"),
            nom::character::complete::multispace1,
            identifier,
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
            identifier,
            nom::character::complete::multispace1,
            data_type,
            nom::character::complete::multispace1,
            nom::multi::separated_list0(nom::character::complete::multispace1, type_modifier),
        ))
        .map(
            |(_, column, _, data_type, _, type_modifiers)| AlterTable::AddColumn {
                table: table.clone(),
                column_name: column,
                data_type,
                type_modifiers,
            },
        ),
        nom::multi::separated_list1(
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
                identifier,
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("TYPE"),
                nom::character::complete::multispace1,
                data_type,
                nom::combinator::opt(nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    type_modifier,
                ))),
            ))
            .map(|(_, _, _, cname, _, _, _, dt, tm)| (cname, dt)),
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
            identifier,
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
            identifier,
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("TO"),
            nom::character::complete::multispace1,
            identifier,
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
            identifier,
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("SET"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("DEFAULT"),
            nom::character::complete::multispace1,
            literal,
        ))
        .map(
            |(_, _, _, _, column, _, _, _, _, _, value)| AlterTable::SetColumnDefault {
                table: table.clone(),
                column,
                value,
            },
        ),
    ))(remaining)?;

    Ok((remaining, tmp))
}

#[cfg(test)]
mod tests {
    use crate::sql::Literal;

    use super::*;

    #[test]
    fn alter_table_rename() {
        let (remaining, alter) =
            alter_table("ALTER TABLE \"user\" RENAME TO \"user_v1\"".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            AlterTable::Rename {
                from: Identifier("user".into()),
                to: Identifier("user_v1".into())
            },
            alter
        );
    }

    #[test]
    fn alter_add_column() {
        let (remaining, alter) = alter_table(
            "alter table \"user\" ADD COLUMN \"help_flags1\" BIGINT NOT NULL DEFAULT 0".as_bytes(),
        )
        .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
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
            },
            alter
        );
    }

    #[test]
    fn alter_column_types() {
        let (remaining, alter) = alter_table("ALTER TABLE \"user\" ALTER \"login\" TYPE VARCHAR(190), ALTER \"email\" TYPE VARCHAR(190), ALTER \"name\" TYPE VARCHAR(255), ALTER \"password\" TYPE VARCHAR(255), ALTER \"salt\" TYPE VARCHAR(50), ALTER \"rands\" TYPE VARCHAR(50), ALTER \"company\" TYPE VARCHAR(255), ALTER \"theme\" TYPE VARCHAR(255)".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            AlterTable::AlterColumnTypes {
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
                ]
            },
            alter
        );
    }

    #[test]
    fn alter_column_drop_not_null() {
        let (remaining, alter) = alter_table(
            "ALTER TABLE \"user\" ALTER COLUMN is_service_account DROP NOT NULL".as_bytes(),
        )
        .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            AlterTable::AtlerColumnDropNotNull {
                table: Identifier("user".into()),
                column: Identifier("is_service_account".into())
            },
            alter
        );
    }

    #[test]
    fn alter_column_add_bytea_null() {
        let query =
            "alter table \"dashboard_snapshot\" ADD COLUMN \"dashboard_encrypted\" BYTEA NULL";
        let (remaining, alter) = alter_table(query.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            AlterTable::AddColumn {
                table: Identifier("dashboard_snapshot".into()),
                column_name: Identifier("dashboard_encrypted".into()),
                data_type: DataType::ByteA,
                type_modifiers: vec![TypeModifier::Null]
            },
            alter
        );
    }

    #[test]
    fn alter_column_type() {
        let query = "ALTER TABLE annotation ALTER COLUMN tags TYPE VARCHAR(4096)";
        let (remaining, alter) = alter_table(query.as_bytes())
            .map_err(|e| e.map_input(|r| core::str::from_utf8(r)))
            .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            AlterTable::AlterColumnTypes {
                table: Identifier("annotation".into()),
                columns: vec![(Identifier("tags".into()), DataType::VarChar { size: 4096 })]
            },
            alter
        );
    }

    #[test]
    fn alter_column_rename() {
        let query = "ALTER TABLE alert_instance RENAME COLUMN def_org_id TO rule_org_id";
        let (remaining, alter) = alter_table(query.as_bytes())
            .map_err(|e| e.map_input(|r| core::str::from_utf8(r)))
            .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            AlterTable::RenameColumn {
                table: Identifier("alert_instance".into()),
                from: Identifier("def_org_id".into()),
                to: Identifier("rule_org_id".into())
            },
            alter
        );
    }

    #[test]
    fn alter_set_default() {
        let query = "ALTER TABLE alert_rule ALTER COLUMN is_paused SET DEFAULT false";
        let (remaining, alter) = alter_table(query.as_bytes())
            .map_err(|e| e.map_input(|r| core::str::from_utf8(r)))
            .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            AlterTable::SetColumnDefault {
                table: Identifier("alert_rule".into()),
                column: Identifier("is_paused".into()),
                value: Literal::Bool(false)
            },
            alter
        );
    }
}
