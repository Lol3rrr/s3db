use nom::{IResult, Parser};

use crate::{ArenaParser, CompatibleParser, Parser as _};

use super::{common::Identifier, ColumnReference, Select, ValueExpression};

#[derive(Debug, PartialEq)]
pub struct Insert<'s, 'a> {
    pub table: Identifier<'s>,
    pub fields: crate::arenas::Vec<'a, Identifier<'s>>,
    pub values: InsertValues<'s, 'a>,
    pub returning: Option<Identifier<'s>>,
    pub on_conflict: Option<ConflictHandling<'s, 'a>>,
}

#[derive(Debug, PartialEq)]
pub struct ConflictHandling<'s, 'a> {
    pub attributes: crate::arenas::Vec<'a, Identifier<'s>>,
    pub update: crate::arenas::Vec<'a, (ColumnReference<'s>, ValueExpression<'s, 'a>)>,
}

impl<'s, 'a> ConflictHandling<'s, 'a> {
    pub fn to_static(&self) -> ConflictHandling<'static, 'static> {
        ConflictHandling {
            attributes: crate::arenas::Vec::Heap(
                self.attributes
                    .iter()
                    .map(|ident| ident.to_static())
                    .collect(),
            ),
            update: crate::arenas::Vec::Heap(
                self.update
                    .iter()
                    .map(|(cr, val)| (cr.to_static(), val.to_static()))
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum InsertValues<'s, 'a> {
    Values(crate::arenas::Vec<'a, crate::arenas::Vec<'a, ValueExpression<'s, 'a>>>),
    Select(Select<'s, 'a>),
}

impl<'i, 'a> CompatibleParser for InsertValues<'i, 'a> {
    type StaticVersion = InsertValues<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::Values(vs) => InsertValues::Values(crate::arenas::Vec::Heap(
                vs.iter()
                    .map(|row| {
                        crate::arenas::Vec::Heap(row.iter().map(|v| v.to_static()).collect())
                    })
                    .collect(),
            )),
            Self::Select(s) => InsertValues::Select(s.to_static()),
        }
    }

    fn parameter_count(&self) -> usize {
        match self {
            Self::Values(parts) => parts
                .iter()
                .flat_map(|i| i.iter())
                .map(|v| v.parameter_count())
                .max()
                .unwrap_or(0),
            Self::Select(s) => s.parameter_count(),
        }
    }
}

impl<'i, 'a> CompatibleParser for Insert<'i, 'a> {
    type StaticVersion = Insert<'static, 'static>;

    fn parameter_count(&self) -> usize {
        match &self.values {
            InsertValues::Values(values) => values
                .iter()
                .flat_map(|r| r.iter())
                .map(|v| v.parameter_count())
                .max()
                .unwrap_or(0),
            InsertValues::Select(s) => s.parameter_count(),
        }
    }

    fn to_static(&self) -> Self::StaticVersion {
        Insert {
            table: self.table.to_static(),
            fields: crate::arenas::Vec::Heap(self.fields.iter().map(|f| f.to_static()).collect()),
            values: self.values.to_static(),
            returning: self.returning.as_ref().map(|r| r.to_static()),
            on_conflict: self.on_conflict.as_ref().map(|c| c.to_static()),
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for Insert<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            insert(i, a)
        }
    }
}

#[deprecated]
pub fn insert<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], Insert<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    let (remaining, (_, _, ident, _, _, fields, _, _, values, returning, on_conflict)) =
        nom::sequence::tuple((
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("INSERT"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("INTO"),
            )),
            nom::character::complete::multispace1,
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
                .map(|(_, ident, _)| ident),
            ),
            nom::bytes::complete::tag(")"),
            nom::character::complete::multispace1,
            nom::branch::alt((|i| insert_values(i, arena),)),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("RETURNING"),
                    nom::character::complete::multispace1,
                    Identifier::parse(),
                ))
                .map(|(_, _, _, id)| id),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::sequence::tuple((
                        nom::character::complete::multispace1,
                        nom::bytes::complete::tag_no_case("ON"),
                        nom::character::complete::multispace1,
                        nom::bytes::complete::tag_no_case("CONFLICT"),
                        nom::character::complete::multispace0,
                    )),
                    nom::bytes::complete::tag("("),
                    crate::nom_util::bump_separated_list1(
                        arena,
                        nom::bytes::complete::tag(","),
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            Identifier::parse(),
                            nom::character::complete::multispace0,
                        ))
                        .map(|(_, ident, _)| ident),
                    ),
                    nom::bytes::complete::tag(")"),
                    nom::character::complete::multispace1,
                    nom::sequence::tuple((
                        nom::bytes::complete::tag_no_case("DO"),
                        nom::character::complete::multispace1,
                        nom::bytes::complete::tag_no_case("UPDATE"),
                        nom::character::complete::multispace1,
                        nom::bytes::complete::tag_no_case("SET"),
                        nom::character::complete::multispace1,
                    )),
                    crate::nom_util::bump_separated_list1(
                        arena,
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag(","),
                            nom::character::complete::multispace0,
                        )),
                        nom::sequence::tuple((
                            ColumnReference::parse(),
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag("="),
                            nom::character::complete::multispace0,
                            ValueExpression::parse_arena(arena),
                        ))
                        .map(|(cr, _, _, _, val)| (cr, val)),
                    ),
                ))
                .map(|(_, _, conflict_idents, _, _, _, updates)| (conflict_idents, updates)),
            ),
        ))(i)?;

    Ok((
        remaining,
        Insert {
            table: ident,
            fields,
            values,
            returning,
            on_conflict: on_conflict.map(|(fields, other)| ConflictHandling {
                attributes: fields,
                update: other,
            }),
        },
    ))
}

fn insert_values<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], InsertValues<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::tuple((
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("VALUES"),
                    nom::character::complete::multispace0,
                )),
                crate::nom_util::bump_separated_list1(
                    arena,
                    nom::sequence::tuple((
                        nom::character::complete::multispace0,
                        nom::bytes::complete::tag(","),
                        nom::character::complete::multispace0,
                    )),
                    nom::sequence::tuple((
                        nom::bytes::complete::tag("("),
                        crate::nom_util::bump_separated_list1(
                            arena,
                            nom::bytes::complete::tag(","),
                            nom::sequence::tuple((
                                nom::character::complete::multispace0,
                                ValueExpression::parse_arena(arena),
                                nom::character::complete::multispace0,
                            ))
                            .map(|(_, v, _)| v),
                        ),
                        nom::bytes::complete::tag(")"),
                    ))
                    .map(|(_, vs, _)| vs),
                ),
            )),
            |(_, values)| InsertValues::Values(values),
        ),
        nom::combinator::map(Select::parse_arena(arena), InsertValues::Select),
    ))(i)
}

#[cfg(test)]
mod tests {
    use crate::{macros::arena_parser_parse, Literal};

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn insert_single_row() {
        arena_parser_parse!(
            Insert,
            "INSERT INTO testing (first, second) VALUES ('fval', 'sval')",
            Insert {
                table: Identifier("testing".into()),
                fields: vec![Identifier("first".into()), Identifier("second".into())].into(),
                values: InsertValues::Values(
                    vec![vec![
                        ValueExpression::Literal(Literal::Str("fval".into())),
                        ValueExpression::Literal(Literal::Str("sval".into()))
                    ]
                    .into()]
                    .into()
                ),
                returning: None,
                on_conflict: None,
            }
        );
    }

    #[test]
    fn insert_multiple_rows() {
        arena_parser_parse!(
            Insert,
            "INSERT INTO testing (first, second) VALUES ('fval', 'sval'), ('fval2', 'sval2')",
            Insert {
                table: Identifier("testing".into()),
                fields: vec![Identifier("first".into()), Identifier("second".into())].into(),
                values: InsertValues::Values(
                    vec![
                        vec![
                            ValueExpression::Literal(Literal::Str("fval".into())),
                            ValueExpression::Literal(Literal::Str("sval".into()))
                        ]
                        .into(),
                        vec![
                            ValueExpression::Literal(Literal::Str("fval2".into())),
                            ValueExpression::Literal(Literal::Str("sval2".into()))
                        ]
                        .into()
                    ]
                    .into()
                ),
                returning: None,
                on_conflict: None,
            }
        );
    }

    #[test]
    fn insert_with_returning() {
        arena_parser_parse!(
            Insert,
            "INSERT INTO \"migration_log\" (\"migration_id\",\"sql\",\"success\",\"error\",\"timestamp\") VALUES ($1, $2, $3, $4, $5) RETURNING \"id\"",
            Insert {
                table: "migration_log".into(),
                fields: vec![
                    Identifier::from("migration_id"),
                    "sql".into(),
                    "success".into(),
                    "error".into(),
                    "timestamp".into()
                ].into(),
                values: InsertValues::Values(vec![vec![
                    ValueExpression::Placeholder(1),
                    ValueExpression::Placeholder(2),
                    ValueExpression::Placeholder(3),
                    ValueExpression::Placeholder(4),
                    ValueExpression::Placeholder(5),
                ].into()].into()),
                returning: Some("id".into()),
                on_conflict: None,
            }
        );
    }

    #[test]
    fn insert_with_select_query() {
        arena_parser_parse!(
            Insert,
            "INSERT INTO \"user\" (\"name\"\n, \"company\"\n, \"org_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\") SELECT \"name\"\n, \"company\"\n, \"account_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\" FROM \"user_v1\""
        );
    }

    #[test]
    fn insert_select_with_function() {
        arena_parser_parse!(
            Insert,
            "INSERT INTO dashboard_version\n(\n\tdashboard_id,\n\tversion,\n\tparent_version,\n\trestored_from,\n\tcreated,\n\tcreated_by,\n\tmessage,\n\tdata\n)\nSELECT\n\tdashboard.id,\n\tdashboard.version,\n\tdashboard.version,\n\tdashboard.version,\n\tdashboard.updated,\n\tCOALESCE(dashboard.updated_by, -1),\n\t'',\n\tdashboard.data\nFROM dashboard"
        );
    }
}
