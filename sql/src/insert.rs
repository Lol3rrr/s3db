use nom::{IResult, Parser};

use crate::common::{column_reference, identifier, value_expression};

use super::{common::Identifier, select::select, ColumnReference, Select, ValueExpression};

#[derive(Debug, PartialEq)]
pub struct Insert<'s> {
    pub table: Identifier<'s>,
    pub fields: Vec<Identifier<'s>>,
    pub values: InsertValues<'s>,
    pub returning: Option<Identifier<'s>>,
    pub on_conflict: Option<ConflictHandling<'s>>,
}

#[derive(Debug, PartialEq)]
pub struct ConflictHandling<'s> {
    pub attributes: Vec<Identifier<'s>>,
    pub update: Vec<(ColumnReference<'s>, ValueExpression<'s>)>,
}

impl<'s> Insert<'s> {
    pub fn to_static(&self) -> Insert<'static> {
        Insert {
            table: self.table.to_static(),
            fields: self.fields.iter().map(|f| f.to_static()).collect(),
            values: self.values.to_static(),
            returning: self.returning.as_ref().map(|r| r.to_static()),
            on_conflict: self.on_conflict.as_ref().map(|c| c.to_static()),
        }
    }

    pub fn max_parameter(&self) -> usize {
        match &self.values {
            InsertValues::Values(values) => values
                .iter()
                .flat_map(|r| r.iter())
                .map(|v| v.max_parameter())
                .max()
                .unwrap_or(0),
            InsertValues::Select(s) => s.max_parameter(),
        }
    }
}

impl<'s> ConflictHandling<'s> {
    pub fn to_static(&self) -> ConflictHandling<'static> {
        ConflictHandling {
            attributes: self
                .attributes
                .iter()
                .map(|ident| ident.to_static())
                .collect(),
            update: self
                .update
                .iter()
                .map(|(cr, val)| (cr.to_static(), val.to_static()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum InsertValues<'s> {
    Values(Vec<Vec<ValueExpression<'s>>>),
    Select(Select<'s>),
}

impl<'s> InsertValues<'s> {
    pub fn to_static(&self) -> InsertValues<'static> {
        match self {
            Self::Values(vs) => InsertValues::Values(
                vs.iter()
                    .map(|row| row.iter().map(|v| v.to_static()).collect())
                    .collect(),
            ),
            Self::Select(s) => InsertValues::Select(s.to_static()),
        }
    }
}

pub fn insert(i: &[u8]) -> IResult<&[u8], Insert<'_>, nom::error::VerboseError<&[u8]>> {
    let (remaining, (_, _, ident, _, _, fields, _, _, values, returning, on_conflict)) =
        nom::sequence::tuple((
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("INSERT"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("INTO"),
            )),
            nom::character::complete::multispace1,
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
                .map(|(_, ident, _)| ident),
            ),
            nom::bytes::complete::tag(")"),
            nom::character::complete::multispace1,
            nom::branch::alt((insert_values,)),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("RETURNING"),
                    nom::character::complete::multispace1,
                    identifier,
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
                    nom::multi::separated_list1(
                        nom::bytes::complete::tag(","),
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            identifier,
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
                    nom::multi::separated_list1(
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag(","),
                            nom::character::complete::multispace0,
                        )),
                        nom::sequence::tuple((
                            column_reference,
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag("="),
                            nom::character::complete::multispace0,
                            value_expression,
                        ))
                        .map(|(cr, _, _, _, val)| (cr, val)),
                    ),
                ))
                .map(|(_, _, conflict_idents, _, _, _, updates)| (conflict_idents, updates)),
            ),
        ))(i)?;

    dbg!(&on_conflict);

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

fn insert_values(i: &[u8]) -> IResult<&[u8], InsertValues<'_>, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::tuple((
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("VALUES"),
                    nom::character::complete::multispace0,
                )),
                nom::multi::separated_list1(
                    nom::sequence::tuple((
                        nom::character::complete::multispace0,
                        nom::bytes::complete::tag(","),
                        nom::character::complete::multispace0,
                    )),
                    nom::sequence::tuple((
                        nom::bytes::complete::tag("("),
                        nom::multi::separated_list1(
                            nom::bytes::complete::tag(","),
                            nom::sequence::tuple((
                                nom::character::complete::multispace0,
                                value_expression,
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
        nom::combinator::map(select, |s| InsertValues::Select(s)),
    ))(i)
}

#[cfg(test)]
mod tests {
    use crate::Literal;

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn insert_single_row() {
        let query = "INSERT INTO testing (first, second) VALUES ('fval', 'sval')";
        let (remaining, ins) = insert(query.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Insert {
                table: Identifier("testing".into()),
                fields: vec![Identifier("first".into()), Identifier("second".into())],
                values: InsertValues::Values(vec![vec![
                    ValueExpression::Literal(Literal::Str("fval".into())),
                    ValueExpression::Literal(Literal::Str("sval".into()))
                ]]),
                returning: None,
                on_conflict: None,
            },
            ins
        );
    }

    #[test]
    fn insert_multiple_rows() {
        let query =
            "INSERT INTO testing (first, second) VALUES ('fval', 'sval'), ('fval2', 'sval2')";
        let (remaining, ins) = insert(query.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Insert {
                table: Identifier("testing".into()),
                fields: vec![Identifier("first".into()), Identifier("second".into())],
                values: InsertValues::Values(vec![
                    vec![
                        ValueExpression::Literal(Literal::Str("fval".into())),
                        ValueExpression::Literal(Literal::Str("sval".into()))
                    ],
                    vec![
                        ValueExpression::Literal(Literal::Str("fval2".into())),
                        ValueExpression::Literal(Literal::Str("sval2".into()))
                    ]
                ]),
                returning: None,
                on_conflict: None,
            },
            ins
        );
    }

    #[test]
    fn insert_with_returning() {
        let (remaining, ins) = insert(
            "INSERT INTO \"migration_log\" (\"migration_id\",\"sql\",\"success\",\"error\",\"timestamp\") VALUES ($1, $2, $3, $4, $5) RETURNING \"id\""
                .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(Identifier::from("migration_log"), ins.table);
        assert_eq!(
            vec![
                Identifier::from("migration_id"),
                "sql".into(),
                "success".into(),
                "error".into(),
                "timestamp".into()
            ],
            ins.fields
        );
        assert_eq!(
            InsertValues::Values(vec![vec![
                ValueExpression::Placeholder(1),
                ValueExpression::Placeholder(2),
                ValueExpression::Placeholder(3),
                ValueExpression::Placeholder(4),
                ValueExpression::Placeholder(5),
            ]]),
            ins.values
        );
        assert_eq!(Some("id".into()), ins.returning);
    }

    #[test]
    fn insert_with_select_query() {
        let (remaining, ins) = insert("INSERT INTO \"user\" (\"name\"\n, \"company\"\n, \"org_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\") SELECT \"name\"\n, \"company\"\n, \"account_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\" FROM \"user_v1\"".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        dbg!(&ins);
    }

    #[test]
    fn insert_select_with_function() {
        let (remaining, ins) = insert("INSERT INTO dashboard_version\n(\n\tdashboard_id,\n\tversion,\n\tparent_version,\n\trestored_from,\n\tcreated,\n\tcreated_by,\n\tmessage,\n\tdata\n)\nSELECT\n\tdashboard.id,\n\tdashboard.version,\n\tdashboard.version,\n\tdashboard.version,\n\tdashboard.updated,\n\tCOALESCE(dashboard.updated_by, -1),\n\t'',\n\tdashboard.data\nFROM dashboard".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        dbg!(&ins);
    }
}
