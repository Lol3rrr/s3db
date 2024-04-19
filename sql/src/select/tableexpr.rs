use nom::{IResult, Parser};

use crate::{CompatibleParser, Condition, Identifier, Parser as _, ArenaParser};

use super::Select;

#[derive(Debug, PartialEq)]
pub enum TableExpression<'s, 'a> {
    Relation(Identifier<'s>),
    Renamed {
        inner: Box<TableExpression<'s, 'a>>,
        name: Identifier<'s>,
        column_rename: Option<Vec<Identifier<'s>>>,
    },
    Join {
        left: Box<TableExpression<'s, 'a>>,
        right: Box<TableExpression<'s, 'a>>,
        kind: JoinKind,
        condition: Condition<'s, 'a>,
        lateral: bool,
    },
    SubQuery(Box<Select<'s, 'a>>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

impl<'i, 'a> CompatibleParser for TableExpression<'i, 'a> {
    type StaticVersion = TableExpression<'static, 'static>;

    fn parameter_count(&self) -> usize {
        match self {
            Self::Relation(_) => 0,
            Self::Renamed { inner, .. } => inner.parameter_count(),
            Self::Join {
                left,
                right,
                condition,
                ..
            } => core::iter::once(left.parameter_count())
                .chain(core::iter::once(right.parameter_count()))
                .chain(core::iter::once(condition.parameter_count()))
                .max()
                .unwrap_or(0),
            Self::SubQuery(sq) => sq.parameter_count(),
        }
    }

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::Relation(r) => TableExpression::Relation(r.to_static()),
            Self::Renamed {
                inner,
                name,
                column_rename,
            } => TableExpression::Renamed {
                inner: Box::new(inner.to_static()),
                name: name.to_static(),
                column_rename: column_rename
                    .as_ref()
                    .map(|c| c.iter().map(|c| c.to_static()).collect()),
            },
            Self::Join {
                left,
                right,
                kind,
                condition,
                lateral,
            } => TableExpression::Join {
                left: Box::new(left.to_static()),
                right: Box::new(right.to_static()),
                kind: kind.clone(),
                condition: condition.to_static(),
                lateral: *lateral,
            },
            Self::SubQuery(inner) => TableExpression::SubQuery(Box::new(inner.to_static())),
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for TableExpression<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            table_expression(i, a)
        }
    }
}

#[deprecated]
pub fn table_expression<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump
) -> IResult<&'i [u8], TableExpression<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    fn base<'i, 'a>(i: &'i [u8], arena: &'a bumpalo::Bump) -> IResult<&'i [u8], TableExpression<'i, 'a>, nom::error::VerboseError<&'i[u8]>> {
        let (remaining, table) = nom::branch::alt((
            nom::sequence::tuple((
                nom::bytes::complete::tag("("),
                nom::character::complete::multispace0,
                Select::parse_arena(arena),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            ))
            .map(|(_, _, query, _, _)| TableExpression::SubQuery(Box::new(query))),
            nom::sequence::tuple((
                Identifier::parse(),
                nom::bytes::complete::tag("."),
                Identifier::parse(),
            ))
            .map(|(_, _, name)| TableExpression::Relation(name)),
            Identifier::parse().map(TableExpression::Relation),
        ))(i)?;

        match nom::branch::alt((
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("AS"),
                nom::character::complete::multispace1,
                Identifier::parse(),
                nom::combinator::opt(nom::sequence::tuple((
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
                ))),
            ))
            .map(|(_, _, _, ident, columns)| (ident, columns.map(|(_, _, _, c, _, _)| c))),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::combinator::not(nom::combinator::peek(nom::branch::alt((
                    nom::bytes::complete::tag_no_case("WHERE"),
                    nom::bytes::complete::tag_no_case("GROUP"),
                    nom::bytes::complete::tag_no_case("JOIN"),
                    nom::bytes::complete::tag_no_case("INNER"),
                    nom::bytes::complete::tag_no_case("RIGHT"),
                    nom::bytes::complete::tag_no_case("LEFT"),
                    nom::bytes::complete::tag_no_case("LIMIT"),
                    nom::bytes::complete::tag_no_case("ORDER"),
                    nom::bytes::complete::tag_no_case("ON"),
                )))),
                Identifier::parse(),
            ))
            .map(|(_, _, ident)| (ident, None)),
        ))(remaining)
        {
            Ok((remaining, (name, columns))) => Ok((
                remaining,
                TableExpression::Renamed {
                    inner: Box::new(table),
                    name,
                    column_rename: columns,
                },
            )),
            Err(_) => Ok((remaining, table)),
        }
    }

    let (remaining, table) = base(i, arena)?;

    let mut outer_remaining = remaining;
    let mut table = table;

    loop {
        let join_res: IResult<_, _> = nom::branch::alt((
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("JOIN"),
                nom::character::complete::multispace1,
            ))
            .map(|_| JoinKind::Inner),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("INNER"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("JOIN"),
                nom::character::complete::multispace1,
            ))
            .map(|_| JoinKind::Inner),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("LEFT"),
                nom::combinator::opt(nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("OUTER"),
                ))),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("JOIN"),
                nom::character::complete::multispace1,
            ))
            .map(|_| JoinKind::LeftOuter),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("OUTER JOIN"),
                nom::character::complete::multispace1,
            ))
            .map(|_| JoinKind::FullOuter),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("CROSS"),
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("JOIN"),
                nom::character::complete::multispace1,
            ))
            .map(|_| JoinKind::Cross),
        ))(outer_remaining);

        let (remaining, join_kind) = match join_res {
            Ok(v) => v,
            Err(_) => break,
        };

        let tmp: IResult<&[u8], _, nom::error::VerboseError<&[u8]>> = nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("LATERAL"),
            nom::character::complete::multispace1,
        ))(remaining);

        let (remaining, is_lateral) = match tmp {
            Ok((rem, _)) => (rem, true),
            Err(_) => (remaining, false),
        };

        let (remaining, other_table) = base(remaining, arena)?;

        let on_condition: IResult<_, _> = nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ON"),
            nom::character::complete::multispace1,
        ))(remaining);

        match on_condition {
            Ok((remaining, _)) => {
                let (remaining, condition) = Condition::parse_arena(arena)(remaining)?;

                outer_remaining = remaining;
                table = TableExpression::Join {
                    left: Box::new(table),
                    right: Box::new(other_table),
                    kind: join_kind,
                    condition,
                    lateral: is_lateral,
                };
            }
            Err(_) => {
                // TODO
                // Investigate the proper handling

                outer_remaining = remaining;

                table = TableExpression::Join {
                    left: Box::new(table),
                    right: Box::new(other_table),
                    kind: join_kind,
                    condition: Condition::And(vec![].into()),
                    lateral: is_lateral,
                };
            }
        };
    }

    Ok((outer_remaining, table))
}

#[cfg(test)]
mod tests {
    use crate::{ColumnReference, ValueExpression};

    use super::*;

    use pretty_assertions::assert_eq;

    use crate::macros::arena_parser_parse;

    #[test]
    fn join_on_suquery() {
        arena_parser_parse!(
            TableExpression,
            "first INNER JOIN (SELECT name FROM second)",
            TableExpression::Join {
                left: Box::new(TableExpression::Relation("first".into())),
                right: Box::new(TableExpression::SubQuery(Box::new(Select {
                    values: vec![ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "name".into(),
                    })],
                    table: Some(TableExpression::Relation("second".into())),
                    where_condition: None,
                    order_by: None,
                    group_by: None,
                    having: None,
                    limit: None,
                    for_update: None,
                    combine: None
                }))),
                kind: JoinKind::Inner,
                condition: Condition::And(vec![].into()),
                lateral: false,
            }
        );
    }

    #[test]
    fn rename_with_columns() {
        arena_parser_parse!(
            TableExpression,
            "something as o(n)",
            TableExpression::Renamed {
                inner: Box::new(TableExpression::Relation(Identifier("something".into()))),
                name: "o".into(),
                column_rename: Some(vec![Identifier("n".into())])
            }
        );
    }

    #[test]
    fn join_on_suquery_lateral() {
        arena_parser_parse!(
            TableExpression,
            "first INNER JOIN LATERAL (SELECT name FROM second)",
            TableExpression::Join {
                left: Box::new(TableExpression::Relation("first".into())),
                right: Box::new(TableExpression::SubQuery(Box::new(Select {
                    values: vec![ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "name".into(),
                    })],
                    table: Some(TableExpression::Relation("second".into())),
                    where_condition: None,
                    order_by: None,
                    group_by: None,
                    having: None,
                    limit: None,
                    for_update: None,
                    combine: None
                }))),
                kind: JoinKind::Inner,
                condition: Condition::And(vec![].into()),
                lateral: true,
            }
        );
    }
}
