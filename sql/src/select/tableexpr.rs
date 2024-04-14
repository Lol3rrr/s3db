use nom::{IResult, Parser};

use crate::{
    self as sql, common::identifier, select::select, CompatibleParser, Condition, Identifier,
};

use super::Select;

#[derive(Debug, PartialEq, Clone)]
pub enum TableExpression<'s> {
    Relation(Identifier<'s>),
    Renamed {
        inner: Box<TableExpression<'s>>,
        name: Identifier<'s>,
        column_rename: Option<Vec<Identifier<'s>>>,
    },
    Join {
        left: Box<TableExpression<'s>>,
        right: Box<TableExpression<'s>>,
        kind: JoinKind,
        condition: Condition<'s>,
    },
    SubQuery(Box<Select<'s>>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

pub fn table_expression(
    i: &[u8],
) -> IResult<&[u8], TableExpression, nom::error::VerboseError<&[u8]>> {
    fn base(i: &[u8]) -> IResult<&[u8], TableExpression, nom::error::VerboseError<&[u8]>> {
        let (remaining, table) = nom::branch::alt((
            nom::sequence::tuple((
                nom::bytes::complete::tag("("),
                nom::character::complete::multispace0,
                select,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            ))
            .map(|(_, _, query, _, _)| TableExpression::SubQuery(Box::new(query))),
            nom::sequence::tuple((identifier, nom::bytes::complete::tag("."), identifier))
                .map(|(schema, _, name)| TableExpression::Relation(name)),
            identifier.map(TableExpression::Relation),
        ))(i)?;

        match nom::branch::alt((
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("AS"),
                nom::character::complete::multispace1,
                identifier,
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
                        identifier,
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
                identifier,
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

    let (remaining, table) = base(i)?;

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

        let (remaining, tmp) = match tmp {
            Ok((rem, other)) => {
                dbg!("Lateral", other);

                (rem, ())
            }
            Err(_) => (remaining, ()),
        };

        let (remaining, other_table) = base(remaining)?;

        let on_condition: IResult<_, _> = nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ON"),
            nom::character::complete::multispace1,
        ))(remaining);

        match on_condition {
            Ok((remaining, _)) => {
                let (remaining, condition) = sql::condition::condition(remaining)?;

                outer_remaining = remaining;
                table = TableExpression::Join {
                    left: Box::new(table),
                    right: Box::new(other_table),
                    kind: join_kind,
                    condition,
                };
            }
            Err(e) => {
                outer_remaining = remaining;

                table = TableExpression::Join {
                    left: Box::new(table),
                    right: Box::new(other_table),
                    kind: join_kind,
                    condition: Condition::And(vec![]),
                };
            }
        };
    }

    Ok((outer_remaining, table))
}

impl<'s> TableExpression<'s> {
    pub fn to_static(&self) -> TableExpression<'static> {
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
            } => TableExpression::Join {
                left: Box::new(left.to_static()),
                right: Box::new(right.to_static()),
                kind: kind.clone(),
                condition: condition.to_static(),
            },
            Self::SubQuery(inner) => TableExpression::SubQuery(Box::new(inner.to_static())),
        }
    }

    pub fn max_parameter(&self) -> usize {
        match self {
            Self::Relation(_) => 0,
            Self::Renamed { inner, .. } => inner.max_parameter(),
            Self::Join {
                left,
                right,
                condition,
                ..
            } => core::iter::once(left.max_parameter())
                .chain(core::iter::once(right.max_parameter()))
                .chain(core::iter::once(condition.max_parameter()))
                .max()
                .unwrap_or(0),
            Self::SubQuery(sq) => sq.max_parameter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ColumnReference, ValueExpression};

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn join_on_suquery() {
        let query_str = "first INNER JOIN (SELECT name FROM second)";
        let (remaining, table) = table_expression(query_str.as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
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
                condition: Condition::And(vec![])
            },
            table
        );
    }

    #[test]
    fn rename_with_columns() {
        let query_str = "something as o(n)";
        let (remaining, table) = table_expression(query_str.as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            TableExpression::Renamed {
                inner: Box::new(TableExpression::Relation(Identifier("something".into()))),
                name: "o".into(),
                column_rename: Some(vec![Identifier("n".into())])
            },
            table
        );
    }
}
