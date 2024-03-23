use nom::{IResult, Parser};

use crate::sql::{self, common::identifier, select::select, Condition, Identifier};

use super::Select;

#[derive(Debug, PartialEq, Clone)]
pub enum TableExpression<'s> {
    Relation(Identifier<'s>),
    Renamed {
        inner: Box<TableExpression<'s>>,
        name: Identifier<'s>,
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
}

pub fn table_expression(i: &[u8]) -> IResult<&[u8], TableExpression> {
    fn base(i: &[u8]) -> IResult<&[u8], TableExpression> {
        let (remaining, table) = nom::branch::alt((
            nom::sequence::tuple((
                nom::bytes::complete::tag("("),
                nom::character::complete::multispace0,
                select,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            ))
            .map(|(_, _, query, _, _)| TableExpression::SubQuery(Box::new(query))),
            identifier.map(|ident| TableExpression::Relation(ident)),
        ))(i)?;

        match nom::branch::alt((
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("AS"),
                nom::character::complete::multispace1,
                identifier,
            ))
            .map(|(_, _, _, ident)| ident),
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
            .map(|(_, _, ident)| ident),
        ))(remaining)
        {
            Ok((remaining, name)) => Ok((
                remaining,
                TableExpression::Renamed {
                    inner: Box::new(table),
                    name,
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
        ))(outer_remaining);

        let (remaining, join_kind) = match join_res {
            Ok(v) => v,
            Err(_) => break,
        };

        let (remaining, other_table) = base(remaining)?;

        let (remaining, _) = nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ON"),
            nom::character::complete::multispace1,
        ))(remaining)?;

        let (remaining, condition) = sql::condition::condition(remaining)?;

        outer_remaining = remaining;
        table = TableExpression::Join {
            left: Box::new(table),
            right: Box::new(other_table),
            kind: join_kind,
            condition,
        };
    }

    Ok((outer_remaining, table))
}

impl<'s> TableExpression<'s> {
    pub fn to_static(&self) -> TableExpression<'static> {
        match self {
            Self::Relation(r) => TableExpression::Relation(r.to_static()),
            Self::Renamed { inner, name } => TableExpression::Renamed {
                inner: Box::new(inner.to_static()),
                name: name.to_static(),
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
}
