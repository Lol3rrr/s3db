use nom::{IResult, Parser};

use crate::{query, Identifier, Parser as _, Query};

#[derive(Debug, PartialEq)]
pub struct WithCTE<'s, 'a> {
    pub name: Identifier<'s>,
    pub query: Query<'s, 'a>,
    pub columns: Option<Vec<Identifier<'s>>>,
}

#[derive(Debug, PartialEq)]
pub struct WithCTEs<'s, 'a> {
    pub parts: Vec<WithCTE<'s, 'a>>,
    pub recursive: bool,
}

impl<'s, 'a> WithCTEs<'s, 'a> {
    pub fn parameter_count(&self) -> usize {
        // TODO
        0
    }
}

impl<'i, 's, 'a> crate::ArenaParser<'i, 'a> for WithCTEs<'s, 'a> where 'i: 's {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            with_ctes(i, a)
        }
    }
}

pub fn with_ctes<'i, 's, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], WithCTEs<'s, 'a>, nom::error::VerboseError<&'i [u8]>>
where
    'i: 's,
{
    let (remaining, (_, _, _, recursive)) = nom::sequence::tuple((
        nom::character::complete::multispace0,
        nom::bytes::complete::tag_no_case("WITH"),
        nom::character::complete::multispace1,
        nom::combinator::opt(nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("RECURSIVE"),
            nom::character::complete::multispace1,
        ))),
    ))(i)?;

    let (remaining, parts) = nom::multi::separated_list1(
        nom::sequence::tuple((
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(","),
            nom::character::complete::multispace0,
        )),
        nom::sequence::tuple((
            Identifier::parse(),
            nom::character::complete::multispace1,
            nom::combinator::opt(nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag("("),
                    nom::character::complete::multispace0,
                )),
                nom::multi::separated_list0(
                    nom::sequence::tuple((
                        nom::character::complete::multispace0,
                        nom::bytes::complete::tag(","),
                        nom::character::complete::multispace0,
                    )),
                    Identifier::parse(),
                ),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                    nom::character::complete::multispace0,
                )),
            )),
            nom::bytes::complete::tag_no_case("AS"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag("("),
            nom::character::complete::multispace0,
            move |i| query(i, arena),
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(")"),
        ))
        .map(|(name, _, columns, _, _, _, _, q, _, _)| WithCTE {
            name,
            query: q,
            columns,
        }),
    )(remaining)?;

    Ok((
        remaining,
        WithCTEs {
            parts,
            recursive: recursive.is_some(),
        },
    ))
}

impl<'s, 'a> WithCTEs<'s, 'a> {
    pub fn to_static(&self) -> WithCTEs<'static, 'a> {
        WithCTEs {
            parts: self
                .parts
                .iter()
                .map(|p| WithCTE {
                    name: p.name.to_static(),
                    query: p.query.to_static(),
                    columns: p
                        .columns
                        .as_ref()
                        .map(|cs| cs.iter().map(|c| c.to_static()).collect()),
                })
                .collect(),
            recursive: self.recursive,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BinaryOperator, ColumnReference, Combination, Condition, Literal, Select, TableExpression,
        ValueExpression, macros::arena_parser_parse,
    };

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn single_cte() {
        arena_parser_parse!(
            WithCTEs,
            "WITH something AS (SELECT 1)",
            WithCTEs {
                parts: vec![WithCTE {
                    name: "something".into(),
                    query: Query::Select(Select {
                        values: vec![ValueExpression::Literal(Literal::SmallInteger(1))],
                        table: None,
                        where_condition: None,
                        order_by: None,
                        group_by: None,
                        having: None,
                        limit: None,
                        for_update: None,
                        combine: None
                    }),
                    columns: None,
                }],
                recursive: false,
            }
        );
    }

    #[test]
    fn multiple() {
        arena_parser_parse!(
            WithCTEs,
"
WITH regional_sales AS (
    SELECT 2
), top_regions AS (
    SELECT 3
)",
            WithCTEs {
                parts: vec![
                    WithCTE {
                        name: "regional_sales".into(),
                        query: Query::Select(Select {
                            values: vec![ValueExpression::Literal(Literal::SmallInteger(2))],
                            table: None,
                            where_condition: None,
                            order_by: None,
                            group_by: None,
                            having: None,
                            limit: None,
                            for_update: None,
                            combine: None
                        }),
                        columns: None,
                    },
                    WithCTE {
                        name: "top_regions".into(),
                        query: Query::Select(Select {
                            values: vec![ValueExpression::Literal(Literal::SmallInteger(3))],
                            table: None,
                            where_condition: None,
                            order_by: None,
                            group_by: None,
                            having: None,
                            limit: None,
                            for_update: None,
                            combine: None
                        }),
                        columns: None,
                    }
                ],
                recursive: false,
            }
        );
    }

    #[test]
    fn recursive_cte() {
        arena_parser_parse!(
            WithCTEs,
            "WITH RECURSIVE cte (n) AS ( SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 2 )",
            WithCTEs {
                parts: vec![WithCTE {
                    name: "cte".into(),
                    query: Query::Select(Select {
                        values: vec![ValueExpression::Literal(Literal::SmallInteger(1))],
                        table: None,
                        where_condition: None,
                        order_by: None,
                        group_by: None,
                        having: None,
                        limit: None,
                        for_update: None,
                        combine: Some((
                            Combination::Union,
                            Box::new(Select {
                                values: vec![ValueExpression::Operator {
                                    first: Box::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: None,
                                            column: "n".into(),
                                        }
                                    )),
                                    second: Box::new(ValueExpression::Literal(
                                        Literal::SmallInteger(1)
                                    )),
                                    operator: BinaryOperator::Add,
                                }],
                                table: Some(TableExpression::Relation("cte".into())),
                                where_condition: Some(Condition::And(vec![Condition::Value(
                                    Box::new(ValueExpression::Operator {
                                        first: Box::new(ValueExpression::ColumnReference(
                                            ColumnReference {
                                                relation: None,
                                                column: "n".into()
                                            }
                                        )),
                                        second: Box::new(ValueExpression::Literal(
                                            Literal::SmallInteger(2)
                                        )),
                                        operator: BinaryOperator::Less
                                    })
                                )])),
                                order_by: None,
                                group_by: None,
                                having: None,
                                limit: None,
                                for_update: None,
                                combine: None
                            })
                        ))
                    }),
                    columns: Some(vec!["n".into()]),
                }],
                recursive: true
            }
        );
    }
}
