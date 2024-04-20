use nom::{IResult, Parser};

use crate::{query, Identifier, Parser as _, Query, CompatibleParser};

#[derive(Debug, PartialEq)]
pub struct WithCTE<'s, 'a> {
    pub name: Identifier<'s>,
    pub query: Query<'s, 'a>,
    pub columns: Option<crate::arenas::Vec<'a, Identifier<'s>>>,
}

#[derive(Debug, PartialEq)]
pub struct WithCTEs<'s, 'a> {
    pub parts: crate::arenas::Vec<'a, WithCTE<'s, 'a>>,
    pub recursive: bool,
}

impl<'i, 'a> crate::ArenaParser<'i, 'a> for WithCTEs<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            with_ctes(i, a)
        }
    }
}

impl<'s, 'a> CompatibleParser for WithCTEs<'s, 'a> {
    type StaticVersion = WithCTEs<'static, 'static>;

    fn parameter_count(&self) -> usize {
        0
    }

    fn to_static(&self) -> Self::StaticVersion {
        WithCTEs {
            parts: crate::arenas::Vec::Heap(self
                .parts
                .iter()
                .map(|p| WithCTE {
                    name: p.name.to_static(),
                    query: p.query.to_static(),
                    columns: p
                        .columns
                        .as_ref()
                        .map(|cs| crate::arenas::Vec::Heap(cs.iter().map(|c| c.to_static()).collect())),
                })
                .collect()),
            recursive: self.recursive,
        }
    }
}

pub fn with_ctes<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], WithCTEs<'i, 'a>, nom::error::VerboseError<&'i [u8]>>
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

    let (remaining, parts) = crate::nom_util::bump_separated_list1(
        arena,
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
                crate::nom_util::bump_separated_list0(
                    arena,
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
                        values: vec![ValueExpression::Literal(Literal::SmallInteger(1))].into(),
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
                }].into(),
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
                            values: vec![ValueExpression::Literal(Literal::SmallInteger(2))].into(),
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
                            values: vec![ValueExpression::Literal(Literal::SmallInteger(3))].into(),
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
                ].into(),
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
                        values: vec![ValueExpression::Literal(Literal::SmallInteger(1))].into(),
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
                                }].into(),
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
                                    }).into()
                                )].into())),
                                order_by: None,
                                group_by: None,
                                having: None,
                                limit: None,
                                for_update: None,
                                combine: None
                            })
                        ))
                    }),
                    columns: Some(vec!["n".into()].into()),
                }].into(),
                recursive: true
            }
        );
    }
}
