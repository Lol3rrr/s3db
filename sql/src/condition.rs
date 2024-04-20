use nom::{IResult, Parser};

use super::ValueExpression;
use crate::{ArenaParser, CompatibleParser, arenas::Boxed};

#[derive(Debug, PartialEq)]
pub enum Condition<'s, 'a> {
    And(crate::arenas::Vec<'a, Condition<'s, 'a>>),
    Or(crate::arenas::Vec<'a, Condition<'s, 'a>>),
    Value(Boxed<'a, ValueExpression<'s, 'a>>),
}

impl<'s, 'a> Condition<'s, 'a> {
    pub fn max_parameter(&self) -> usize {
        self.parameter_count()
    }
}

impl<'s, 'a> CompatibleParser for Condition<'s, 'a> {
    type StaticVersion = Condition<'static, 'static>;

    fn parameter_count(&self) -> usize {
        match self {
            Self::And(p) => p.iter().map(|p| p.max_parameter()).max().unwrap_or(0),
            Self::Or(p) => p.iter().map(|p| p.max_parameter()).max().unwrap_or(0),
            Self::Value(v) => v.parameter_count(),
        }
    }

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::And(parts) => Condition::And(crate::arenas::Vec::Heap(parts.iter().map(|p| p.to_static()).collect())),
            Self::Or(parts) => Condition::Or(crate::arenas::Vec::Heap(parts.iter().map(|p| p.to_static()).collect())),
            Self::Value(v) => Condition::Value(v.to_static()),
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for Condition<'i, 'a>  {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            condition(i, a)
        }
    }
}

#[deprecated]
pub fn condition<'i, 'a>(i: &'i [u8], arena: &'a bumpalo::Bump) -> IResult<&'i [u8], Condition<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    #[derive(Debug, PartialEq)]
    enum Connector {
        And,
        Or,
    }

    let (mut remaining, tmp) = nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag("("),
            nom::character::complete::multispace0,
            Condition::parse_arena(arena),
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(")"),
        ))
        .map(|(_, _, c, _, _)| c),
        ValueExpression::parse_arena(arena).map(|v| Condition::Value(crate::arenas::Boxed::arena(arena, v))),
    ))(i)?;

    let mut conditions = bumpalo::vec![in arena; tmp];
    let mut connectors = bumpalo::collections::Vec::new_in(arena);

    loop {
        let (rem, connector) = match nom::branch::alt::<_, _, nom::error::Error<&[u8]>, _>((
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("AND").map(|_| Connector::And),
                nom::character::complete::multispace1,
            ))
            .map(|(_, c, _)| c),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag_no_case("OR").map(|_| Connector::Or),
                nom::character::complete::multispace1,
            ))
            .map(|(_, c, _)| c),
        ))(remaining)
        {
            Ok(r) => r,
            Err(_) => break,
        };

        let (rem, other) = match nom::branch::alt((
            nom::sequence::tuple((
                nom::bytes::complete::tag("("),
                nom::character::complete::multispace0,
                Condition::parse_arena(arena),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            ))
            .map(|(_, _, c, _, _)| c),
            ValueExpression::parse_arena(arena).map(|v| Condition::Value(crate::arenas::Boxed::arena(arena, v))),
        ))(rem)
        {
            Ok(c) => c,
            Err(_) => break,
        };

        remaining = rem;

        conditions.push(other);
        connectors.push(connector);
    }

    let or_indices = connectors
        .into_iter()
        .enumerate()
        .filter(|(_, con)| con == &Connector::Or)
        .map(|(i, _)| i + 1);

    let mut and_conditions = bumpalo::collections::Vec::new_in(arena);
    let mut prev_index = 0;

    let conditions_length = conditions.len();
    let mut condition_iter = conditions.into_iter();
    for end_index in or_indices.chain(core::iter::once(conditions_length)) {
        let length = end_index - prev_index;

        and_conditions.push(Condition::And({
            let mut tmp = bumpalo::collections::Vec::with_capacity_in(length, arena);
            tmp.extend((&mut condition_iter).take(length));
            crate::arenas::Vec::Arena(tmp)
        }));
        prev_index = end_index;
    }

    let result = if and_conditions.len() == 1 {
        and_conditions.remove(0)
    } else {
        Condition::Or(crate::arenas::Vec::Arena(and_conditions))
    };

    Ok((remaining, result))
}

#[cfg(test)]
mod tests {

    use pretty_assertions::assert_eq;

    use crate::{
        common::{BinaryOperator, Identifier},
        macros::arena_parser_parse,
        select::{NullOrdering, OrderAttribute, Ordering, SelectLimit},
        ColumnReference, Literal, OrderBy, Select, TableExpression,
    };

    use super::*;

    #[test]
    fn new_parser_basic_and() {
        arena_parser_parse!(
            Condition,
            "name = 'first' AND id = 132",
            Condition::And(vec![
                Condition::Value(Boxed::new(ValueExpression::Operator {
                    first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("name".into())
                    })),
                    second: Boxed::new(ValueExpression::Literal(Literal::Str("first".into()))),
                    operator: BinaryOperator::Equal
                }).into()),
                Condition::Value(Boxed::new(ValueExpression::Operator {
                    first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    })),
                    second: Boxed::new(ValueExpression::Literal(Literal::SmallInteger(132))),
                    operator: BinaryOperator::Equal
                }).into())
            ].into())
        );
    }

    #[test]
    fn condition_complex() {
        arena_parser_parse!(
            Condition,
            "(org_id = $2 AND configuration_hash = $3) AND (CTID IN (SELECT CTID FROM \"alert_configuration_history\" ORDER BY \"id\" DESC LIMIT 1))",
            Condition::And(vec![
                Condition::And(vec![
                    Condition::Value(Boxed::new(ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("org_id".into())
                        })),
                        second: Boxed::new(ValueExpression::Placeholder(2)),
                        operator: BinaryOperator::Equal
                    }).into()),
                    Condition::Value(Boxed::new(ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("configuration_hash".into())
                        })),
                        second: Boxed::new(ValueExpression::Placeholder(3)),
                        operator: BinaryOperator::Equal
                    }).into())
                ].into()),
                Condition::And(vec![Condition::Value(Boxed::new(
                    ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("CTID".into())
                        })),
                        second: Boxed::new(ValueExpression::SubQuery(Select {
                            values: vec![ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("CTID".into()),
                            })].into(),
                            table: Some(TableExpression::Relation(Identifier(
                                "alert_configuration_history".into()
                            ))),
                            where_condition: None,
                            order_by: Some(vec![Ordering {
                                column: OrderAttribute::ColumnRef(ColumnReference {
                                    relation: None,
                                    column: "id".into(),
                                }),
                                order: OrderBy::Descending,
                                nulls: NullOrdering::First,
                            }].into()),
                            group_by: None,
                            having: None,
                            limit: Some(SelectLimit {
                                limit: 1,
                                offset: None
                            }),
                            for_update: None,
                            combine: None
                        })),
                        operator: BinaryOperator::In
                    }
                ).into())].into())
            ].into())
        );
    }

    #[test]
    fn mixed_ands_ors() {
        arena_parser_parse!(
            Condition,
            "true AND false OR false OR true AND true",
            Condition::Or(vec![
                Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(true))).into()),
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(false))).into())
                ].into()),
                Condition::And(vec![Condition::Value(Box::new(ValueExpression::Literal(
                    Literal::Bool(false)
                )).into()),].into()),
                Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(true))).into()),
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(true))).into())
                ].into()),
            ].into())
        );
    }

    #[test]
    fn single() {
        arena_parser_parse!(
            Condition,
            "customer.ID=orders.customer INNER",
            Condition::And(vec![Condition::Value(Boxed::new(
                ValueExpression::Operator {
                    first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("customer".into())),
                        column: Identifier("ID".into())
                    })),
                    second: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("orders".into())),
                        column: Identifier("customer".into())
                    })),
                    operator: BinaryOperator::Equal
                }
            ).into())].into()),
            " INNER".as_bytes()
        );
    }

    #[test]
    fn not_field() {
        arena_parser_parse!(
            Condition,
            "NOT table.field",
            Condition::And(vec![Condition::Value(Boxed::new(ValueExpression::Not(
                Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: Some("table".into()),
                    column: "field".into()
                }))
            )).into())].into())
        );
    }
}
