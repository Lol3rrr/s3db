use nom::{IResult, Parser};

use crate::{ArenaParser, CompatibleParser, Parser as _};

use super::{Condition, Identifier};

#[derive(Debug, PartialEq)]
pub struct Delete<'s, 'a> {
    pub table: Identifier<'s>,
    pub condition: Option<Condition<'s, 'a>>,
}

impl<'s, 'a> CompatibleParser for Delete<'s, 'a> {
    type StaticVersion = Delete<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
        Delete {
            table: self.table.to_static(),
            condition: self.condition.as_ref().map(|c| c.to_static()),
        }
    }

    fn parameter_count(&self) -> usize {
        self.condition
            .as_ref()
            .map(|c| c.max_parameter())
            .unwrap_or(0)
    }
}
impl<'s, 'a> Delete<'s, 'a> {
    pub fn max_parameter(&self) -> usize {
        Self::parameter_count(self)
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for Delete<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| delete(i, a)
    }
}

pub fn delete<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], Delete<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    let (remaining, (_, _, _, _, table)) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("DELETE"),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("FROM"),
        nom::character::complete::multispace1,
        Identifier::parse(),
    ))(i)?;

    let (remaining, cond) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("WHERE"),
            nom::character::complete::multispace1,
            Condition::parse_arena(arena),
        ))
        .map(|(_, _, _, cond)| cond),
    )(remaining)?;

    Ok((
        remaining,
        Delete {
            table,
            condition: cond,
        },
    ))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{
        arenas::Boxed, macros::arena_parser_parse, BinaryOperator, ColumnReference, Literal,
        Select, TableExpression, ValueExpression,
    };

    use super::*;

    #[test]
    fn delete_no_condition() {
        arena_parser_parse!(
            Delete,
            "DELETE FROM dashboard_tag",
            Delete {
                table: Identifier("dashboard_tag".into()),
                condition: None,
            }
        );
    }

    #[test]
    fn delete_basic_condition() {
        arena_parser_parse!(
            Delete,
            "DELETE FROM dashboard_tag WHERE name = 'something'",
            Delete {
                table: Identifier("dashboard_tag".into()),
                condition: Some(Condition::And(
                    vec![Condition::Value(crate::arenas::Boxed::Heap(Box::new(
                        ValueExpression::Operator {
                            first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("name".into())
                            })),
                            second: Boxed::new(ValueExpression::Literal(Literal::Str(
                                "something".into()
                            ))),
                            operator: BinaryOperator::Equal
                        }
                    )))]
                    .into()
                ))
            }
        );
    }

    #[test]
    fn delete_with_subquery() {
        arena_parser_parse!(
            Delete,
            "DELETE FROM dashboard_tag WHERE dashboard_id NOT IN (SELECT id FROM dashboard)",
            Delete {
                table: Identifier("dashboard_tag".into()),
                condition: Some(Condition::And(
                    vec![Condition::Value(Boxed::new(ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("dashboard_id".into())
                        })),
                        second: Boxed::new(ValueExpression::SubQuery(Select {
                            values: vec![ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("id".into())
                            })]
                            .into(),
                            table: Some(TableExpression::Relation(Identifier("dashboard".into()))),
                            where_condition: None,
                            order_by: None,
                            group_by: None,
                            having: None,
                            limit: None,
                            for_update: None,
                            combine: None
                        })),
                        operator: BinaryOperator::NotIn
                    }))]
                    .into()
                )),
            }
        );
    }

    #[test]
    fn delete_testing() {
        arena_parser_parse!(
            Delete,
            "DELETE from user_auth_token WHERE created_at <= $1 OR rotated_at <= $2",
            Delete {
                table: Identifier("user_auth_token".into()),
                condition: Some(Condition::Or(
                    vec![
                        Condition::And(
                            vec![Condition::Value(
                                Box::new(ValueExpression::Operator {
                                    first: Boxed::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: None,
                                            column: Identifier("created_at".into())
                                        }
                                    )),
                                    second: Boxed::new(ValueExpression::Placeholder(1)),
                                    operator: BinaryOperator::LessEqual
                                })
                                .into()
                            )]
                            .into()
                        ),
                        Condition::And(
                            vec![Condition::Value(
                                Box::new(ValueExpression::Operator {
                                    first: Boxed::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: None,
                                            column: Identifier("rotated_at".into())
                                        }
                                    )),
                                    second: Boxed::new(ValueExpression::Placeholder(2)),
                                    operator: BinaryOperator::LessEqual
                                })
                                .into()
                            )]
                            .into()
                        )
                    ]
                    .into()
                )),
            }
        );
    }
}
