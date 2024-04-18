use nom::{IResult, Parser};

use crate::{dialects, CompatibleParser, Parser as _};

use super::{condition::condition, Condition, Identifier};

#[derive(Debug, PartialEq)]
pub struct Delete<'s> {
    pub table: Identifier<'s>,
    pub condition: Option<Condition<'s>>,
}

impl<'s> CompatibleParser<dialects::Postgres> for Delete<'s> {
    type StaticVersion = Delete<'static>;

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
impl<'s> Delete<'s> {
    pub fn max_parameter(&self) -> usize {
        Self::parameter_count(self)
    }
}

pub fn delete(i: &[u8]) -> IResult<&[u8], Delete<'_>, nom::error::VerboseError<&[u8]>> {
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
            condition,
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
        BinaryOperator, ColumnReference, Literal, Select, TableExpression, ValueExpression,
    };

    use super::*;

    #[test]
    fn delete_no_condition() {
        let query = "DELETE FROM dashboard_tag";
        let (remaining, delete) = delete(query.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Delete {
                table: Identifier("dashboard_tag".into()),
                condition: None,
            },
            delete
        );
    }

    #[test]
    fn delete_basic_condition() {
        let query = "DELETE FROM dashboard_tag WHERE name = 'something'";
        let (remaining, delete) = delete(query.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Delete {
                table: Identifier("dashboard_tag".into()),
                condition: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("name".into())
                        })),
                        second: Box::new(ValueExpression::Literal(Literal::Str(
                            "something".into()
                        ))),
                        operator: BinaryOperator::Equal
                    }
                ))]))
            },
            delete
        );
    }

    #[test]
    fn delete_with_subquery() {
        let query =
            "DELETE FROM dashboard_tag WHERE dashboard_id NOT IN (SELECT id FROM dashboard)";
        let (remaining, delete) = delete(query.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Delete {
                table: Identifier("dashboard_tag".into()),
                condition: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("dashboard_id".into())
                        })),
                        second: Box::new(ValueExpression::SubQuery(Select {
                            values: vec![ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("id".into())
                            })],
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
                    }
                ))])),
            },
            delete
        );
    }

    #[test]
    fn delete_testing() {
        let query_str = "DELETE from user_auth_token WHERE created_at <= $1 OR rotated_at <= $2";
        let (remaining, delete) = delete(query_str.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Delete {
                table: Identifier("user_auth_token".into()),
                condition: Some(Condition::Or(vec![
                    Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("created_at".into())
                            })),
                            second: Box::new(ValueExpression::Placeholder(1)),
                            operator: BinaryOperator::LessEqual
                        }
                    ))]),
                    Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("rotated_at".into())
                            })),
                            second: Box::new(ValueExpression::Placeholder(2)),
                            operator: BinaryOperator::LessEqual
                        }
                    ))])
                ])),
            },
            delete
        );
    }
}
