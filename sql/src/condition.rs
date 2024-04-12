use nom::{IResult, Parser};

use super::{common::value_expression, ValueExpression};

#[derive(Debug, PartialEq, Clone)]
pub enum Condition<'s> {
    And(Vec<Condition<'s>>),
    Or(Vec<Condition<'s>>),
    Value(Box<ValueExpression<'s>>),
}

impl<'s> Condition<'s> {
    pub fn to_static(&self) -> Condition<'static> {
        match self {
            Self::And(parts) => Condition::And(parts.iter().map(|p| p.to_static()).collect()),
            Self::Or(parts) => Condition::Or(parts.iter().map(|p| p.to_static()).collect()),
            Self::Value(v) => Condition::Value(Box::new(v.to_static())),
        }
    }

    pub fn max_parameter(&self) -> usize {
        match self {
            Self::And(p) => p.iter().map(|p| p.max_parameter()).max().unwrap_or(0),
            Self::Or(p) => p.iter().map(|p| p.max_parameter()).max().unwrap_or(0),
            Self::Value(v) => v.max_parameter(),
        }
    }
}

pub fn condition(i: &[u8]) -> IResult<&[u8], Condition<'_>, nom::error::VerboseError<&[u8]>> {
    #[derive(Debug, PartialEq)]
    enum Connector {
        And,
        Or,
    }

    let (mut remaining, tmp) = nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag("("),
            nom::character::complete::multispace0,
            condition,
            nom::character::complete::multispace0,
            nom::bytes::complete::tag(")"),
        ))
        .map(|(_, _, c, _, _)| c),
        value_expression.map(|v| Condition::Value(Box::new(v))),
    ))(i)?;

    let mut conditions = vec![tmp];
    let mut connectors = vec![];

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
                condition,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            ))
            .map(|(_, _, c, _, _)| c),
            value_expression.map(|v| Condition::Value(Box::new(v))),
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

    let mut and_conditions = Vec::new();
    let mut prev_index = 0;
    for end_index in or_indices.chain(core::iter::once(conditions.len())) {
        let parts = &conditions[prev_index..end_index];
        and_conditions.push(Condition::And(parts.to_vec()));
        prev_index = end_index;
    }

    let result = if and_conditions.len() == 1 {
        and_conditions.remove(0)
    } else {
        Condition::Or(and_conditions)
    };

    Ok((remaining, result))
}

#[cfg(test)]
mod tests {

    use pretty_assertions::{assert_eq, assert_str_eq};

    use crate::{
        common::{BinaryOperator, Identifier},
        select::{NullOrdering, Ordering, SelectLimit},
        ColumnReference, FunctionCall, Literal, OrderBy, Select, TableExpression,
    };

    use super::*;

    /*
    #[test]
    fn single_condition() {
        let (remaining, equals_condition) =
            nom::combinator::complete(or_condition)("first = second".as_bytes()).unwrap();
        assert!(
            remaining.is_empty(),
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            OrCondition {
                conditions: vec![AndCondition {
                    conditions: vec![ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier(Cow::Borrowed("first")),
                        })),
                        second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier(Cow::Borrowed("second")),
                        })),
                        operator: BinaryOperator::Equal
                    }]
                }]
            },
            equals_condition
        );
    }

    #[test]
    fn single_condition_no_spaces() {
        let (remaining, equals_condition) =
            nom::combinator::complete(or_condition)("first=second".as_bytes()).unwrap();
        assert!(
            remaining.is_empty(),
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            OrCondition {
                conditions: vec![AndCondition {
                    conditions: vec![ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier(Cow::Borrowed("first")),
                        })),
                        second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier(Cow::Borrowed("second")),
                        })),
                        operator: BinaryOperator::Equal
                    }]
                }]
            },
            equals_condition
        );
    }

    #[test]
    fn multiple_condition() {
        let (remaining, equals_condition) =
            or_condition("first = second AND second = third".as_bytes())
                .map_err(|e| {
                    // TODO
                    e.map_input(|d| core::str::from_utf8(d).unwrap())
                })
                .unwrap();
        assert!(
            remaining.is_empty(),
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            OrCondition {
                conditions: vec![AndCondition {
                    conditions: vec![
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier(Cow::Borrowed("first")),
                            })),
                            second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier(Cow::Borrowed("second")),
                            })),
                            operator: BinaryOperator::Equal
                        },
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier(Cow::Borrowed("second")),
                            })),
                            second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier(Cow::Borrowed("third")),
                            })),
                            operator: BinaryOperator::Equal
                        }
                    ]
                }]
            },
            equals_condition
        );
    }

    #[test]
    fn exists() {
        let (remaining, condition) =
            or_condition("EXISTS (SELECT name FROM users)".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            OrCondition {
                conditions: vec![AndCondition {
                    conditions: vec![ValueExpression::FunctionCall(FunctionCall::Exists {
                        query: Box::new(Select {
                            values: vec![ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("name".into())
                            })],
                            table: Some(TableExpression::Relation(Identifier("users".into()))),
                            where_condition: None,
                            order_by: None,
                            group_by: None,
                            limit: None,
                            for_update: None,
                        })
                    })]
                }]
            },
            condition
        );
    }

    #[test]
    fn or_conditions() {
        let (remaining, condition) =
            or_condition("LOWER(email)=LOWER($1) OR LOWER(login)=LOWER($2)".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
    }

    #[test]
    fn or_ands() {
        let (remaining, paren_condition) =
            or_condition("((namespace = $1) AND (\"key\" LIKE $2))".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        let (remaining, condition) =
            or_condition("(namespace = $1) AND (\"key\" LIKE $2)".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            OrCondition {
                conditions: vec![AndCondition {
                    conditions: vec![
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("namespace".into())
                            })),
                            second: Box::new(ValueExpression::Placeholder(1)),
                            operator: BinaryOperator::Equal
                        },
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("key".into())
                            })),
                            second: Box::new(ValueExpression::Placeholder(2)),
                            operator: BinaryOperator::Like
                        }
                    ]
                }]
            },
            condition
        );

        assert_eq!(paren_condition, condition);
    }

    #[test]
    fn attribute_is_not_null() {
        let (remaining, condition) =
            or_condition("service_account_id IS NOT NULL".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            OrCondition {
                conditions: vec![AndCondition {
                    conditions: vec![ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("service_account_id".into())
                        })),
                        second: Box::new(ValueExpression::Null),
                        operator: BinaryOperator::IsNot
                    }]
                }]
            },
            condition
        );
    }
    */

    #[test]
    fn new_parser_basic_and() {
        let query_str = "name = 'first' AND id = 132";

        let (remaining, condition) = condition(query_str.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Condition::And(vec![
                Condition::Value(Box::new(ValueExpression::Operator {
                    first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("name".into())
                    })),
                    second: Box::new(ValueExpression::Literal(Literal::Str("first".into()))),
                    operator: BinaryOperator::Equal
                })),
                Condition::Value(Box::new(ValueExpression::Operator {
                    first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    })),
                    second: Box::new(ValueExpression::Literal(Literal::SmallInteger(132))),
                    operator: BinaryOperator::Equal
                }))
            ]),
            condition
        );
    }

    #[test]
    fn condition_complex() {
        let query_str = "(org_id = $2 AND configuration_hash = $3) AND (CTID IN (SELECT CTID FROM \"alert_configuration_history\" ORDER BY \"id\" DESC LIMIT 1))";

        let (remaining, condition) = condition(query_str.as_bytes()).unwrap();

        assert_eq!(
            Condition::And(vec![
                Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("org_id".into())
                        })),
                        second: Box::new(ValueExpression::Placeholder(2)),
                        operator: BinaryOperator::Equal
                    })),
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("configuration_hash".into())
                        })),
                        second: Box::new(ValueExpression::Placeholder(3)),
                        operator: BinaryOperator::Equal
                    }))
                ]),
                Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("CTID".into())
                        })),
                        second: Box::new(ValueExpression::SubQuery(Select {
                            values: vec![ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("CTID".into()),
                            })],
                            table: Some(TableExpression::Relation(Identifier(
                                "alert_configuration_history".into()
                            ))),
                            where_condition: None,
                            order_by: Some(vec![Ordering {
                                column: ColumnReference {
                                    relation: None,
                                    column: "id".into(),
                                },
                                order: OrderBy::Descending,
                                nulls: NullOrdering::First,
                            }]),
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
                ))])
            ]),
            condition
        );
    }

    #[test]
    fn mixed_ands_ors() {
        let query_str = "true AND false OR false OR true AND true";

        let (remaining, condition) = condition(query_str.as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            condition,
            Condition::Or(vec![
                Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(true)))),
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(false))))
                ]),
                Condition::And(vec![Condition::Value(Box::new(ValueExpression::Literal(
                    Literal::Bool(false)
                ))),]),
                Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(true)))),
                    Condition::Value(Box::new(ValueExpression::Literal(Literal::Bool(true))))
                ]),
            ]),
        );
    }

    #[test]
    fn single() {
        let query_str = "customer.ID=orders.customer INNER";

        let (remaining, condition) = condition(query_str.as_bytes()).unwrap();
        assert_eq!(
            " INNER".as_bytes(),
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Condition::And(vec![Condition::Value(Box::new(
                ValueExpression::Operator {
                    first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("customer".into())),
                        column: Identifier("ID".into())
                    })),
                    second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("orders".into())),
                        column: Identifier("customer".into())
                    })),
                    operator: BinaryOperator::Equal
                }
            ))]),
            condition
        );
    }

    #[test]
    #[ignore = "TODO"]
    fn testing_parts() {
        let query_str = "scope LIKE 'dashboards:uid:%' AND role_id IN (
                    SELECT id
                    FROM role
                    INNER JOIN (
                        SELECT ur.role_id\n\t\t\t
                        FROM user_role AS ur\n\t\t\t
                        WHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\t
                        UNION\n\t\t\t
                        SELECT br.role_id
                        FROM builtin_role AS br\n\t\t\t
                        WHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t
                    ) as all_role ON role.id = all_role.role_id
                ) AND action = $8";

        let (remaining, condition) = condition(query_str.as_bytes()).unwrap();

        dbg!(&remaining, condition);

        todo!()
    }

    #[test]
    #[ignore = "TODO"]
    fn testing_with_remainder2() {
        let query_str = "(
            (dashboard.uid IN (
                SELECT substr(scope, 16)
                FROM permission
                WHERE scope LIKE 'dashboards:uid:%' AND role_id IN (
                    SELECT id
                    FROM role
                    INNER JOIN (
                        SELECT ur.role_id\n\t\t\t
                        FROM user_role AS ur\n\t\t\t
                        WHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\t
                        UNION\n\t\t\t
                        SELECT br.role_id
                        FROM builtin_role AS br\n\t\t\t
                        WHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t
                    ) as all_role ON role.id = all_role.role_id
                ) AND action = $8
            ) AND NOT dashboard.is_folder) 
        ) ";

        let (remaining, condition) = condition(query_str.as_bytes()).unwrap();

        dbg!(&remaining, &condition);
        todo!()
    }

    #[test]
    #[ignore = "TODO"]
    fn testing_with_remainder() {
        let query_str = "(
            (dashboard.uid IN (
                SELECT substr(scope, 16)
                FROM permission
                WHERE scope LIKE 'dashboards:uid:%' AND role_id IN (
                    SELECT id
                    FROM role
                    INNER JOIN (
                        SELECT ur.role_id\n\t\t\t
                        FROM user_role AS ur\n\t\t\t
                        WHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\t
                        UNION\n\t\t\t
                        SELECT br.role_id
                        FROM builtin_role AS br\n\t\t\t
                        WHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t
                    ) as all_role ON role.id = all_role.role_id
                ) AND action = $8
            ) AND NOT dashboard.is_folder
            ) OR (dashboard.folder_id IN (
                SELECT d.id
                FROM dashboard as d
                WHERE d.org_id = $9 AND d.uid IN (
                    SELECT substr(scope, 13)
                    FROM permission
                    WHERE scope LIKE 'folders:uid:%' AND role_id IN (
                        SELECT id
                        FROM role
                        INNER JOIN (
                            SELECT ur.role_id\n\t\t\t
                            FROM user_role AS ur\n\t\t\t
                            WHERE ur.user_id = $10\n\t\t\tAND (ur.org_id = $11 OR ur.org_id = $12)\n\t\t
                            UNION\n\t\t\t
                            SELECT br.role_id
                            FROM builtin_role AS br\n\t\t\t
                            WHERE br.role IN ($13, $14)\n\t\t\tAND (br.org_id = $15 OR br.org_id = $16)\n\t\t
                        ) as all_role ON role.id = all_role.role_id
                    ) AND action = $17
                    )
                ) AND NOT dashboard.is_folder
            )
        ) AND dashboard.org_id=$18 AND dashboard.title ILIKE $19 AND dashboard.is_folder = false AND dashboard.folder_id = $20
        ORDER BY";

        let (remaining, condition) = condition(query_str.as_bytes()).unwrap();

        dbg!(&remaining, &condition);
        todo!()
    }

    #[test]
    fn testing() {
        let query_str = "(
(dashboard.uid IN (
    SELECT substr(scope, 16)
    FROM permission
    WHERE scope LIKE 'dashboards:uid:%' AND role_id IN (
        SELECT id
        FROM role
        INNER JOIN (
            SELECT ur.role_id
            FROM user_role AS ur
            WHERE ur.user_id = $1 AND (ur.org_id = $2 OR ur.org_id = $3)
            UNION
            SELECT br.role_id
            FROM builtin_role AS br
            WHERE br.role IN ($4, $5) AND (br.org_id = $6 OR br.org_id = $7)
        ) as all_role ON role.id = all_role.role_id
    ) AND action = $8
    ) AND NOT dashboard.is_folder
) OR (dashboard.folder_id IN (
    SELECT d.id
    FROM dashboard as d
    WHERE d.org_id = $9 AND d.uid IN (
        SELECT substr(scope, 13)
        FROM permission
        WHERE scope LIKE 'folders:uid:%' AND role_id IN (
            SELECT id
            FROM role
            INNER JOIN (
                SELECT ur.role_id
                FROM user_role AS ur
                WHERE ur.user_id = $10 AND (ur.org_id = $11 OR ur.org_id = $12)
                UNION
                SELECT br.role_id
                FROM builtin_role AS br
                WHERE br.role IN ($13, $14) AND (br.org_id = $15 OR br.org_id = $16)
            ) as all_role ON role.id = all_role.role_id
        ) AND action = $17
    )
    ) AND NOT dashboard.is_folder
)
)";

        let (remaining, condition) = condition(query_str.as_bytes())
            .map_err(|e| {
                match e {
                    nom::Err::Error(e) => {
                        for entry in e.errors.iter() {
                            println!(
                                "- [{:?}] {:?}\n",
                                entry.1,
                                core::str::from_utf8(entry.0).unwrap()
                            );
                        }
                    }
                    nom::Err::Failure(f) => {
                        for entry in f.errors.iter() {
                            println!(
                                "- [{:?}] {:?}\n",
                                entry.1,
                                core::str::from_utf8(entry.0).unwrap()
                            );
                        }
                    }
                    nom::Err::Incomplete(_) => todo!(),
                };
                ()
            })
            .unwrap();
        assert_eq!(&[] as &[u8], remaining);

        let _ = condition;
    }

    #[test]
    fn not_field() {
        let (remaining, condition) = condition("NOT table.field".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Condition::And(vec![Condition::Value(Box::new(ValueExpression::Not(
                Box::new(ValueExpression::ColumnReference(ColumnReference {
                    relation: Some("table".into()),
                    column: "field".into()
                }))
            )))]),
            condition
        );
    }
}
