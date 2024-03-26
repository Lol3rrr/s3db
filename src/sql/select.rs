use nom::{IResult, Parser};

use crate::sql::{common::value_expressions, condition::condition};

use super::{
    common::{column_reference, ValueExpression},
    condition::Condition,
    ColumnReference,
};

mod combine;
use combine::combine;
pub use combine::Combination;

mod tableexpr;
use tableexpr::table_expression;
pub use tableexpr::{JoinKind, TableExpression};

mod order;
use order::order_by;
pub use order::{NullOrdering, OrderBy, Ordering};

/// A single Select Query with all the containing parts
///
/// # References:
/// * [Postgres Docs](https://www.postgresql.org/docs/16/sql-select.html)
#[derive(Debug, PartialEq, Clone)]
pub struct Select<'s> {
    /// The Values that will be returned by the Query for each Row
    pub values: Vec<ValueExpression<'s>>,
    /// The base table expression to select values from
    pub table: Option<TableExpression<'s>>,
    /// A condition to filter out unwanted queries
    pub where_condition: Option<Condition<'s>>,
    pub order_by: Option<Vec<Ordering<'s>>>,
    pub group_by: Option<Vec<ColumnReference<'s>>>,
    pub having: Option<Condition<'s>>,
    pub limit: Option<SelectLimit>,
    pub for_update: Option<()>,
    pub combine: Option<(Combination, Box<Select<'s>>)>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectLimit {
    pub limit: usize,
    pub offset: Option<usize>,
}

impl<'s> Select<'s> {
    pub fn to_static(&self) -> Select<'static> {
        Select {
            values: self.values.iter().map(|v| v.to_static()).collect(),
            table: self.table.as_ref().map(|t| t.to_static()),
            where_condition: self.where_condition.as_ref().map(|c| c.to_static()),
            order_by: self.order_by.as_ref().map(|orders| {
                orders
                    .iter()
                    .map(|order| Ordering {
                        column: order.column.to_static(),
                        order: order.order.clone(),
                        nulls: order.nulls.clone(),
                    })
                    .collect()
            }),
            group_by: self
                .group_by
                .as_ref()
                .map(|idents| idents.iter().map(|i| i.to_static()).collect()),
            having: self.having.as_ref().map(|h| h.to_static()),
            limit: self.limit.clone(),
            for_update: self.for_update.clone(),
            combine: self
                .combine
                .as_ref()
                .map(|(c, s)| (c.clone(), Box::new(s.to_static()))),
        }
    }

    pub fn max_parameter(&self) -> usize {
        let value_max = self
            .values
            .iter()
            .map(|v| v.max_parameter())
            .max()
            .unwrap_or(0);
        let table_max = self.table.as_ref().map(|t| t.max_parameter()).unwrap_or(0);
        let where_max = self
            .where_condition
            .as_ref()
            .map(|c| c.max_parameter())
            .unwrap_or(0);
        let comb_max = self
            .combine
            .as_ref()
            .map(|(_, s)| s.max_parameter())
            .unwrap_or(0);

        [value_max, table_max, where_max, comb_max]
            .into_iter()
            .max()
            .unwrap_or(0)
    }
}

pub fn select(i: &[u8]) -> IResult<&[u8], Select<'_>, nom::error::VerboseError<&[u8]>> {
    let (remaining, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("SELECT"),
        nom::character::complete::multispace1,
    ))(i)?;

    let (
        remaining,
        (
            _distinct,
            _,
            value_exprs,
            table,
            where_exists,
            something,
            order_by,
            group_by,
            having,
            limit,
            for_update,
        ),
    ) = nom::error::context(
        "Parsing Select",
        nom::sequence::tuple((
            nom::combinator::opt(nom::bytes::complete::tag_no_case("distinct")),
            nom::character::complete::multispace0,
            value_expressions,
            nom::combinator::opt(nom::combinator::map(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag_no_case("from"),
                    nom::combinator::cut(nom::error::context(
                        "FROM",
                        nom::sequence::tuple((
                            nom::character::complete::multispace1,
                            table_expression,
                        )),
                    )),
                )),
                |(_, _, (_, table))| table,
            )),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag_no_case("where"),
                    nom::error::context(
                        "where condition",
                        nom::combinator::cut(nom::sequence::tuple((
                            nom::combinator::opt(nom::character::complete::multispace1),
                            condition,
                        ))),
                    ),
                ))
                .map(|(_, _, (_, cond))| cond),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    combine,
                    nom::character::complete::multispace1,
                    select,
                ))
                .map(|(_, c, _, s)| (c, Box::new(s))),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((nom::character::complete::multispace1, order_by))
                    .map(|(_, order)| order),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("GROUP"),
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("BY"),
                    nom::character::complete::multispace1,
                    nom::multi::separated_list1(
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag(","),
                            nom::character::complete::multispace0,
                        )),
                        column_reference,
                    ),
                ))
                .map(|(_, _, _, _, _, attributes)| attributes),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("HAVING"),
                    nom::character::complete::multispace1,
                    condition,
                ))
                .map(|(_, _, _, cond)| cond),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("LIMIT"),
                    nom::character::complete::multispace1,
                    nom::character::complete::digit1
                        .map(|raw| core::str::from_utf8(raw).unwrap().parse::<usize>().unwrap()),
                    nom::combinator::opt(
                        nom::sequence::tuple((
                            nom::character::complete::multispace1,
                            nom::bytes::complete::tag_no_case("OFFSET"),
                            nom::character::complete::multispace1,
                            nom::character::complete::digit1.map(|raw| {
                                core::str::from_utf8(raw).unwrap().parse::<usize>().unwrap()
                            }),
                        ))
                        .map(|(_, _, _, v)| v),
                    ),
                ))
                .map(|(_, _, _, count, offset)| SelectLimit {
                    limit: count,
                    offset,
                }),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("FOR"),
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("UPDATE"),
                ))
                .map(|_| ()),
            ),
        )),
    )(remaining)?;

    Ok((
        remaining,
        Select {
            values: value_exprs,
            table,
            where_condition: where_exists,
            order_by,
            group_by,
            having,
            limit,
            for_update,
            combine: something,
        },
    ))
}

#[cfg(test)]
mod tests {
    use crate::sql::{self, AggregateExpression, BinaryOperator, Identifier, Literal, Query};

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn count_select() {
        let (_, select) = select("select count(*) from pgbench_branches".as_bytes()).unwrap();
        dbg!(select);
    }

    #[test]
    fn select_where_and() {
        let (remain, select) = select(
            "SELECT 1 FROM \"pg_indexes\" WHERE \"tablename\"=$1 AND \"indexname\"=$2".as_bytes(),
        )
        .unwrap();
        dbg!(remain, select);
    }

    #[test]
    fn select_with_limit() {
        let (remaining, select) = select("SELECT name FROM users LIMIT 1".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("name".into())
                })],
                table: Some(TableExpression::Relation(Identifier("users".into()))),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: Some(SelectLimit {
                    limit: 1,
                    offset: None
                }),
                for_update: None,
                combine: None,
            },
            select
        );
    }

    #[test]
    fn select_with_alias_without_as() {
        let (remaining, select) = select("SELECT u.name FROM user u".as_bytes()).unwrap();

        dbg!(&remaining, &select);

        assert_eq!(
            Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: Some(Identifier("u".into())),
                    column: Identifier("name".into())
                })],
                table: Some(TableExpression::Renamed {
                    inner: Box::new(TableExpression::Relation(Identifier("user".into()))),
                    name: Identifier("u".into())
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            },
            select
        );
    }

    #[test]
    fn select_where_not_null() {
        let (remaining, select) = select(
            "SELECT COUNT(*) FROM \"api_key\"WHERE service_account_id IS NOT NULL".as_bytes(),
        )
        .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Select {
                values: vec![ValueExpression::AggregateExpression(
                    AggregateExpression::Count(Box::new(ValueExpression::All))
                )],
                table: Some(TableExpression::Relation(Identifier("api_key".into()))),
                where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("service_account_id".into())
                        })),
                        second: Box::new(ValueExpression::Null),
                        operator: sql::BinaryOperator::IsNot
                    }
                ))])),
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            },
            select
        );
    }

    #[test]
    fn select_multiple_inner_joins() {
        let (remaining, select) = select("SELECT * FROM customer INNER JOIN orders ON customer.ID=orders.customer INNER JOIN products ON orders.product=products.ID".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        dbg!(&select);

        assert_eq!(
            Select {
                values: vec![ValueExpression::All],
                table: Some(TableExpression::Join {
                    left: Box::new(TableExpression::Join {
                        left: Box::new(TableExpression::Relation(Identifier("customer".into()))),
                        right: Box::new(TableExpression::Relation(Identifier("orders".into()))),
                        kind: JoinKind::Inner,
                        condition: Condition::And(vec![Condition::Value(Box::new(
                            ValueExpression::Operator {
                                first: Box::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: Some(Identifier("customer".into())),
                                        column: Identifier("ID".into())
                                    }
                                )),
                                second: Box::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: Some(Identifier("orders".into())),
                                        column: Identifier("customer".into())
                                    }
                                )),
                                operator: BinaryOperator::Equal
                            }
                        ))]),
                    }),
                    right: Box::new(TableExpression::Relation(Identifier("products".into()))),
                    kind: JoinKind::Inner,
                    condition: Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some(Identifier("orders".into())),
                                column: Identifier("product".into())
                            })),
                            second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some(Identifier("products".into())),
                                column: Identifier("ID".into())
                            })),
                            operator: BinaryOperator::Equal,
                        }
                    ))])
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None,
            },
            select
        );
    }

    #[test]
    fn select_multiple_joins() {
        let (remaining, select) = select("SELECT * FROM customer JOIN orders ON customer.ID=orders.customer JOIN products ON orders.product=products.ID".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Select {
                values: vec![ValueExpression::All],
                table: Some(TableExpression::Join {
                    left: Box::new(TableExpression::Join {
                        left: Box::new(TableExpression::Relation(Identifier("customer".into()))),
                        right: Box::new(TableExpression::Relation(Identifier("orders".into()))),
                        kind: JoinKind::Inner,
                        condition: Condition::And(vec![Condition::Value(Box::new(
                            ValueExpression::Operator {
                                first: Box::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: Some(Identifier("customer".into())),
                                        column: Identifier("ID".into())
                                    }
                                )),
                                second: Box::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: Some(Identifier("orders".into())),
                                        column: Identifier("customer".into())
                                    }
                                )),
                                operator: BinaryOperator::Equal
                            }
                        ))]),
                    }),
                    right: Box::new(TableExpression::Relation(Identifier("products".into()))),
                    kind: JoinKind::Inner,
                    condition: Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some(Identifier("orders".into())),
                                column: Identifier("product".into())
                            })),
                            second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some(Identifier("products".into())),
                                column: Identifier("ID".into())
                            })),
                            operator: BinaryOperator::Equal,
                        }
                    ))])
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            },
            select
        );
    }

    #[test]
    fn select_order_by_column_reference() {
        let query_str = "SELECT name FROM users ORDER BY users.age";

        let select = match Query::parse(query_str.as_bytes()) {
            Ok(Query::Select(s)) => s,
            other => panic!("{:?}", other),
        };

        assert_eq!(
            Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("name".into())
                })],
                table: Some(TableExpression::Relation(Identifier("users".into()))),
                where_condition: None,
                order_by: Some(vec![Ordering {
                    column: ColumnReference {
                        relation: Some("users".into()),
                        column: "age".into(),
                    },
                    order: OrderBy::Ascending,
                    nulls: NullOrdering::Last
                }]),
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            },
            select
        );
    }

    #[test]
    fn union_all() {
        let query_str = "SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 2";

        let select = match Query::parse(query_str.as_bytes()) {
            Ok(Query::Select(s)) => s,
            other => panic!("{:?}", other),
        };

        assert_eq!(
            Select {
                values: vec![ValueExpression::Literal(sql::Literal::SmallInteger(1))],
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
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: "n".into()
                            })),
                            second: Box::new(ValueExpression::Literal(sql::Literal::SmallInteger(
                                1
                            ))),
                            operator: BinaryOperator::Add,
                        }],
                        table: Some(TableExpression::Relation("cte".into())),
                        where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                            ValueExpression::Operator {
                                first: Box::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: None,
                                        column: "n".into()
                                    }
                                )),
                                second: Box::new(ValueExpression::Literal(
                                    sql::Literal::SmallInteger(2)
                                )),
                                operator: BinaryOperator::Less
                            }
                        ))])),
                        order_by: None,
                        group_by: None,
                        having: None,
                        limit: None,
                        for_update: None,
                        combine: None
                    })
                )),
            },
            select
        );
    }

    #[test]
    fn having_clause() {
        let query_str = "SELECT count(*) FROM orders GROUP BY uid HAVING uid > 0";

        let (remaining, select) = select(query_str.as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);

        assert_eq!(
            Select {
                values: vec![ValueExpression::AggregateExpression(
                    AggregateExpression::Count(Box::new(ValueExpression::All))
                )],
                table: Some(TableExpression::Relation("orders".into())),
                where_condition: None,
                order_by: None,
                group_by: Some(vec![ColumnReference {
                    relation: None,
                    column: "uid".into(),
                }]),
                having: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: "uid".into(),
                        })),
                        second: Box::new(ValueExpression::Literal(Literal::SmallInteger(0))),
                        operator: BinaryOperator::Greater
                    }
                ))])),
                limit: None,
                for_update: None,
                combine: None,
            },
            select
        );
    }

    #[test]
    fn errors() {
        let tmp = select("SELECT something FROM".as_bytes()).unwrap_err();
        assert!(
            matches!(tmp, nom::Err::Failure(_)),
            "Expected Failure: {:?}",
            tmp
        );
    }
}
