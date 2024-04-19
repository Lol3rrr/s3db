use nom::{IResult, Parser};

use crate::{dialects, ArenaParser as _, CompatibleParser, Parser as _};

use super::{common::ValueExpression, condition::Condition};

mod combine;
pub use combine::Combination;

mod tableexpr;
pub use tableexpr::{JoinKind, TableExpression};

mod group;
pub use group::GroupAttribute;

mod order;
pub use order::{NullOrdering, OrderAttribute, OrderBy, Ordering};

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
    pub group_by: Option<Vec<GroupAttribute<'s>>>,
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

impl<'s> CompatibleParser<dialects::Postgres> for Select<'s> {
    type StaticVersion = Select<'static>;

    fn to_static(&self) -> Self::StaticVersion {
        Select {
            values: self.values.iter().map(|v| v.to_static()).collect(),
            table: self.table.as_ref().map(|t| t.to_static()),
            where_condition: self.where_condition.as_ref().map(|c| c.to_static()),
            order_by: self.order_by.as_ref().map(|orders| {
                orders
                    .iter()
                    .map(|order| Ordering {
                        column: match &order.column {
                            OrderAttribute::ColumnRef(c) => {
                                OrderAttribute::ColumnRef(c.to_static())
                            }
                            OrderAttribute::ColumnIndex(i) => OrderAttribute::ColumnIndex(*i),
                        },
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
            for_update: self.for_update,
            combine: self
                .combine
                .as_ref()
                .map(|(c, s)| (c.clone(), Box::new(s.to_static()))),
        }
    }

    fn parameter_count(&self) -> usize {
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

impl<'s> Select<'s> {
    pub fn max_parameter(&self) -> usize {
        Self::parameter_count(self)
    }
}

impl<'i, 's> crate::Parser<'i> for Select<'s>
where
    'i: 's,
{
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            select(i, &bumpalo::Bump::new())
        }
    }
}

impl<'i, 'a, 's> crate::ArenaParser<'i, 'a> for Select<'s>
where
    'i: 's,
{
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            select(i, a)
        }
    }
}

#[deprecated]
pub fn select<'i, 's, 'a>(
    i: &'i [u8],
    a: &'a bumpalo::Bump,
) -> IResult<&'i [u8], Select<'s>, nom::error::VerboseError<&'i [u8]>>
where
    'i: 's,
{
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
            group_by,
            order_by,
            having,
            limit,
            for_update,
        ),
    ) = nom::error::context(
        "Parsing Select",
        nom::sequence::tuple((
            nom::combinator::opt(nom::bytes::complete::tag_no_case("distinct")),
            nom::character::complete::multispace0,
            <Vec<ValueExpression<'_>>>::parse(),
            nom::combinator::opt(nom::combinator::map(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag_no_case("from"),
                    nom::combinator::cut(nom::error::context(
                        "FROM",
                        nom::sequence::tuple((
                            nom::character::complete::multispace1,
                            TableExpression::parse(),
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
                            Condition::parse(),
                        ))),
                    ),
                ))
                .map(|(_, _, (_, cond))| cond),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    Combination::parse(),
                    nom::character::complete::multispace1,
                    Select::parse_arena(a),
                ))
                .map(|(_, c, _, s)| (c, Box::new(s))),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    <Vec<GroupAttribute<'_>>>::parse(),
                ))
                .map(|(_, attributes)| attributes),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    <Vec<Ordering<'_>>>::parse(),
                ))
                .map(|(_, order)| order),
            ),
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("HAVING"),
                    nom::character::complete::multispace1,
                    Condition::parse(),
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
    use crate::{
        self as sql,
        macros::{parser_parse, parser_parse_err},
        AggregateExpression, BinaryOperator, ColumnReference, Identifier, Literal,
    };

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn count_select() {
        parser_parse!(Select, "select count(*) from pgbench_branches");
    }

    #[test]
    fn select_where_and() {
        parser_parse!(
            Select,
            "SELECT 1 FROM \"pg_indexes\" WHERE \"tablename\"=$1 AND \"indexname\"=$2"
        );
    }

    #[test]
    fn select_with_limit() {
        parser_parse!(
            Select,
            "SELECT name FROM users LIMIT 1",
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
            }
        );
    }

    #[test]
    fn select_with_alias_without_as() {
        parser_parse!(
            Select,
            "SELECT u.name FROM user u",
            Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: Some(Identifier("u".into())),
                    column: Identifier("name".into())
                })],
                table: Some(TableExpression::Renamed {
                    inner: Box::new(TableExpression::Relation(Identifier("user".into()))),
                    name: Identifier("u".into()),
                    column_rename: None,
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }
        );
    }

    #[test]
    fn select_where_not_null() {
        parser_parse!(
            Select,
            "SELECT COUNT(*) FROM \"api_key\"WHERE service_account_id IS NOT NULL",
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
            }
        );
    }

    #[test]
    fn select_multiple_inner_joins() {
        parser_parse!(
            Select,
            "SELECT * FROM customer INNER JOIN orders ON customer.ID=orders.customer INNER JOIN products ON orders.product=products.ID",
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
                        lateral: false,
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
                    ))]),
                    lateral: false,
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None,
            }
        );
    }

    #[test]
    fn select_multiple_joins() {
        parser_parse!(
            Select,
            "SELECT * FROM customer JOIN orders ON customer.ID=orders.customer JOIN products ON orders.product=products.ID",
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
                        lateral: false,
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
                    ))]),
                    lateral: false,
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }
        );
    }

    #[test]
    fn select_order_by_column_reference() {
        parser_parse!(
            Select,
            "SELECT name FROM users ORDER BY users.age",
            Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("name".into())
                })],
                table: Some(TableExpression::Relation(Identifier("users".into()))),
                where_condition: None,
                order_by: Some(vec![Ordering {
                    column: OrderAttribute::ColumnRef(ColumnReference {
                        relation: Some("users".into()),
                        column: "age".into(),
                    }),
                    order: OrderBy::Ascending,
                    nulls: NullOrdering::Last
                }]),
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }
        );
    }

    #[test]
    fn union_all() {
        parser_parse!(
            Select,
            "SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 2",
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
            }
        );
    }

    #[test]
    fn having_clause() {
        parser_parse!(
            Select,
            "SELECT count(*) FROM orders GROUP BY uid HAVING uid > 0",
            Select {
                values: vec![ValueExpression::AggregateExpression(
                    AggregateExpression::Count(Box::new(ValueExpression::All))
                )],
                table: Some(TableExpression::Relation("orders".into())),
                where_condition: None,
                order_by: None,
                group_by: Some(vec![GroupAttribute::ColumnRef(ColumnReference {
                    relation: None,
                    column: "uid".into(),
                })]),
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
            }
        );
    }

    #[test]
    fn errors() {
        parser_parse_err!(Select, "SELECT something FROM");
    }
}
