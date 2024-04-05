use nom::{IResult, Parser};

use super::{
    common::{data_type, identifier},
    query, DataType, Identifier, Query,
};

#[derive(Debug, PartialEq)]
pub struct Prepare<'s> {
    pub name: Identifier<'s>,
    pub params: Vec<DataType>,
    pub query: Box<Query<'s>>,
}

pub fn parse(i: &[u8]) -> IResult<&[u8], Prepare<'_>, nom::error::VerboseError<&[u8]>> {
    let (i, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("PREPARE"),
        nom::character::complete::multispace1,
    ))(i)?;

    let (i, tmp) = nom::combinator::map(
        nom::sequence::tuple((
            identifier,
            nom::combinator::opt(
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                    nom::character::complete::multispace0,
                    nom::multi::separated_list1(
                        nom::sequence::tuple((
                            nom::character::complete::multispace0,
                            nom::bytes::complete::tag(","),
                            nom::character::complete::multispace0,
                        )),
                        data_type,
                    ),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                ))
                .map(|(_, _, _, dtys, _, _)| dtys),
            ),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag("as"),
            nom::character::complete::multispace1,
            query,
        )),
        |(name, params, _, _, _, q)| Prepare {
            name,
            params: params.unwrap_or(Vec::new()),
            query: Box::new(q),
        },
    )(i)?;

    Ok((i, tmp))
}

impl<'s> Prepare<'s> {
    pub fn to_static(&self) -> Prepare<'static> {
        Prepare {
            name: self.name.to_static(),
            params: self.params.clone(),
            query: Box::new(self.query.to_static()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BinaryOperator, ColumnReference, Condition, Select, TableExpression, ValueExpression,
    };

    use super::*;

    #[test]
    fn prepared_no_params() {
        let (remaining, tmp) = parse("PREPARE testing as SELECT * FROM users".as_bytes()).unwrap();

        dbg!(remaining, &tmp);

        assert_eq!(
            Prepare {
                name: "testing".into(),
                params: vec![],
                query: Box::new(Query::Select(Select {
                    values: vec![ValueExpression::All],
                    table: Some(TableExpression::Relation("users".into())),
                    where_condition: None,
                    having: None,
                    order_by: None,
                    group_by: None,
                    limit: None,
                    for_update: None,
                    combine: None,
                }))
            },
            tmp
        );
    }

    #[test]
    fn prepared_params() {
        let (remaining, tmp) =
            parse("PREPARE testing(integer) as SELECT * FROM users WHERE id = $1".as_bytes())
                .unwrap();

        dbg!(remaining, &tmp);

        assert_eq!(
            Prepare {
                name: "testing".into(),
                params: vec![DataType::Integer],
                query: Box::new(Query::Select(Select {
                    values: vec![ValueExpression::All],
                    table: Some(TableExpression::Relation("users".into())),
                    where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: "id".into(),
                            })),
                            second: Box::new(ValueExpression::Placeholder(1)),
                            operator: BinaryOperator::Equal
                        }
                    ))])),
                    having: None,
                    order_by: None,
                    group_by: None,
                    limit: None,
                    for_update: None,
                    combine: None,
                }))
            },
            tmp
        );
    }

    #[test]
    #[ignore = "Too large"]
    fn test() {
        let (remaining, tmp) = parse(
            "prepare neword (INTEGER, INTEGER, INTEGER, INTEGER, INTEGER) as select neword($1,$2,$3,$4,$5,0)"
                .as_bytes(),
        )
        .unwrap();

        dbg!(remaining, &tmp);

        assert_eq!(
            Prepare {
                name: "testing".into(),
                params: vec![DataType::Integer],
                query: Box::new(Query::Select(Select {
                    values: vec![ValueExpression::All],
                    table: Some(TableExpression::Relation("users".into())),
                    where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: "id".into(),
                            })),
                            second: Box::new(ValueExpression::Placeholder(1)),
                            operator: BinaryOperator::Equal
                        }
                    ))])),
                    having: None,
                    order_by: None,
                    group_by: None,
                    limit: None,
                    for_update: None,
                    combine: None,
                }))
            },
            tmp
        );
    }
}
