use nom::{IResult, Parser};

use crate::{Parser as _, CompatibleParser};

use super::{query, DataType, Identifier, Query};

#[derive(Debug, PartialEq)]
pub struct Prepare<'s, 'a> {
    pub name: Identifier<'s>,
    pub params: Vec<DataType>,
    pub query: Box<Query<'s, 'a>>,
}

impl<'i, 'a> crate::ArenaParser<'i, 'a> for Prepare<'i, 'a>
{
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            parse(i, a)
        }
    }
}

impl<'s, 'a> CompatibleParser for Prepare<'s, 'a> {
    type StaticVersion = Prepare<'static, 'static>;

    fn parameter_count(&self) -> usize {
        0
    }

    fn to_static(&self) -> Self::StaticVersion {
        Prepare {
            name: self.name.to_static(),
            params: self.params.clone(),
            query: Box::new(self.query.to_static()),
        }
    }
}

#[deprecated]
pub fn parse<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], Prepare<'i, 'a>, nom::error::VerboseError<&'i [u8]>>
{
    let (i, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("PREPARE"),
        nom::character::complete::multispace1,
    ))(i)?;

    let (i, tmp) = nom::combinator::map(
        nom::sequence::tuple((
            Identifier::parse(),
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
                        DataType::parse(),
                    ),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                ))
                .map(|(_, _, _, dtys, _, _)| dtys),
            ),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag("as"),
            nom::character::complete::multispace1,
            move |i| query(i, arena),
        )),
        |(name, params, _, _, _, q)| Prepare {
            name,
            params: params.unwrap_or(Vec::new()),
            query: Box::new(q),
        },
    )(i)?;

    Ok((i, tmp))
}

#[cfg(test)]
mod tests {
    use crate::{
        macros::arena_parser_parse, BinaryOperator, ColumnReference, Condition, Select, TableExpression,
        ValueExpression,
    };

    use super::*;

    #[test]
    fn prepared_no_params() {
        arena_parser_parse!(
            Prepare,
            "PREPARE testing as SELECT * FROM users",
            Prepare {
                name: "testing".into(),
                params: vec![],
                query: Box::new(Query::Select(Select {
                    values: vec![ValueExpression::All].into(),
                    table: Some(TableExpression::Relation("users".into())),
                    where_condition: None,
                    having: None,
                    order_by: None,
                    group_by: None,
                    limit: None,
                    for_update: None,
                    combine: None,
                }))
            }
        );
    }

    #[test]
    fn prepared_params() {
        arena_parser_parse!(
            Prepare,
            "PREPARE testing(integer) as SELECT * FROM users WHERE id = $1",
            Prepare {
                name: "testing".into(),
                params: vec![DataType::Integer],
                query: Box::new(Query::Select(Select {
                    values: vec![ValueExpression::All].into(),
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
                    ).into())].into())),
                    having: None,
                    order_by: None,
                    group_by: None,
                    limit: None,
                    for_update: None,
                    combine: None,
                }))
            }
        );
    }
}
