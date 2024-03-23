use nom::IResult;

use crate::sql::{select, Select};

use super::{literal, value_expression, Identifier, Literal, ValueExpression};

#[derive(Debug, PartialEq, Clone)]
pub enum FunctionCall<'s> {
    LPad {
        base: Box<ValueExpression<'s>>,
        length: Literal<'s>,
        padding: Literal<'s>,
    },
    Coalesce {
        values: Vec<ValueExpression<'s>>,
    },
    Exists {
        query: Box<Select<'s>>,
    },
    SetValue {
        sequence_name: Identifier<'s>,
        value: Box<ValueExpression<'s>>,
        is_called: bool,
    },
    Lower {
        value: Box<ValueExpression<'s>>,
    },
}

pub fn function_call(i: &[u8]) -> IResult<&[u8], FunctionCall<'_>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag("lpad"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                value_expression,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                literal,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                literal,
                nom::bytes::complete::tag(")"),
            )),
            |(_, _, _, base, _, _, length, _, _, padding, _)| FunctionCall::LPad {
                base: Box::new(base),
                length,
                padding,
            },
        ),
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag("COALESCE"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                nom::multi::separated_list1(
                    nom::sequence::tuple((
                        nom::character::complete::multispace0,
                        nom::bytes::complete::tag(","),
                        nom::character::complete::multispace0,
                    )),
                    value_expression,
                ),
                nom::bytes::complete::tag(")"),
            )),
            |(_, _, _, values, _)| FunctionCall::Coalesce { values },
        ),
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("exists"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                select::select,
                nom::bytes::complete::tag(")"),
            )),
            |(_, _, _, query, _)| FunctionCall::Exists {
                query: Box::new(query),
            },
        ),
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("setval"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                literal,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                value_expression,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            )),
            |(_, _, _, name_lit, _, _, _, value, _, _)| {
                let name = match name_lit {
                    Literal::Str(name) => name,
                    other => todo!("{:?}", other),
                };

                FunctionCall::SetValue {
                    sequence_name: Identifier(name),
                    value: Box::new(value),
                    is_called: true,
                }
            },
        ),
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("lower"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                nom::character::complete::multispace0,
                value_expression,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            )),
            |(_, _, _, _, val, _, _)| FunctionCall::Lower {
                value: Box::new(val),
            },
        ),
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag_no_case("substr"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                nom::character::complete::multispace0,
                value_expression,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                value_expression,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(")"),
            )),
            |tmp| {
                dbg!(&tmp);
                todo!()
            },
        ),
    ))(i)
}

impl<'s> FunctionCall<'s> {
    pub fn to_static(&self) -> FunctionCall<'static> {
        match self {
            Self::LPad {
                base,
                length,
                padding,
            } => FunctionCall::LPad {
                base: Box::new(base.to_static()),
                length: length.to_static(),
                padding: padding.to_static(),
            },
            Self::Coalesce { values } => FunctionCall::Coalesce {
                values: values.iter().map(|v| v.to_static()).collect(),
            },
            Self::Exists { query } => FunctionCall::Exists {
                query: Box::new(query.to_static()),
            },
            Self::SetValue {
                sequence_name,
                value,
                is_called,
            } => FunctionCall::SetValue {
                sequence_name: sequence_name.to_static(),
                value: Box::new(value.to_static()),
                is_called: *is_called,
            },
            Self::Lower { value } => FunctionCall::Lower {
                value: Box::new(value.to_static()),
            },
        }
    }

    pub fn max_parameter(&self) -> usize {
        match self {
            Self::LPad { base, .. } => base.max_parameter(),
            Self::Coalesce { values } => {
                values.iter().map(|v| v.max_parameter()).max().unwrap_or(0)
            }
            Self::Exists { query } => query.max_parameter(),
            Self::SetValue { .. } => 0,
            Self::Lower { value } => value.max_parameter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use select::TableExpression;

    use crate::sql::{AggregateExpression, ColumnReference};

    use super::*;

    #[test]
    fn coalesce() {
        let (remaining, call) = function_call("COALESCE(dashboard.updated_by, -1)".as_bytes())
            .map_err(|e| e.map_input(|d| core::str::from_utf8(d)))
            .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            FunctionCall::Coalesce {
                values: vec![
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("dashboard".into())),
                        column: Identifier("updated_by".into()),
                    }),
                    ValueExpression::Literal(Literal::SmallInteger(-1))
                ]
            },
            call
        );
    }

    #[test]
    fn set_val_without_is_called() {
        let (remaining, call) = function_call("setval('id', 123)".as_bytes())
            .map_err(|e| e.map_input(|d| core::str::from_utf8(d)))
            .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            FunctionCall::SetValue {
                sequence_name: Identifier("id".into()),
                value: Box::new(ValueExpression::Literal(Literal::SmallInteger(123))),
                is_called: true
            },
            call
        );
    }

    #[test]
    fn set_val_with_subquery() {
        let (remaining, call) =
            function_call("setval('org_id_seq', (SELECT max(id) FROM org))".as_bytes())
                .map_err(|e| e.map_input(|d| core::str::from_utf8(d)))
                .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            FunctionCall::SetValue {
                sequence_name: Identifier("org_id_seq".into()),
                value: Box::new(ValueExpression::SubQuery(Select {
                    values: vec![ValueExpression::AggregateExpression(
                        AggregateExpression::Max(Box::new(ValueExpression::ColumnReference(
                            ColumnReference {
                                relation: None,
                                column: Identifier("id".into())
                            }
                        )))
                    )],
                    table: Some(TableExpression::Relation(Identifier("org".into()))),
                    where_condition: None,
                    order_by: None,
                    group_by: None,
                    limit: None,
                    for_update: None,
                    combine: None
                })),
                is_called: true
            },
            call
        );
    }

    #[test]
    fn lower() {
        let (remaining, call) = function_call("lower($1)".as_bytes())
            .map_err(|e| e.map_input(|d| core::str::from_utf8(d)))
            .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            FunctionCall::Lower {
                value: Box::new(ValueExpression::Placeholder(1))
            },
            call
        );
    }
}
