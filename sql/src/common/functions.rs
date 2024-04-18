use nom::{IResult, Parser};

use crate::{select, CompatibleParser, Select, Parser as _};

use super::{value_expression, Identifier, Literal, ValueExpression};

macro_rules! function_call {
    ($name:literal, $parser:expr, $mapping:expr) => {
        nom::combinator::map(
            nom::sequence::tuple((
                nom::combinator::opt(nom::bytes::complete::tag_no_case("pg_catalog.")),
                nom::bytes::complete::tag_no_case($name),
                nom::character::complete::multispace0,
                nom::sequence::delimited(
                    nom::sequence::tuple((
                        nom::bytes::complete::tag("("),
                        nom::character::complete::multispace0,
                    )),
                    $parser,
                    nom::sequence::tuple((
                        nom::character::complete::multispace0,
                        nom::bytes::complete::tag(")"),
                    )),
                ),
            )),
            |(_, _, _, raw)| $mapping(raw),
        )
    };
}

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
    Substr {
        value: Box<ValueExpression<'s>>,
        start: Box<ValueExpression<'s>>,
        count: Option<Box<ValueExpression<'s>>>,
    },
    CurrentTimestamp,
    CurrentSchemas {
        implicit: bool,
    },
    ArrayPosition {
        array: Box<ValueExpression<'s>>,
        target: Box<ValueExpression<'s>>,
    },
}

impl<'i, 's> crate::Parser<'i> for FunctionCall<'s> where 'i: 's {
    fn parse() -> impl Fn(&'i[u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            function_call(i)
        }
    }
}

#[deprecated]
pub fn function_call(
    i: &[u8],
) -> IResult<&[u8], FunctionCall<'_>, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag("lpad"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                value_expression,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                Literal::parse(),
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                Literal::parse(),
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
                Literal::parse(),
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
        function_call!(
            "substr",
            nom::sequence::tuple((
                value_expression,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                value_expression,
                nom::character::complete::multispace0,
                nom::combinator::opt(
                    nom::sequence::tuple((
                        nom::bytes::complete::tag(","),
                        nom::character::complete::multispace0,
                        value_expression,
                        nom::character::complete::multispace0,
                    ))
                    .map(|(_, _, v, _)| Box::new(v)),
                )
            )),
            |(content, _, _, _, start, _, count)| {
                FunctionCall::Substr {
                    value: Box::new(content),
                    start: Box::new(start),
                    count,
                }
            }
        ),
        nom::combinator::map(
            nom::bytes::complete::tag_no_case("CURRENT_TIMESTAMP"),
            |_| FunctionCall::CurrentTimestamp,
        ),
        function_call!(
            "current_schemas",
            nom::combinator::map_res(value_expression, |val| match val {
                ValueExpression::Literal(Literal::Bool(v)) => Ok(v),
                _ => Err(()),
            }),
            |val| { FunctionCall::CurrentSchemas { implicit: val } }
        ),
        function_call!(
            "array_position",
            nom::sequence::tuple((
                value_expression,
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                value_expression,
            )),
            |(array, _, _, _, target)| FunctionCall::ArrayPosition {
                array: Box::new(array),
                target: Box::new(target)
            }
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
            Self::Substr {
                value,
                start,
                count,
            } => FunctionCall::Substr {
                value: Box::new(value.to_static()),
                start: Box::new(start.to_static()),
                count: count.as_ref().map(|c| Box::new(c.to_static())),
            },
            Self::CurrentTimestamp => FunctionCall::CurrentTimestamp,
            Self::CurrentSchemas { implicit } => FunctionCall::CurrentSchemas {
                implicit: *implicit,
            },
            Self::ArrayPosition { array, target } => FunctionCall::ArrayPosition {
                array: Box::new(array.to_static()),
                target: Box::new(target.to_static()),
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
            Self::Substr {
                value,
                start,
                count,
            } => [
                value.max_parameter(),
                start.max_parameter(),
                count.as_ref().map(|c| c.max_parameter()).unwrap_or(0),
            ]
            .into_iter()
            .max()
            .unwrap_or(0),
            Self::CurrentTimestamp => 0,
            Self::CurrentSchemas { .. } => 0,
            Self::ArrayPosition { array, target } => {
                core::cmp::max(array.max_parameter(), target.max_parameter())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use select::TableExpression;

    use crate::{AggregateExpression, ColumnReference};

    use super::*;

    #[test]
    fn coalesce() {
        let (remaining, call) =
            function_call("COALESCE(dashboard.updated_by, -1)".as_bytes()).unwrap();

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
        let (remaining, call) = function_call("setval('id', 123)".as_bytes()).unwrap();

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
            function_call("setval('org_id_seq', (SELECT max(id) FROM org))".as_bytes()).unwrap();

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
                    having: None,
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
        let (remaining, call) = function_call("lower($1)".as_bytes()).unwrap();

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

    #[test]
    fn substr_without_count() {
        let (remaining, call) = function_call("substr('content', 4)".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            FunctionCall::Substr {
                value: Box::new(ValueExpression::Literal(Literal::Str("content".into()))),
                start: Box::new(ValueExpression::Literal(Literal::SmallInteger(4))),
                count: None
            },
            call
        );
    }

    #[test]
    fn substr_with_count() {
        let (remaining, call) = function_call("substr('content', 4, 2)".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            FunctionCall::Substr {
                value: Box::new(ValueExpression::Literal(Literal::Str("content".into()))),
                start: Box::new(ValueExpression::Literal(Literal::SmallInteger(4))),
                count: Some(Box::new(ValueExpression::Literal(Literal::SmallInteger(2))))
            },
            call
        );
    }

    #[test]
    fn current_schemas() {
        let (remaining, tmp) = function_call("current_schemas(true)".as_bytes()).unwrap();

        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(FunctionCall::CurrentSchemas { implicit: true }, tmp);
    }

    #[test]
    fn array_position() {
        let (remaining, tmp) =
            function_call("array_position('this is wrong but anyway', 'first')".as_bytes())
                .unwrap();

        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            FunctionCall::ArrayPosition {
                array: Box::new(ValueExpression::Literal(Literal::Str(
                    "this is wrong but anyway".into()
                ))),
                target: Box::new(ValueExpression::Literal(Literal::Str("first".into())))
            },
            tmp
        );
    }
}
