use nom::{IResult, Parser};

use crate::{CompatibleParser, Parser as _, Select, ArenaParser};

use super::{Identifier, Literal, ValueExpression};

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

#[derive(Debug, PartialEq)]
pub enum FunctionCall<'s, 'a> {
    LPad {
        base: Box<ValueExpression<'s, 'a>>,
        length: Literal<'s>,
        padding: Literal<'s>,
    },
    Coalesce {
        values: Vec<ValueExpression<'s, 'a>>,
    },
    Exists {
        query: Box<Select<'s, 'a>>,
    },
    SetValue {
        sequence_name: Identifier<'s>,
        value: Box<ValueExpression<'s, 'a>>,
        is_called: bool,
    },
    Lower {
        value: Box<ValueExpression<'s, 'a>>,
    },
    Substr {
        value: Box<ValueExpression<'s, 'a>>,
        start: Box<ValueExpression<'s, 'a>>,
        count: Option<Box<ValueExpression<'s, 'a>>>,
    },
    CurrentTimestamp,
    CurrentSchemas {
        implicit: bool,
    },
    ArrayPosition {
        array: Box<ValueExpression<'s, 'a>>,
        target: Box<ValueExpression<'s, 'a>>,
    },
}

impl<'i, 'a> CompatibleParser for FunctionCall<'i, 'a> {
    type StaticVersion = FunctionCall<'static, 'static>;

    fn parameter_count(&self) -> usize {
        match self {
            Self::LPad { base, .. } => base.parameter_count(),
            Self::Coalesce { values } => {
                values.iter().map(|v| v.parameter_count()).max().unwrap_or(0)
            }
            Self::Exists { query } => query.parameter_count(),
            Self::SetValue { .. } => 0,
            Self::Lower { value } => value.parameter_count(),
            Self::Substr {
                value,
                start,
                count,
            } => [
                value.parameter_count(),
                start.parameter_count(),
                count.as_ref().map(|c| c.parameter_count()).unwrap_or(0),
            ]
            .into_iter()
            .max()
            .unwrap_or(0),
            Self::CurrentTimestamp => 0,
            Self::CurrentSchemas { .. } => 0,
            Self::ArrayPosition { array, target } => {
                core::cmp::max(array.parameter_count(), target.parameter_count())
            }
        }
    }

    fn to_static(&self) -> Self::StaticVersion {
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
}

impl<'i, 'a> ArenaParser<'i, 'a> for FunctionCall<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            function_call(i, a)
        }
    }
}

#[deprecated]
pub fn function_call<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], FunctionCall<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::tuple((
                nom::bytes::complete::tag("lpad"),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag("("),
                ValueExpression::parse_arena(arena),
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
                    ValueExpression::parse_arena(arena),
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
                Select::parse_arena(arena),
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
                ValueExpression::parse_arena(arena),
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
                ValueExpression::parse_arena(arena),
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
                ValueExpression::parse_arena(arena),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                ValueExpression::parse_arena(arena),
                nom::character::complete::multispace0,
                nom::combinator::opt(
                    nom::sequence::tuple((
                        nom::bytes::complete::tag(","),
                        nom::character::complete::multispace0,
                        ValueExpression::parse_arena(arena),
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
            nom::combinator::map_res(ValueExpression::parse_arena(arena), |val| match val {
                ValueExpression::Literal(Literal::Bool(v)) => Ok(v),
                _ => Err(()),
            }),
            |val| { FunctionCall::CurrentSchemas { implicit: val } }
        ),
        function_call!(
            "array_position",
            nom::sequence::tuple((
                ValueExpression::parse_arena(arena),
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
                ValueExpression::parse_arena(arena),
            )),
            |(array, _, _, _, target)| FunctionCall::ArrayPosition {
                array: Box::new(array),
                target: Box::new(target)
            }
        ),
    ))(i)
}

#[cfg(test)]
mod tests {

    use crate::{macros::arena_parser_parse, AggregateExpression, ColumnReference, TableExpression};

    use super::*;

    #[test]
    fn coalesce() {
        arena_parser_parse!(
            FunctionCall,
            "COALESCE(dashboard.updated_by, -1)",
            FunctionCall::Coalesce {
                values: vec![
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("dashboard".into())),
                        column: Identifier("updated_by".into()),
                    }),
                    ValueExpression::Literal(Literal::SmallInteger(-1))
                ]
            }
        );
    }

    #[test]
    fn set_val_without_is_called() {
        arena_parser_parse!(
            FunctionCall,
            "setval('id', 123)",
            FunctionCall::SetValue {
                sequence_name: Identifier("id".into()),
                value: Box::new(ValueExpression::Literal(Literal::SmallInteger(123))),
                is_called: true
            }
        );
    }

    #[test]
    fn set_val_with_subquery() {
        arena_parser_parse!(
            FunctionCall,
            "setval('org_id_seq', (SELECT max(id) FROM org))",
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
            }
        );
    }

    #[test]
    fn lower() {
        arena_parser_parse!(
            FunctionCall,
            "lower($1)",
            FunctionCall::Lower {
                value: Box::new(ValueExpression::Placeholder(1)),
            }
        );
    }

    #[test]
    fn substr_without_count() {
        arena_parser_parse!(
            FunctionCall,
            "substr('content', 4)",
            FunctionCall::Substr {
                value: Box::new(ValueExpression::Literal(Literal::Str("content".into()))),
                start: Box::new(ValueExpression::Literal(Literal::SmallInteger(4))),
                count: None
            }
        );
    }

    #[test]
    fn substr_with_count() {
        arena_parser_parse!(
            FunctionCall,
            "substr('content', 4, 2)",
            FunctionCall::Substr {
                value: Box::new(ValueExpression::Literal(Literal::Str("content".into()))),
                start: Box::new(ValueExpression::Literal(Literal::SmallInteger(4))),
                count: Some(Box::new(ValueExpression::Literal(Literal::SmallInteger(2))))
            }
        );
    }

    #[test]
    fn current_schemas() {
        arena_parser_parse!(
            FunctionCall,
            "current_schemas(true)",
            FunctionCall::CurrentSchemas { implicit: true }
        );
    }

    #[test]
    fn array_position() {
        arena_parser_parse!(
            FunctionCall,
            "array_position('this is wrong but anyway', 'first')",
            FunctionCall::ArrayPosition {
                array: Box::new(ValueExpression::Literal(Literal::Str(
                    "this is wrong but anyway".into()
                ))),
                target: Box::new(ValueExpression::Literal(Literal::Str("first".into())))
            }
        );
    }
}
