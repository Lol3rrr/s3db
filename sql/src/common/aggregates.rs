use nom::IResult;

use crate::{arenas::Boxed, ArenaParser, CompatibleParser, ValueExpression};

/// References:
/// * [Postgres](https://www.postgresql.org/docs/16/functions-aggregate.html)
#[derive(Debug, PartialEq)]
pub enum AggregateExpression<'i, 'a> {
    /// Returns an arbitrary non-null value
    AnyValue(Boxed<'a, ValueExpression<'i, 'a>>),
    /// Counts the number of rows if used as `Count(*)`
    /// Otherwise counts number of non-null values
    Count(Boxed<'a, ValueExpression<'i, 'a>>),
    Sum(Boxed<'a, ValueExpression<'i, 'a>>),
    Max(Boxed<'a, ValueExpression<'i, 'a>>),
    Min(Boxed<'a, ValueExpression<'i, 'a>>),
}

impl<'i, 'a> CompatibleParser for AggregateExpression<'i, 'a> {
    type StaticVersion = AggregateExpression<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::Count(c) => AggregateExpression::Count(c.to_static()),
            Self::Sum(s) => AggregateExpression::Sum(s.to_static()),
            Self::AnyValue(v) => AggregateExpression::AnyValue(v.to_static()),
            Self::Max(m) => AggregateExpression::Max(m.to_static()),
            Self::Min(v) => AggregateExpression::Min(v.to_static()),
        }
    }

    fn parameter_count(&self) -> usize {
        match self {
            Self::Count(c) => c.parameter_count(),
            Self::Sum(s) => s.parameter_count(),
            Self::AnyValue(v) => v.parameter_count(),
            Self::Max(m) => m.parameter_count(),
            Self::Min(v) => v.parameter_count(),
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for AggregateExpression<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| aggregate(i, a)
    }
}

fn aggregate<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], AggregateExpression<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    nom::branch::alt((
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::branch::alt((
                        nom::bytes::complete::tag_no_case("count"),
                        nom::bytes::complete::tag_no_case("pg_catalog.count"),
                    )),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                nom::sequence::delimited(nom::character::complete::multispace0, ValueExpression::parse_arena(arena), nom::character::complete::multispace0),
                nom::bytes::complete::tag(")"),
            ),
            |exp| AggregateExpression::Count(Boxed::arena(arena, exp)),
        ),
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("sum"),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                ValueExpression::parse_arena(arena),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
            |exp| AggregateExpression::Sum(Boxed::arena(arena, exp)),
        ),
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("any_value"),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                ValueExpression::parse_arena(arena),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
            |exp| AggregateExpression::AnyValue(Boxed::arena(arena, exp)),
        ),
        nom::combinator::map(
            nom::sequence::delimited(
                nom::sequence::tuple((
                    nom::bytes::complete::tag_no_case("max"),
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("("),
                )),
                ValueExpression::parse_arena(arena),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag(")"),
                )),
            ),
            |exp| AggregateExpression::Max(Boxed::arena(arena, exp)),
        ),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{macros::arena_parser_parse, arenas::Boxed, common::ColumnReference};

    #[test]
    fn count_rows() {
        let expected = AggregateExpression::Count(Boxed::new(ValueExpression::All));

        arena_parser_parse!(AggregateExpression, "count(*)", expected.to_static());
        arena_parser_parse!(AggregateExpression, "count ( * )", expected.to_static());
    }

    #[test]
    fn count_attribute() {
        let expected = AggregateExpression::Count(Boxed::new(ValueExpression::ColumnReference(ColumnReference {
            column: "test".into(),
            relation: None,
        })));

        arena_parser_parse!(AggregateExpression, "count(test)", expected.to_static());
        arena_parser_parse!(AggregateExpression, "count ( test )", expected.to_static());
    }

    #[test]
    fn max_attribute() {
        let expected = AggregateExpression::Max(Boxed::new(ValueExpression::ColumnReference(ColumnReference {
            column: "test".into(),
            relation: None,
        })));

        arena_parser_parse!(AggregateExpression, "max(test)", expected.to_static());
        arena_parser_parse!(AggregateExpression, "max ( test )", expected.to_static());
    }
}
