use nom::{IResult, Parser};

use crate::{
    common::column_reference,
    ColumnReference, Literal,
    Parser as _
};

#[derive(Debug, PartialEq, Clone)]
pub struct Ordering<'s> {
    pub column: OrderAttribute<'s>,
    pub order: OrderBy,
    pub nulls: NullOrdering,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderAttribute<'s> {
    ColumnRef(ColumnReference<'s>),
    ColumnIndex(usize),
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderBy {
    Ascending,
    Descending,
}

#[derive(Debug, PartialEq, Clone)]
pub enum NullOrdering {
    First,
    Last,
}

impl<'i, 's> crate::Parser<'i> for Vec<Ordering<'s>> where 'i: 's {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            order_by(i)
        }
    }
}

#[deprecated]
pub fn order_by(i: &[u8]) -> IResult<&[u8], Vec<Ordering<'_>>, nom::error::VerboseError<&[u8]>> {
    let (remaining, _) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("ORDER"),
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("BY"),
        nom::character::complete::multispace1,
    ))(i)?;

    let (remaining, tmp) = nom::combinator::cut(nom::error::context(
        "ORDER BY",
        nom::multi::separated_list1(
            nom::sequence::tuple((
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
            )),
            nom::sequence::tuple((
                nom::branch::alt((
                    column_reference.map(OrderAttribute::ColumnRef),
                    nom::combinator::map_res(Literal::parse(), |lit| match lit {
                        Literal::SmallInteger(v) => Ok(OrderAttribute::ColumnIndex(v as usize)),
                        Literal::Integer(v) => Ok(OrderAttribute::ColumnIndex(v as usize)),
                        Literal::BigInteger(v) => Ok(OrderAttribute::ColumnIndex(v as usize)),
                        _ => Err(()),
                    }),
                )),
                nom::combinator::opt(nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::branch::alt((
                        nom::bytes::complete::tag_no_case("ASC").map(|_| OrderBy::Ascending),
                        nom::bytes::complete::tag_no_case("DESC").map(|_| OrderBy::Descending),
                    )),
                ))),
                nom::combinator::opt(nom::sequence::tuple((
                    nom::character::complete::multispace1,
                    nom::bytes::complete::tag_no_case("NULLS"),
                    nom::character::complete::multispace1,
                    nom::branch::alt((
                        nom::bytes::complete::tag_no_case("FIRST").map(|_| NullOrdering::First),
                        nom::bytes::complete::tag_no_case("LAST").map(|_| NullOrdering::Last),
                    )),
                ))),
            ))
            .map(|(cr, order, null_ordering)| {
                let order = order.map(|(_, o)| o).unwrap_or(OrderBy::Ascending);
                let null_order = null_ordering
                    .map(|(_, _, _, ordering)| ordering)
                    .unwrap_or_else(|| match &order {
                        OrderBy::Ascending => NullOrdering::Last,
                        OrderBy::Descending => NullOrdering::First,
                    });

                Ordering {
                    column: cr,
                    order,
                    nulls: null_order,
                }
            }),
        ),
    ))(remaining)?;

    Ok((remaining, tmp))
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;
    use crate::macros::parser_parse;

    #[test]
    fn order_by_column() {
        parser_parse!(Vec<Ordering<'_>>, "ORDER BY something", vec![Ordering {
                column: OrderAttribute::ColumnRef(ColumnReference {
                    relation: None,
                    column: "something".into()
                }),
                order: OrderBy::Ascending,
                nulls: NullOrdering::Last,
            }]);
    }

    #[test]
    fn order_by_column_asc() {
        parser_parse!(Vec<Ordering<'_>>, "ORDER BY something ASC", vec![Ordering {
                column: OrderAttribute::ColumnRef(ColumnReference {
                    relation: None,
                    column: "something".into()
                }),
                order: OrderBy::Ascending,
                nulls: NullOrdering::Last,
            }]);
    }

    #[test]
    fn orderings() {
        parser_parse!(Vec<Ordering<'_>>, "ORDER BY something DESC", vec![Ordering {
                column: OrderAttribute::ColumnRef(ColumnReference {
                    relation: None,
                    column: "something".into()
                }),
                order: OrderBy::Descending,
                nulls: NullOrdering::First,
            }]);

        parser_parse!(Vec<Ordering<'_>>, "ORDER BY something DESC NULLS LAST", vec![Ordering {
                column: OrderAttribute::ColumnRef(ColumnReference {
                    relation: None,
                    column: "something".into()
                }),
                order: OrderBy::Descending,
                nulls: NullOrdering::Last,
            }]);

        parser_parse!(Vec<Ordering<'_>>, "ORDER BY something ASC NULLS FIRST", vec![Ordering {
                column: OrderAttribute::ColumnRef(ColumnReference {
                    relation: None,
                    column: "something".into()
                }),
                order: OrderBy::Ascending,
                nulls: NullOrdering::First,
            }]);
    }

    #[test]
    fn error() {
        #[allow(deprecated)]
        let test = order_by("ORDER BY ".as_bytes()).unwrap_err();
        assert!(
            matches!(test, nom::Err::Failure(_)),
            "Expected Failure error, got {:?}",
            test
        );
    }

    #[test]
    fn order_by_index() {
        parser_parse!(Vec<Ordering<'_>>, "order by 1", vec![Ordering {
                column: OrderAttribute::ColumnIndex(1),
                order: OrderBy::Ascending,
                nulls: NullOrdering::Last
            }]);
    }
}
