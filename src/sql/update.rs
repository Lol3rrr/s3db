use nom::{IResult, Parser};

use crate::sql::{
    common::{identifier, value_expression},
    condition::condition,
};

use super::{Condition, Identifier, ValueExpression};

#[derive(Debug, PartialEq)]
pub struct Update<'s> {
    pub table: Identifier<'s>,
    pub fields: Vec<(Identifier<'s>, ValueExpression<'s>)>,
    pub condition: Option<Condition<'s>>,
    pub from: Option<UpdateFrom<'s>>,
}

#[derive(Debug, PartialEq)]
pub enum UpdateFrom<'s> {
    Relation(Identifier<'s>),
    Renamed {
        inner: Identifier<'s>,
        name: Identifier<'s>,
    },
}

impl<'s> Update<'s> {
    pub fn to_static(&self) -> Update<'static> {
        Update {
            table: self.table.to_static(),
            fields: self
                .fields
                .iter()
                .map(|f| (f.0.to_static(), f.1.to_static()))
                .collect(),
            condition: self.condition.as_ref().map(|c| c.to_static()),
            from: self.from.as_ref().map(|f| f.to_static()),
        }
    }

    pub fn max_parameter(&self) -> usize {
        core::cmp::max(
            self.fields
                .iter()
                .map(|(_, expr)| expr.max_parameter())
                .max()
                .unwrap_or(0),
            self.condition
                .as_ref()
                .map(|c| c.max_parameter())
                .unwrap_or(0),
        )
    }
}

impl<'s> UpdateFrom<'s> {
    pub fn to_static(&self) -> UpdateFrom<'static> {
        match self {
            Self::Relation(ident) => UpdateFrom::Relation(ident.to_static()),
            Self::Renamed { inner, name } => UpdateFrom::Renamed {
                inner: inner.to_static(),
                name: name.to_static(),
            },
        }
    }
}

pub fn update(i: &[u8]) -> IResult<&[u8], Update<'_>> {
    let (remaining, (_, _, table)) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("UPDATE"),
        nom::character::complete::multispace1,
        identifier,
    ))(i)?;

    let (remaining, (_, _, _, fields)) = nom::sequence::tuple((
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("SET"),
        nom::character::complete::multispace1,
        nom::multi::separated_list1(
            nom::sequence::tuple((
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
            )),
            nom::sequence::tuple((
                identifier,
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("="),
                    nom::character::complete::multispace0,
                )),
                value_expression,
            ))
            .map(|(ident, _, val)| (ident, val)),
        ),
    ))(remaining)?;

    let (remaining, from_clause) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("FROM"),
            nom::character::complete::multispace1,
            identifier,
            nom::combinator::opt(nom::combinator::verify(
                nom::sequence::tuple((nom::character::complete::multispace1, identifier))
                    .map(|(_, name)| name),
                |ident| !ident.0.as_ref().eq_ignore_ascii_case("WHERE"),
            )),
        ))
        .map(|(_, _, _, src, renamed)| (src, renamed)),
    )(remaining)?;

    let (remaining, condition) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("WHERE"),
            nom::character::complete::multispace1,
            condition,
        ))
        .map(|(_, _, _, cond)| cond),
    )(remaining)?;

    Ok((
        remaining,
        Update {
            table,
            fields,
            condition,
            from: from_clause.map(|(src, name)| match name {
                Some(n) => UpdateFrom::Renamed {
                    inner: src,
                    name: n,
                },
                None => UpdateFrom::Relation(src),
            }),
        },
    ))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::sql::{common::FunctionCall, BinaryOperator, ColumnReference, DataType, Literal};

    use super::*;

    #[test]
    fn basic_update() {
        let (remaining, update) =
            update("UPDATE user SET name = 'changed' WHERE name = 'other'".as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Update {
                table: Identifier("user".into()),
                fields: vec![(
                    Identifier("name".into()),
                    ValueExpression::Literal(Literal::Str("changed".into()))
                )],
                condition: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("name".into()),
                        })),
                        second: Box::new(ValueExpression::Literal(Literal::Str("other".into()))),
                        operator: BinaryOperator::Equal
                    }
                ))])),
                from: None,
            },
            update
        );
    }

    #[test]
    fn more_complicated() {
        let (remaining, update) = update(
            "UPDATE \"temp_user\" SET created = $1, updated = $2 WHERE created = '0' AND status in ('SignUpStarted', 'InvitePending')"
                .as_bytes(),
        )
        .unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Update {
                table: Identifier("temp_user".into()),
                fields: vec![
                    (
                        Identifier("created".into()),
                        ValueExpression::Placeholder(1)
                    ),
                    (
                        Identifier("updated".into()),
                        ValueExpression::Placeholder(2)
                    )
                ],
                condition: Some(Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("created".into())
                        })),
                        second: Box::new(ValueExpression::Literal(Literal::Str("0".into()))),
                        operator: BinaryOperator::Equal
                    })),
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("status".into())
                        })),
                        second: Box::new(ValueExpression::List(vec![
                            ValueExpression::Literal(Literal::Str("SignUpStarted".into())),
                            ValueExpression::Literal(Literal::Str("InvitePending".into()))
                        ])),
                        operator: BinaryOperator::In
                    })),
                ])),
                from: None,
            },
            update
        );
    }

    #[test]
    fn update_complex() {
        let query = "UPDATE dashboard SET uid=lpad('' || id::text,9,'0') WHERE uid IS NULL";

        let (remaining, update) = update(query.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Update {
                table: Identifier("dashboard".into()),
                fields: vec![(
                    Identifier("uid".into()),
                    ValueExpression::FunctionCall(FunctionCall::LPad {
                        base: Box::new(ValueExpression::Operator {
                            first: Box::new(ValueExpression::Literal(Literal::Str("".into()))),
                            second: Box::new(ValueExpression::TypeCast {
                                base: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                    relation: None,
                                    column: Identifier("id".into())
                                })),
                                target_ty: DataType::Text
                            }),
                            operator: BinaryOperator::Concat
                        }),
                        length: Literal::SmallInteger(9),
                        padding: Literal::Str("0".into())
                    })
                )],
                condition: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("uid".into())
                        })),
                        second: Box::new(ValueExpression::Null),
                        operator: BinaryOperator::Is
                    }
                ))])),
                from: None,
            },
            update
        );
    }

    #[test]
    fn update_with_from() {
        let query_str = "UPDATE dashboard\n\tSET folder_uid = folder.uid\n\tFROM dashboard folder\n\tWHERE dashboard.folder_id = folder.id\n\t  AND dashboard.is_folder = $1";

        let (remaining, update) = update(query_str.as_bytes()).unwrap();

        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );

        assert_eq!(
            Update {
                table: Identifier("dashboard".into()),
                fields: vec![(
                    Identifier("folder_uid".into()),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("folder".into())),
                        column: Identifier("uid".into())
                    })
                )],
                condition: Some(Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("dashboard".into())),
                            column: Identifier("folder_id".into())
                        })),
                        second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("folder".into())),
                            column: Identifier("id".into())
                        })),
                        operator: BinaryOperator::Equal
                    })),
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("dashboard".into())),
                            column: Identifier("is_folder".into())
                        })),
                        second: Box::new(ValueExpression::Placeholder(1)),
                        operator: BinaryOperator::Equal
                    }))
                ])),
                from: Some(UpdateFrom::Renamed {
                    inner: Identifier("dashboard".into()),
                    name: Identifier("folder".into())
                })
            },
            update
        );
    }
}
