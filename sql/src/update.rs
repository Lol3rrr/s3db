use nom::{IResult, Parser};

use crate::{ArenaParser, CompatibleParser, Parser as _};

use super::{Condition, Identifier, ValueExpression};

#[derive(Debug, PartialEq)]
pub struct Update<'s, 'a> {
    pub table: Identifier<'s>,
    pub fields: crate::arenas::Vec<'a, (Identifier<'s>, ValueExpression<'s, 'a>)>,
    pub condition: Option<Condition<'s, 'a>>,
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

impl<'s, 'a> CompatibleParser for Update<'s, 'a> {
    type StaticVersion = Update<'static, 'static>;

    fn to_static(&self) -> Self::StaticVersion {
        Update {
            table: self.table.to_static(),
            fields: crate::arenas::Vec::Heap(
                self.fields
                    .iter()
                    .map(|f| (f.0.to_static(), f.1.to_static()))
                    .collect(),
            ),
            condition: self.condition.as_ref().map(|c| c.to_static()),
            from: self.from.as_ref().map(|f| f.to_static()),
        }
    }

    fn parameter_count(&self) -> usize {
        core::cmp::max(
            self.fields
                .iter()
                .map(|(_, expr)| expr.parameter_count())
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

impl<'i, 'a> ArenaParser<'i, 'a> for Update<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            #[allow(deprecated)]
            update(i, a)
        }
    }
}

#[deprecated]
pub fn update<'i, 'a>(
    i: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], Update<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    let (remaining, (_, _, table)) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("UPDATE"),
        nom::character::complete::multispace1,
        Identifier::parse(),
    ))(i)?;

    let (remaining, (_, _, _, fields)) = nom::sequence::tuple((
        nom::character::complete::multispace1,
        nom::bytes::complete::tag_no_case("SET"),
        nom::character::complete::multispace1,
        crate::nom_util::bump_separated_list1(
            arena,
            nom::sequence::tuple((
                nom::character::complete::multispace0,
                nom::bytes::complete::tag(","),
                nom::character::complete::multispace0,
            )),
            nom::sequence::tuple((
                Identifier::parse(),
                nom::sequence::tuple((
                    nom::character::complete::multispace0,
                    nom::bytes::complete::tag("="),
                    nom::character::complete::multispace0,
                )),
                ValueExpression::parse_arena(arena),
            ))
            .map(|(ident, _, val)| (ident, val)),
        ),
    ))(remaining)?;

    let (remaining, from_clause) = nom::combinator::opt(
        nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("FROM"),
            nom::character::complete::multispace1,
            Identifier::parse(),
            nom::combinator::opt(nom::combinator::verify(
                nom::sequence::tuple((nom::character::complete::multispace1, Identifier::parse()))
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
            Condition::parse_arena(arena),
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

    use crate::{
        arenas::Boxed, common::FunctionCall, macros::arena_parser_parse, BinaryOperator,
        ColumnReference, DataType, Literal,
    };

    use super::*;

    #[test]
    fn basic_update() {
        arena_parser_parse!(
            Update,
            "UPDATE user SET name = 'changed' WHERE name = 'other'",
            Update {
                table: Identifier("user".into()),
                fields: vec![(
                    Identifier("name".into()),
                    ValueExpression::Literal(Literal::Str("changed".into()))
                )]
                .into(),
                condition: Some(Condition::And(
                    vec![Condition::Value(
                        Box::new(ValueExpression::Operator {
                            first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("name".into()),
                            })),
                            second: Boxed::new(ValueExpression::Literal(Literal::Str(
                                "other".into()
                            ))),
                            operator: BinaryOperator::Equal
                        })
                        .into()
                    )]
                    .into()
                )),
                from: None,
            }
        );
    }

    #[test]
    fn more_complicated() {
        arena_parser_parse!(
            Update,
            "UPDATE \"temp_user\" SET created = $1, updated = $2 WHERE created = '0' AND status in ('SignUpStarted', 'InvitePending')",
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
                ].into(),
                condition: Some(Condition::And(vec![
                    Condition::Value(Boxed::new(ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("created".into())
                        })),
                        second: Boxed::new(ValueExpression::Literal(Literal::Str("0".into()))),
                        operator: BinaryOperator::Equal
                    }).into()),
                    Condition::Value(Boxed::new(ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("status".into())
                        })),
                        second: Boxed::new(ValueExpression::List(vec![
                            ValueExpression::Literal(Literal::Str("SignUpStarted".into())),
                            ValueExpression::Literal(Literal::Str("InvitePending".into()))
                        ].into())),
                        operator: BinaryOperator::In
                    })),
                ].into())),
                from: None,
            }
        );
    }

    #[test]
    fn update_complex() {
        arena_parser_parse!(
            Update,
            "UPDATE dashboard SET uid=lpad('' || id::text,9,'0') WHERE uid IS NULL",
            Update {
                table: Identifier("dashboard".into()),
                fields: vec![(
                    Identifier("uid".into()),
                    ValueExpression::FunctionCall(FunctionCall::LPad {
                        base: Boxed::new(ValueExpression::Operator {
                            first: Boxed::new(ValueExpression::Literal(Literal::Str("".into()))),
                            second: Boxed::new(ValueExpression::TypeCast {
                                base: Boxed::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: None,
                                        column: Identifier("id".into())
                                    }
                                )),
                                target_ty: DataType::Text
                            }),
                            operator: BinaryOperator::Concat
                        }),
                        length: Literal::SmallInteger(9),
                        padding: Literal::Str("0".into())
                    })
                )]
                .into(),
                condition: Some(Condition::And(
                    vec![Condition::Value(
                        Box::new(ValueExpression::Operator {
                            first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: Identifier("uid".into())
                            })),
                            second: Boxed::new(ValueExpression::Null),
                            operator: BinaryOperator::Is
                        })
                        .into()
                    )]
                    .into()
                )),
                from: None,
            }
        );
    }

    #[test]
    fn update_with_from() {
        arena_parser_parse!(
            Update,
            "UPDATE dashboard\n\tSET folder_uid = folder.uid\n\tFROM dashboard folder\n\tWHERE dashboard.folder_id = folder.id\n\t  AND dashboard.is_folder = $1",
            Update {
                table: Identifier("dashboard".into()),
                fields: vec![(
                    Identifier("folder_uid".into()),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("folder".into())),
                        column: Identifier("uid".into())
                    })
                )].into(),
                condition: Some(Condition::And(vec![
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("dashboard".into())),
                            column: Identifier("folder_id".into())
                        })),
                        second: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("folder".into())),
                            column: Identifier("id".into())
                        })),
                        operator: BinaryOperator::Equal
                    }).into()),
                    Condition::Value(Box::new(ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("dashboard".into())),
                            column: Identifier("is_folder".into())
                        })),
                        second: Boxed::new(ValueExpression::Placeholder(1)),
                        operator: BinaryOperator::Equal
                    }).into())
                ].into())),
                from: Some(UpdateFrom::Renamed {
                    inner: Identifier("dashboard".into()),
                    name: Identifier("folder".into())
                })
            }
        );
    }
}
