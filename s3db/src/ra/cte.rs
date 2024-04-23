use std::collections::HashMap;

use crate::{
    ra::{ParsingContext, ProjectionAttribute, RaValueExpression},
    storage::Schemas,
};
use sql::{DataType, Query, WithCTE, WithCTEs};

use super::{ParseSelectError, RaExpression, Scope};

#[derive(Debug, PartialEq, Clone)]
pub struct CTE {
    pub name: String,
    pub value: CTEValue,
}

#[derive(Debug, PartialEq, Clone)]
pub enum CTEValue {
    Standard {
        query: CTEQuery,
    },
    Recursive {
        query: CTEQuery,
        columns: Vec<String>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum CTEQuery {
    Select(RaExpression),
}

pub fn parse_ctes(
    raw: &WithCTEs<'_, '_>,
    schemas: &Schemas,
) -> Result<(Vec<CTE>, HashMap<usize, DataType>), ParseSelectError> {
    let mut scope = Scope::new(schemas);
    let mut placeholders = HashMap::new();

    let mut result = Vec::new();
    for entry in raw.parts.iter() {
        let parsed = CTE::parse_internal(entry, &mut scope, &mut placeholders, raw.recursive)?;
        result.push(parsed);
    }

    Ok((result, placeholders))
}

impl CTE {
    fn parse_internal<'i, 'a>(
        raw: &WithCTE<'i, 'a>,
        scope: &mut Scope<'_>,
        placeholders: &mut HashMap<usize, DataType>,
        recursive: bool,
    ) -> Result<Self, ParseSelectError> {
        if recursive {
            // First only parse the base case to know types for the Table, then parse the entire
            // query with the cte table registered
            let query = match &raw.query {
                Query::Select(s) => {
                    if s.combine.is_none() {
                        return Err(ParseSelectError::NotImplemented("Expect Select to be in the From: SELECT base_case UNION recursive_case"));
                    }

                    let base_select = {
                        use sql::CompatibleParser;
                        let mut c = s.to_static();
                        c.combine = None;
                        c
                    };

                    let base_ra =
                        RaExpression::parse_s(&base_select, scope, placeholders, &mut Vec::new())?;

                    let mappings: Vec<_> = base_ra
                        .get_columns()
                        .into_iter()
                        .zip(raw.columns.as_ref().unwrap())
                        .map(|((_, val_name, val_type, val_attribute), column_name)| {
                            ProjectionAttribute {
                                id: scope.attribute_id(),
                                name: column_name.0.to_string(),
                                value: RaValueExpression::Attribute {
                                    name: val_name,
                                    ty: val_type,
                                    a_id: val_attribute,
                                },
                            }
                        })
                        .collect();

                    let base_ra = RaExpression::Projection {
                        inner: Box::new(base_ra),
                        attributes: mappings,
                    };

                    match scope.context.as_mut() {
                        Some(_ctx) => {
                            return Err(ParseSelectError::NotImplemented(
                                "Parse Recursive with Context",
                            ));
                        }
                        None => {
                            let mut ctx = ParsingContext::new();
                            ctx.add_cte(CTE {
                                name: raw.name.0.to_string(),
                                value: CTEValue::Standard {
                                    query: CTEQuery::Select(base_ra),
                                },
                            });
                            scope.context = Some(crate::ra::CustomCow::Owned(ctx));
                        }
                    };

                    let recursive_ra =
                        RaExpression::parse_s(s, scope, placeholders, &mut Vec::new())?;

                    CTEQuery::Select(recursive_ra)
                }
                other => {
                    dbg!(&other);
                    return Err(ParseSelectError::NotImplemented("Other Query"));
                }
            };

            Ok(Self {
                name: raw.name.0.to_string(),
                value: CTEValue::Recursive {
                    query,
                    columns: raw
                        .columns
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|c| c.0.to_string())
                        .collect(),
                },
            })
        } else {
            let query = match &raw.query {
                Query::Select(s) => {
                    let s = RaExpression::parse_s(s, scope, placeholders, &mut Vec::new())?;

                    CTEQuery::Select(s)
                }
                other => {
                    dbg!(other);
                    return Err(ParseSelectError::NotImplemented("Parsing other CTE Query"));
                }
            };

            Ok(Self {
                name: raw.name.0.to_string(),
                value: CTEValue::Standard { query },
            })
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::ra::{Attribute, AttributeId};
    use sql::{
        arenas::Boxed, BinaryOperator, ColumnReference, Combination, Literal, Select,
        TableExpression, ValueExpression,
    };

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn standard_ctes() {
        let raw_ctes = WithCTEs {
            parts: vec![WithCTE {
                name: "something".into(),
                query: Query::Select(Select {
                    values: vec![ValueExpression::Literal(Literal::SmallInteger(1))].into(),
                    table: None,
                    where_condition: None,
                    order_by: None,
                    group_by: None,
                    having: None,
                    limit: None,
                    for_update: None,
                    combine: None,
                }),
                columns: None,
            }]
            .into(),
            recursive: false,
        };

        let schemas: Schemas = [("".to_string(), vec![("".to_string(), DataType::Text)])]
            .into_iter()
            .collect();

        let (result, placeholders) = parse_ctes(&raw_ctes, &schemas).unwrap();

        assert_eq!(HashMap::new(), placeholders);

        assert_eq!(
            vec![CTE {
                name: "something".into(),
                value: CTEValue::Standard {
                    query: CTEQuery::Select(RaExpression::Projection {
                        inner: Box::new(RaExpression::EmptyRelation),
                        attributes: vec![Attribute {
                            id: AttributeId::new(0),
                            name: "".into(),
                            value: RaValueExpression::Literal(Literal::SmallInteger(1))
                        }]
                    })
                }
            }],
            result
        );
    }

    #[test]
    fn recursive_ctes() {
        let raw_ctes = WithCTEs {
            parts: vec![WithCTE {
                name: "something".into(),
                query: Query::Select(Select {
                    values: vec![ValueExpression::Literal(Literal::SmallInteger(1))].into(),
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
                                first: Boxed::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: None,
                                        column: "n".into(),
                                    },
                                )),
                                second: Boxed::new(ValueExpression::Literal(
                                    Literal::SmallInteger(1),
                                )),
                                operator: BinaryOperator::Add,
                            }]
                            .into(),
                            table: Some(TableExpression::Relation("something".into())),
                            where_condition: None,
                            order_by: None,
                            group_by: None,
                            having: None,
                            limit: None,
                            for_update: None,
                            combine: None,
                        })
                        .into(),
                    )),
                }),
                columns: Some(vec!["n".into()].into()),
            }]
            .into(),
            recursive: true,
        };

        let schemas: Schemas = [("".to_string(), vec![("".to_string(), DataType::Text)])]
            .into_iter()
            .collect();

        let (result, placeholders) = parse_ctes(&raw_ctes, &schemas).unwrap();

        assert_eq!(HashMap::new(), placeholders);

        assert_eq!(
            vec![CTE {
                name: "something".into(),
                value: CTEValue::Recursive {
                    query: CTEQuery::Select(RaExpression::Chain {
                        parts: vec![
                            RaExpression::Projection {
                                inner: Box::new(RaExpression::EmptyRelation),
                                attributes: vec![ProjectionAttribute {
                                    id: AttributeId::new(2),
                                    name: "".into(),
                                    value: RaValueExpression::Literal(Literal::SmallInteger(1)),
                                }]
                            },
                            RaExpression::Projection {
                                inner: Box::new(RaExpression::CTE {
                                    name: "something".into(),
                                    columns: vec![(
                                        "n".into(),
                                        DataType::SmallInteger,
                                        AttributeId::new(1)
                                    )]
                                }),
                                attributes: vec![ProjectionAttribute {
                                    id: AttributeId::new(3),
                                    name: "".into(),
                                    value: RaValueExpression::BinaryOperation {
                                        first: Box::new(RaValueExpression::Attribute {
                                            name: "n".into(),
                                            ty: DataType::SmallInteger,
                                            a_id: AttributeId::new(1)
                                        }),
                                        second: Box::new(RaValueExpression::Literal(
                                            Literal::SmallInteger(1)
                                        )),
                                        operator: BinaryOperator::Add
                                    }
                                }]
                            }
                        ]
                    }),
                    columns: vec!["n".into()],
                }
            }],
            result
        );
    }
}
