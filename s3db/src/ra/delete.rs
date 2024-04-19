use std::collections::HashMap;

use crate::{ra::Scope, storage::Schemas};
use sql::{DataType, Delete};

use super::{AttributeId, ParseSelectError, RaCondition, RaExpression};

#[derive(Debug, PartialEq)]
pub struct RaDelete {
    pub table: String,
    pub condition: Option<RaCondition>,
}

impl RaDelete {
    pub fn parse(
        query: &Delete<'_, '_>,
        schemas: &Schemas,
    ) -> Result<(Self, HashMap<usize, DataType>), ParseSelectError> {
        let mut scope = Scope::new(schemas);

        let mut placeholders = HashMap::new();

        let table_schema = schemas
            .get_table(query.table.0.as_ref())
            .ok_or_else(|| ParseSelectError::UnknownRelation(query.table.0.to_string()))?;

        let mut base_relation = RaExpression::BaseRelation {
            name: query.table.to_static(),
            columns: table_schema
                .rows
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.clone(), c.ty.clone(), AttributeId::new(i)))
                .collect(),
        };

        scope.current_attributes.extend(
            table_schema
                .rows
                .iter()
                .map(|c| (c.name.clone(), c.ty.clone())),
        );

        let condition = match query.condition.as_ref() {
            Some(c) => {
                let rac = RaCondition::parse_internal(
                    &mut scope,
                    c,
                    &mut placeholders,
                    &mut base_relation,
                    &mut Vec::new(),
                )?;

                Some(rac)
            }
            None => None,
        };

        Ok((
            Self {
                table: query.table.0.to_string(),
                condition,
            },
            placeholders,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::ra::{
        AttributeId, ProjectionAttribute, RaComparisonOperator, RaCondition, RaConditionValue,
        RaExpression, RaValueExpression,
    };
    use sql::{Literal, Query};
    use bumpalo::Bump;

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn parse_with_subquery() {
        let arena = Bump::new();
        let query = "DELETE FROM dashboard_acl WHERE dashboard_id NOT IN (SELECT id FROM dashboard) AND dashboard_id != -1";

        let query = Query::parse(query.as_bytes(), &arena).unwrap();
        let delete = match query {
            Query::Delete(d) => d,
            other => panic!("{:#?}", other),
        };

        let schemas: Schemas = [
            (
                "dashboard_acl".to_string(),
                vec![("dashboard_id".to_string(), DataType::Integer)],
            ),
            (
                "dashboard".to_string(),
                vec![("id".to_string(), DataType::Integer)],
            ),
        ]
        .into_iter()
        .collect();

        let (delete, placeholders) = RaDelete::parse(&delete, &schemas).unwrap();

        assert_eq!(HashMap::new(), placeholders);

        assert_eq!(
            RaDelete {
                table: "dashboard_acl".into(),
                condition: Some(RaCondition::And(vec![
                    RaCondition::Value(Box::new(RaConditionValue::Comparison {
                        first: RaValueExpression::Attribute {
                            ty: DataType::Integer,
                            name: "dashboard_id".into(),
                            a_id: AttributeId::new(0),
                        },
                        second: RaValueExpression::SubQuery {
                            query: RaExpression::Projection {
                                inner: Box::new(RaExpression::BaseRelation {
                                    name: "dashboard".into(),
                                    columns: vec![("id".into(), DataType::Integer, AttributeId(0))]
                                }),
                                attributes: vec![ProjectionAttribute {
                                    value: RaValueExpression::Attribute {
                                        name: "id".into(),
                                        ty: DataType::Integer,
                                        a_id: AttributeId(0)
                                    },
                                    id: AttributeId(1),
                                    name: "id".to_string(),
                                }]
                            }
                        },
                        comparison: RaComparisonOperator::NotIn
                    })),
                    RaCondition::Value(Box::new(RaConditionValue::Comparison {
                        first: RaValueExpression::Attribute {
                            ty: DataType::Integer,
                            name: "dashboard_id".into(),
                            a_id: AttributeId::new(0)
                        },
                        second: RaValueExpression::Cast {
                            inner: Box::new(RaValueExpression::Literal(Literal::SmallInteger(-1))),
                            target: DataType::Integer,
                        },
                        comparison: RaComparisonOperator::NotEquals
                    }))
                ])),
            },
            delete
        );
    }
}
