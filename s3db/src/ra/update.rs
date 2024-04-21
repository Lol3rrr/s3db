use std::collections::HashMap;

use crate::{ra::RaValueExpression, storage::Schemas};
use sql::{DataType, Identifier, JoinKind, Update, UpdateFrom};

use super::{AttributeId, ParseSelectError, RaCondition, RaExpression, Scope};

#[derive(Debug, PartialEq)]
pub enum RaUpdate {
    Standard {
        fields: Vec<UpdateFields>,
        condition: Option<RaCondition>,
    },
    UpdateFrom {
        fields: Vec<UpdateFields>,
        condition: Option<RaCondition>,
        other_table_src: Identifier<'static>,
        other_table_name: Identifier<'static>,
    },
}

#[derive(Debug, PartialEq)]
pub struct UpdateFields {
    pub field: String,
    pub value: RaValueExpression,
}

impl RaUpdate {
    pub fn parse(
        query: &Update<'_, '_>,
        schemas: &Schemas,
    ) -> Result<(Self, HashMap<usize, DataType>), ParseSelectError> {
        let mut scope = Scope::new(schemas);

        let mut placeholders = HashMap::new();

        let table_schema = schemas
            .get_table(query.table.0.as_ref())
            .ok_or(ParseSelectError::Other("Loading Schemas"))?;

        let mut table_expr = RaExpression::BaseRelation {
            name: query.table.to_static(),
            columns: table_schema
                .rows
                .iter()
                .enumerate()
                .map(|(i, r)| (r.name.clone(), r.ty.clone(), AttributeId::new(i)))
                .collect(),
        };

        scope.current_attributes.extend(
            table_schema
                .rows
                .iter()
                .map(|c| (c.name.clone(), c.ty.clone())),
        );

        match query.from.as_ref() {
            Some(tmp) => {
                let (other_src, other_name) = match tmp {
                    UpdateFrom::Relation(name) => {
                        let table_schema = schemas
                            .get_table(name.0.as_ref())
                            .ok_or(ParseSelectError::Other("Getting Table Schema"))?;

                        scope.current_attributes.extend(
                            table_schema
                                .rows
                                .iter()
                                .map(|c| (c.name.clone(), c.ty.clone())),
                        );

                        (name.to_static(), name.to_static())
                    }
                    UpdateFrom::Renamed { inner, name } => {
                        let table_schema = schemas
                            .get_table(inner.0.as_ref())
                            .ok_or(ParseSelectError::Other("Getting Table Schema"))?;

                        scope.current_attributes.extend(
                            table_schema
                                .rows
                                .iter()
                                .map(|c| (c.name.clone(), c.ty.clone())),
                        );

                        (inner.to_static(), name.to_static())
                    }
                };

                // TODO
                // Create a new table expression that is basically a join, where each row of the
                // FROM table is joined to each Row of the updated table. Then you filter down the
                // rows based on the Condition and finally update the target table based on the
                // resulting rows

                let other_table = {
                    let schema = schemas
                        .get_table(&other_src.0)
                        .ok_or(ParseSelectError::UnknownRelation(other_src.0.to_string()))?;

                    RaExpression::Renamed {
                        name: other_name.0.to_string(),
                        inner: Box::new(RaExpression::BaseRelation {
                            name: other_src.to_static(),
                            columns: schema
                                .rows
                                .iter()
                                .enumerate()
                                .map(|(i, r)| (r.name.clone(), r.ty.clone(), AttributeId::new(i)))
                                .collect(),
                        }),
                    }
                };

                let mut joined = RaExpression::Join {
                    left: Box::new(table_expr),
                    right: Box::new(other_table),
                    kind: JoinKind::Inner,
                    condition: RaCondition::And(vec![]),
                };

                let condition = match query.condition.as_ref() {
                    Some(c) => {
                        let cond = RaCondition::parse_internal(
                            &mut scope,
                            c,
                            &mut placeholders,
                            &mut joined,
                            &mut Vec::new(),
                        )?;
                        Some(cond)
                    }
                    None => None,
                };

                let mut fields = Vec::new();
                for (name, value) in query.fields.iter() {
                    let expr = RaValueExpression::parse_internal(
                        &mut scope,
                        value,
                        &mut placeholders,
                        &mut joined,
                        &mut Vec::new(),
                    )?;

                    let target_ty_res = table_schema
                        .rows
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(name.0.as_ref()))
                        .map(|c| &c.ty);
                    let target_ty = match target_ty_res {
                        Some(t) => t,
                        None => {
                            panic!("Determining Type for {:?}", name);
                        }
                    };
                    let expr = expr
                        .enforce_type(target_ty.clone(), &mut placeholders)
                        .unwrap();

                    fields.push(UpdateFields {
                        field: name.0.to_string(),
                        value: expr,
                    });
                }

                // TODO
                // This needs to be restructured
                Ok((
                    Self::UpdateFrom {
                        fields,
                        condition,
                        other_table_src: other_src,
                        other_table_name: other_name,
                    },
                    placeholders,
                ))
            }
            None => {
                let condition = match query.condition.as_ref() {
                    Some(c) => {
                        let cond = RaCondition::parse_internal(
                            &mut scope,
                            c,
                            &mut placeholders,
                            &mut table_expr,
                            &mut Vec::new(),
                        )?;
                        Some(cond)
                    }
                    None => None,
                };

                let mut fields = Vec::new();
                for (name, value) in query.fields.iter() {
                    let expr = RaValueExpression::parse_internal(
                        &mut scope,
                        value,
                        &mut placeholders,
                        &mut table_expr,
                        &mut Vec::new(),
                    )?;

                    let target_ty = table_schema
                        .rows
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(name.0.as_ref()))
                        .map(|c| &c.ty)
                        .unwrap();
                    let expr = expr
                        .enforce_type(target_ty.clone(), &mut placeholders)
                        .unwrap();

                    fields.push(UpdateFields {
                        field: name.0.to_string(),
                        value: expr,
                    });
                }

                Ok((Self::Standard { fields, condition }, placeholders))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::ra::{RaComparisonOperator, RaConditionValue};
    use bumpalo::Bump;
    use sql::{BinaryOperator, Literal, Query};

    use super::*;

    #[test]
    fn expressions() {
        let arena = Bump::new();
        let query_str = "UPDATE annotation SET epoch = (epoch*1000) where epoch < 9999999999";

        let update_query = match Query::parse(query_str.as_bytes(), &arena) {
            Ok(Query::Update(u)) => u,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [(
            "annotation".to_string(),
            vec![("epoch".to_string(), DataType::Integer)],
        )]
        .into_iter()
        .collect();

        let (ra_update, parameter_types) = RaUpdate::parse(&update_query, &schemas).unwrap();

        assert_eq!(HashMap::new(), parameter_types);

        dbg!(&ra_update);

        assert_eq!(
            RaUpdate::Standard {
                fields: vec![UpdateFields {
                    field: "epoch".to_string(),
                    value: RaValueExpression::Cast {
                        inner: Box::new(RaValueExpression::BinaryOperation {
                            first: Box::new(RaValueExpression::Attribute {
                                ty: DataType::Integer,
                                name: "epoch".into(),
                                a_id: AttributeId::new(0)
                            }),
                            second: Box::new(RaValueExpression::Cast {
                                inner: Box::new(RaValueExpression::Literal(Literal::SmallInteger(
                                    1000
                                ))),
                                target: DataType::Integer
                            }),
                            operator: BinaryOperator::Multiply,
                        }),
                        target: DataType::Integer
                    }
                }],
                condition: Some(RaCondition::And(vec![RaCondition::Value(Box::new(
                    RaConditionValue::Comparison {
                        first: RaValueExpression::Cast {
                            inner: Box::new(RaValueExpression::Attribute {
                                ty: DataType::Integer,
                                name: "epoch".into(),
                                a_id: AttributeId::new(0)
                            }),
                            target: DataType::BigInteger
                        },
                        second: RaValueExpression::Literal(Literal::BigInteger(9999999999)),
                        comparison: RaComparisonOperator::Less
                    }
                ))])),
            },
            ra_update
        );
    }

    #[test]
    #[ignore = "Should work but dont yet know the correct return data"]
    fn update_from() {
        let arena = Bump::new();
        let query_str = "UPDATE orders SET completed = deliveries.delivered FROM deliveries WHERE orders.id = deliveries.order_id";

        let update_query = match Query::parse(query_str.as_bytes(), &arena) {
            Ok(Query::Update(u)) => u,
            other => panic!("{:?}", other),
        };

        let schemas: Schemas = [
            (
                "orders".to_string(),
                vec![
                    ("name".to_string(), DataType::Text),
                    ("id".into(), DataType::Integer),
                    ("completed".into(), DataType::Bool),
                ],
            ),
            (
                "deliveries".to_string(),
                vec![
                    ("order_id".to_string(), DataType::Integer),
                    ("delivered".into(), DataType::Bool),
                ],
            ),
        ]
        .into_iter()
        .collect();

        let (ra_update, parameter_types) = RaUpdate::parse(&update_query, &schemas).unwrap();

        assert_eq!(HashMap::new(), parameter_types);

        dbg!(&ra_update);

        todo!()
    }
}
