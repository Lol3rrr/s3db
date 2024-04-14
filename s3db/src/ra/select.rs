use std::collections::HashMap;

use sql::{Condition, DataType, GroupAttribute, OrderAttribute, Ordering, Select, TypeModifier};

use super::{
    AggregateExpression, AggregationCondition, ParseSelectError, ProjectionAttribute, RaCondition,
    RaExpression, RaValueExpression, Scope,
};

pub fn parse_condition(
    mut base: RaExpression,
    condition: &Option<Condition<'_>>,
    scope: &mut Scope<'_>,
    placeholders: &mut HashMap<usize, DataType>,
    outer: &mut Vec<RaExpression>,
) -> Result<RaExpression, ParseSelectError> {
    match condition {
        Some(condition) => {
            let or_condition =
                RaCondition::parse_internal(scope, condition, placeholders, &mut base, outer)?;

            Ok(RaExpression::Selection {
                inner: Box::new(base),
                filter: or_condition,
            })
        }
        None => Ok(base),
    }
}

pub fn parse_order(
    base: RaExpression,
    order: &Option<Vec<Ordering<'_>>>,
) -> Result<RaExpression, ParseSelectError> {
    match order {
        Some(orders) => {
            let attributes: Vec<_> = orders
                .iter()
                .map(|ordering| {
                    let attr = match &ordering.column {
                        OrderAttribute::ColumnRef(column) => base
                            .get_columns()
                            .into_iter()
                            .find(|(_, name, _, _)| name == column.column.0.as_ref())
                            .map(|(_, _, _, id)| id)
                            .ok_or_else(|| {
                                dbg!(&base, base.get_columns(), column);

                                ParseSelectError::Other("Getting Column Reference for Order")
                            })?,
                        OrderAttribute::ColumnIndex(idx) => base
                            .get_columns()
                            .into_iter()
                            .nth(*idx - 1)
                            .map(|(_, _, _, id)| id)
                            .ok_or_else(|| {
                                ParseSelectError::Other("Getting Column Index for Order")
                            })?,
                    };

                    Ok((attr, ordering.order.clone()))
                })
                .collect::<Result<_, _>>()?;

            Ok(RaExpression::OrderBy {
                inner: Box::new(base),
                attributes,
            })
        }
        None => Ok(base),
    }
}

pub fn parse_aggregate(
    mut base: RaExpression,
    query: &Select<'_>,
    scope: &mut Scope<'_>,
    placeholders: &mut HashMap<usize, DataType>,
    outer: &mut Vec<RaExpression>,
) -> Result<RaExpression, ParseSelectError> {
    if query.values.iter().any(|v| v.is_aggregate()) || query.group_by.is_some() {
        let previous_columns = base
            .get_columns()
            .into_iter()
            .map(|(_, n, t, i)| (n, t, i))
            .collect::<Vec<_>>();

        let agg_condition = match query.group_by.as_ref() {
            Some(tmp) => {
                let mut fields: Vec<_> = tmp
                    .iter()
                    .map(|field| match field {
                        GroupAttribute::ColumnRef(column) => previous_columns
                            .iter()
                            .find_map(|(n, _, id)| {
                                if n == column.column.0.as_ref() {
                                    Some((n.to_string(), *id))
                                } else {
                                    None
                                }
                            })
                            .ok_or_else(|| ParseSelectError::UnknownAttribute {
                                attr: column.to_static(),
                                available: previous_columns
                                    .iter()
                                    .map(|(n, _, _)| (String::new(), n.clone()))
                                    .collect(),
                                context: "Aggregate Condition Parsing".into(),
                            }),
                        GroupAttribute::ColumnIndex(idx) => previous_columns
                            .get(*idx)
                            .map(|(n, _, id)| (n.to_string(), *id))
                            .ok_or_else(|| {
                                ParseSelectError::Other("Getting Column By Index for Aggregate")
                            }),
                    })
                    .collect::<Result<_, _>>()?;

                let extra_fields_from_primary_keys: Vec<_> = fields
                    .iter()
                    .filter_map(|(f_name, f_id)| base.get_source(*f_id).zip(Some(f_id)))
                    .filter_map(|(src_expr, f_id)| {
                        let (table_name, column_name) = match src_expr {
                            RaExpression::BaseRelation { name, columns } => {
                                let column_name = columns
                                    .iter()
                                    .find(|(_, _, id)| f_id == id)
                                    .map(|(n, _, _)| n)?;

                                (name.0.as_ref(), column_name)
                            }
                            _ => return None,
                        };
                        let table_schema = match scope.schemas.get_table(table_name) {
                            Some(s) => s,
                            None => return None,
                        };
                        let column_schema = match table_schema.get_column_by_name(column_name) {
                            Some(s) => s,
                            None => return None,
                        };

                        if !column_schema
                            .mods
                            .iter()
                            .any(|modifier| modifier == &TypeModifier::PrimaryKey)
                        {
                            return None;
                        }

                        Some(src_expr.get_columns())
                    })
                    .flat_map(|tmp| {
                        tmp.into_iter()
                            .map(|(_, n, _, id)| (n, id))
                            .filter(|(_, id)| previous_columns.iter().any(|(_, _, pid)| id == pid))
                    })
                    .collect();

                fields.extend(extra_fields_from_primary_keys);

                fields.sort_unstable_by_key(|(_, id)| id.0);
                fields.dedup_by_key(|(_, id)| id.0);

                AggregationCondition::GroupBy { fields }
            }
            None => AggregationCondition::Everything,
        };

        let attribute_values: Vec<_> = query
            .values
            .iter()
            .map(|value| {
                AggregateExpression::parse(value, scope, &previous_columns, placeholders, &mut base)
            })
            .collect::<Result<_, _>>()?;

        // TODO
        // Check if everything used here is  correct
        for value in attribute_values.iter() {
            value.value.check(&agg_condition, scope.schemas)?;
        }

        let select_columns: Vec<_> = attribute_values
            .iter()
            .map(|attr| ProjectionAttribute {
                id: scope.attribute_id(),
                name: attr.name.clone(),
                value: RaValueExpression::Attribute {
                    name: attr.name.clone(),
                    ty: attr.value.return_ty(),
                    a_id: attr.id,
                },
            })
            .collect();

        let mut aggregated = RaExpression::Aggregation {
            inner: Box::new(base),
            attributes: attribute_values,
            aggregation_condition: agg_condition,
        };

        let filtered = match query.having.as_ref() {
            Some(having) => {
                let condition = RaCondition::parse_internal(
                    scope,
                    having,
                    placeholders,
                    &mut aggregated,
                    &mut Vec::new(),
                )?;

                RaExpression::Selection {
                    inner: Box::new(aggregated),
                    filter: condition,
                }
            }
            None => aggregated,
        };

        Ok(RaExpression::Projection {
            inner: Box::new(filtered),
            attributes: select_columns,
        })
    } else {
        let select_attributes: Vec<Vec<ProjectionAttribute>> = query
            .values
            .iter()
            .map(|ve| {
                ProjectionAttribute::parse_internal(scope, ve, placeholders, &mut base, outer)
            })
            .collect::<Result<_, _>>()?;

        Ok(RaExpression::Projection {
            inner: Box::new(base),
            attributes: select_attributes.into_iter().flatten().collect(),
        })
    }
}
