use ra::{RaCondition, RaExpression};

use super::Optimize;

pub struct Conditions {}

impl Conditions {
    fn flatten(&self, condition: &mut RaCondition) {
        match condition {
            RaCondition::And(parts) => {
                for part in parts.iter_mut() {
                    self.flatten(part);
                }

                let mut n_part = Vec::with_capacity(parts.len());
                for part in parts.drain(..) {
                    match part {
                        RaCondition::And(inner) => {
                            n_part.extend(inner);
                        }
                        other => {
                            n_part.push(other);
                        }
                    };
                }

                *parts = n_part;
            }
            RaCondition::Or(parts) => {
                for part in parts.iter_mut() {
                    self.flatten(part);
                }

                let mut n_part = Vec::with_capacity(parts.len());
                for part in parts.drain(..) {
                    match part {
                        RaCondition::Or(inner) => {
                            n_part.extend(inner);
                        }
                        other => {
                            n_part.push(other);
                        }
                    };
                }

                *parts = n_part;
            }
            RaCondition::Value(_) => {}
        };
    }
}

impl Optimize for Conditions {
    fn optimize(&self, mut expr: RaExpression) -> RaExpression {
        match &mut expr {
            RaExpression::CTE { .. }
            | RaExpression::Renamed { .. }
            | RaExpression::Chain { .. }
            | RaExpression::Projection { .. }
            | RaExpression::BaseRelation { .. }
            | RaExpression::Limit { .. }
            | RaExpression::EmptyRelation
            | RaExpression::Aggregation { .. }
            | RaExpression::OrderBy { .. } => {}
            RaExpression::Selection {
                filter: condition, ..
            }
            | RaExpression::Join { condition, .. }
            | RaExpression::LateralJoin { condition, .. } => {
                self.flatten(condition);
            }
        };

        expr
    }
}

#[cfg(test)]
mod tests {
    use ra::{AttributeId, RaConditionValue};
    use sql::DataType;

    use super::*;

    #[test]
    fn flatten_ands() {
        let src = RaExpression::Selection {
            inner: Box::new(RaExpression::EmptyRelation),
            filter: RaCondition::And(vec![RaCondition::And(vec![RaCondition::Value(Box::new(
                RaConditionValue::Attribute {
                    name: "".into(),
                    ty: DataType::Bool,
                    a_id: AttributeId::new(0),
                },
            ))])]),
        };

        let optimizer = Conditions {};

        let output = optimizer.optimize(src.clone());

        assert_eq!(
            RaExpression::Selection {
                inner: Box::new(RaExpression::EmptyRelation),
                filter: RaCondition::And(vec![RaCondition::Value(Box::new(
                    RaConditionValue::Attribute {
                        name: "".into(),
                        ty: DataType::Bool,
                        a_id: AttributeId::new(0),
                    },
                ))])
            },
            output
        );
    }

    #[test]
    fn flatten_ors() {
        let src = RaExpression::Selection {
            inner: Box::new(RaExpression::EmptyRelation),
            filter: RaCondition::Or(vec![RaCondition::Or(vec![RaCondition::Value(Box::new(
                RaConditionValue::Attribute {
                    name: "".into(),
                    ty: DataType::Bool,
                    a_id: AttributeId::new(0),
                },
            ))])]),
        };

        let optimizer = Conditions {};

        let output = optimizer.optimize(src.clone());

        assert_eq!(
            RaExpression::Selection {
                inner: Box::new(RaExpression::EmptyRelation),
                filter: RaCondition::Or(vec![RaCondition::Value(Box::new(
                    RaConditionValue::Attribute {
                        name: "".into(),
                        ty: DataType::Bool,
                        a_id: AttributeId::new(0),
                    },
                ))])
            },
            output
        );
    }
}
