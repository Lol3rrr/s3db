use std::collections::HashMap;

use pretty_assertions::assert_eq;

use bumpalo::Bump;

use s3db::{
    ra::{
        self, AggregateExpression, AggregationCondition, Attribute, AttributeId, CTEQuery,
        CTEValue, ParsingContext, ProjectionAttribute, RaComparisonOperator, RaCondition,
        RaConditionValue, RaExpression, RaUpdate, RaValueExpression, CTE,
    },
    storage::Schemas,
};
use sql::{
    BinaryOperator, ColumnReference, DataType, Identifier, JoinKind, Literal, Query, Select,
    TypeModifier, ValueExpression,
};

#[test]
fn count_all_rows() {
    let query_str = "SELECT count(*) FROM users";

    let arena = Bump::new();
    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(select)) => select,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [(
        "users".to_string(),
        vec![("name".to_string(), DataType::Text)],
    )]
    .into_iter()
    .collect();

    let (expression, parameter_types) =
        RaExpression::parse_select(&select_query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::Aggregation {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("users".into()),
                    columns: vec![("name".to_string(), DataType::Text, AttributeId::new(0))]
                }),
                attributes: vec![Attribute {
                    id: AttributeId::new(1),
                    name: "".to_string(),
                    value: AggregateExpression::CountRows,
                }],
                aggregation_condition: AggregationCondition::Everything
            }),
            attributes: vec![ProjectionAttribute {
                id: AttributeId::new(2),
                name: "".into(),
                value: RaValueExpression::Attribute {
                    name: "".into(),
                    ty: DataType::BigInteger,
                    a_id: AttributeId::new(1)
                }
            }]
        },
        expression
    );
}

#[test]
fn group_by() {
    let query_str = "SELECT role FROM users GROUP BY role";

    let arena = Bump::new();
    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(select)) => select,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [(
        "users".to_string(),
        vec![
            ("name".to_string(), DataType::Text),
            ("role".to_string(), DataType::Text),
        ],
    )]
    .into_iter()
    .collect();

    let (expression, parameter_types) =
        RaExpression::parse_select(&select_query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::Aggregation {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("users".into()),
                    columns: vec![
                        ("name".to_string(), DataType::Text, AttributeId::new(0)),
                        ("role".to_string(), DataType::Text, AttributeId::new(1))
                    ]
                }),
                attributes: vec![Attribute {
                    id: AttributeId::new(2),
                    name: "role".to_string(),
                    value: AggregateExpression::Column {
                        name: "role".to_string(),
                        dtype: DataType::Text,
                        a_id: AttributeId::new(1)
                    },
                }],
                aggregation_condition: AggregationCondition::GroupBy {
                    fields: vec![("role".to_string(), AttributeId::new(1))]
                }
            }),
            attributes: vec![ProjectionAttribute {
                id: AttributeId::new(3),
                name: "role".into(),
                value: RaValueExpression::Attribute {
                    name: "role".into(),
                    ty: DataType::Text,
                    a_id: AttributeId::new(2)
                }
            }]
        },
        expression
    );
}

#[test]
fn count_values() {
    let query_str = "SELECT count(role) FROM users";

    let arena = Bump::new();
    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(select)) => select,
        other => panic!("{:?}", other),
    };

    dbg!(&select_query);

    let schemas: Schemas = [(
        "users".to_string(),
        vec![
            ("name".to_string(), DataType::Text),
            ("role".to_string(), DataType::Text),
        ],
    )]
    .into_iter()
    .collect();

    let (expression, parameter_types) =
        RaExpression::parse_select(&select_query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::Aggregation {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("users".into()),
                    columns: vec![
                        ("name".to_string(), DataType::Text, AttributeId::new(0)),
                        ("role".to_string(), DataType::Text, AttributeId::new(1))
                    ]
                }),
                attributes: vec![Attribute {
                    id: AttributeId::new(2),
                    name: "".to_string(),
                    value: AggregateExpression::Count {
                        a_id: AttributeId::new(1)
                    },
                }],
                aggregation_condition: AggregationCondition::Everything
            }),
            attributes: vec![ProjectionAttribute {
                id: AttributeId::new(3),
                name: "".into(),
                value: RaValueExpression::Attribute {
                    name: "".into(),
                    ty: DataType::BigInteger,
                    a_id: AttributeId::new(2)
                }
            }]
        },
        expression
    );
}

#[test]
fn count_with_rename() {
    let query_str = "SELECT count(role) as role_count FROM users";

    let arena = Bump::new();
    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(select)) => select,
        other => panic!("{:?}", other),
    };

    dbg!(&select_query);

    let schemas: Schemas = [(
        "users".to_string(),
        vec![
            ("name".to_string(), DataType::Text),
            ("role".to_string(), DataType::Text),
        ],
    )]
    .into_iter()
    .collect();

    let (expression, parameter_types) =
        RaExpression::parse_select(&select_query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::Aggregation {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("users".into()),
                    columns: vec![
                        ("name".to_string(), DataType::Text, AttributeId::new(0)),
                        ("role".to_string(), DataType::Text, AttributeId::new(1))
                    ]
                }),
                attributes: vec![Attribute {
                    id: AttributeId::new(2),
                    name: "role_count".to_string(),
                    value: AggregateExpression::Renamed {
                        inner: Box::new(AggregateExpression::Count {
                            a_id: AttributeId::new(1)
                        }),
                        name: "role_count".to_string()
                    },
                }],
                aggregation_condition: AggregationCondition::Everything
            }),
            attributes: vec![ProjectionAttribute {
                id: AttributeId::new(3),
                name: "role_count".into(),
                value: RaValueExpression::Attribute {
                    name: "role_count".into(),
                    ty: DataType::BigInteger,
                    a_id: AttributeId::new(2)
                }
            }]
        },
        expression
    );
}

#[test]
fn select_from_subquery() {
    let arena = Bump::new();
    let query_str = "SELECT role_name FROM (SELECT role as role_name FROM users)";

    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(select)) => select,
        other => panic!("{:?}", other),
    };

    dbg!(&select_query);

    let schemas: Schemas = [(
        "users".to_string(),
        vec![
            ("name".to_string(), DataType::Text),
            ("role".to_string(), DataType::Text),
        ],
    )]
    .into_iter()
    .collect();

    let (expression, parameter_types) =
        RaExpression::parse_select(&select_query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::Projection {
                inner: Box::new(RaExpression::BaseRelation {
                    name: Identifier("users".into()),
                    columns: vec![
                        ("name".to_string(), DataType::Text, AttributeId::new(0)),
                        ("role".to_string(), DataType::Text, AttributeId::new(1))
                    ]
                }),
                attributes: vec![Attribute {
                    id: AttributeId::new(2),
                    name: "role_name".to_string(),
                    value: RaValueExpression::Attribute {
                        name: "role".to_string(),
                        ty: DataType::Text,
                        a_id: AttributeId::new(1)
                    }
                }]
            }),
            attributes: vec![Attribute {
                id: AttributeId::new(3),
                name: "role_name".to_string(),
                value: RaValueExpression::Attribute {
                    name: "role_name".to_string(),
                    ty: DataType::Text,
                    a_id: AttributeId::new(2)
                }
            }]
        },
        expression
    );
}

#[test]
fn delete_with_exist() {
    let arena = Bump::new();
    let query_str =
        "DELETE FROM users WHERE NOT EXISTS (SELECT 1 FROM deleted_users WHERE name = 'something')";

    let delete_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Delete(delete)) => delete,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [
        (
            "users".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("role".to_string(), DataType::Text),
            ],
        ),
        (
            "deleted_users".to_string(),
            vec![("name".to_string(), DataType::Text)],
        ),
    ]
    .into_iter()
    .collect();

    let (expression, parameter_types) =
        RaCondition::parse(delete_query.condition.as_ref().unwrap(), &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    dbg!(&expression);

    // todo!("Finish test")
}

#[test]
fn select_with_lpad_concat_cast() {
    let arena = Bump::new();
    let query_str = "SELECT lpad('' || id::text, 5, '0') FROM users";

    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [(
        "users".to_string(),
        vec![
            ("name".to_string(), DataType::Text),
            ("id".to_string(), DataType::Integer),
        ],
    )]
    .into_iter()
    .collect();

    let (expression, parameter_types) =
        RaExpression::parse_select(&select_query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::BaseRelation {
                name: Identifier("users".into()),
                columns: vec![
                    ("name".into(), DataType::Text, AttributeId::new(0)),
                    ("id".into(), DataType::Integer, AttributeId::new(1))
                ]
            }),
            attributes: vec![Attribute {
                name: "".to_string(),
                id: AttributeId::new(2),
                value: RaValueExpression::Function(ra::RaFunction::LeftPad {
                    base: Box::new(RaValueExpression::BinaryOperation {
                        first: Box::new(RaValueExpression::Literal(Literal::Str("".into()))),
                        second: Box::new(RaValueExpression::Cast {
                            inner: Box::new(RaValueExpression::Attribute {
                                ty: DataType::Integer,
                                name: "id".into(),
                                a_id: AttributeId::new(1)
                            }),
                            target: DataType::Text
                        }),
                        operator: BinaryOperator::Concat
                    }),
                    length: 5,
                    padding: "0".to_string(),
                })
            }]
        },
        expression
    );
}

#[test]
fn update_fields_using_parameters() {
    let arena = Bump::new();
    let query_str = "UPDATE user SET last_login = $1 WHERE name = $2";

    let update_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Update(u)) => u,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [(
        "user".to_string(),
        vec![
            ("name".to_string(), DataType::Text),
            ("last_login".to_string(), DataType::Timestamp),
        ],
    )]
    .into_iter()
    .collect();

    let (update, parameter_types) = RaUpdate::parse(&update_query, &schemas).unwrap();

    assert_eq!(
        [(1, DataType::Timestamp), (2, DataType::Text)]
            .into_iter()
            .collect::<HashMap<_, _>>(),
        parameter_types
    );

    assert_eq!(
        RaUpdate::Standard {
            fields: vec![ra::UpdateFields {
                field: "last_login".into(),
                value: RaValueExpression::Placeholder(1)
            }],
            condition: Some(RaCondition::And(vec![RaCondition::Value(Box::new(
                RaConditionValue::Comparison {
                    first: RaValueExpression::Attribute {
                        ty: DataType::Text,
                        name: "name".into(),
                        a_id: AttributeId::new(0)
                    },
                    second: RaValueExpression::Placeholder(2),
                    comparison: RaComparisonOperator::Equals,
                }
            ))]))
        },
        update
    );
}

#[test]
fn inner_join() {
    let arena = Bump::new();
    let query = "SELECT user.name, password.hash FROM user JOIN password ON user.id = password.uid";

    let query = match Query::parse(query.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [
        (
            "user".to_string(),
            vec![
                ("id".to_string(), DataType::Integer, vec![]),
                ("name".to_string(), DataType::Text, vec![]),
            ],
        ),
        (
            "password".to_string(),
            vec![
                ("uid".to_string(), DataType::Integer, vec![]),
                ("hash".to_string(), DataType::Text, vec![]),
            ],
        ),
    ]
    .into_iter()
    .collect();

    let (ra_expr, placeholders) = RaExpression::parse_select(&query, &schemas).unwrap();
    assert_eq!(HashMap::new(), placeholders);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::Join {
                left: Box::new(RaExpression::BaseRelation {
                    name: "user".into(),
                    columns: vec![
                        ("id".into(), DataType::Integer, AttributeId::new(0)),
                        ("name".into(), DataType::Text, AttributeId::new(1))
                    ]
                }),
                right: Box::new(RaExpression::BaseRelation {
                    name: "password".into(),
                    columns: vec![
                        ("uid".into(), DataType::Integer, AttributeId::new(2)),
                        ("hash".into(), DataType::Text, AttributeId::new(3))
                    ]
                }),
                kind: JoinKind::Inner,
                condition: RaCondition::And(vec![RaCondition::Value(Box::new(
                    RaConditionValue::Comparison {
                        first: RaValueExpression::Attribute {
                            name: "id".into(),
                            ty: DataType::Integer,
                            a_id: AttributeId::new(0)
                        },
                        second: RaValueExpression::Attribute {
                            name: "uid".into(),
                            ty: DataType::Integer,
                            a_id: AttributeId::new(2)
                        },
                        comparison: RaComparisonOperator::Equals
                    }
                ))]),
            }),
            attributes: vec![
                ProjectionAttribute {
                    id: AttributeId::new(4),
                    name: "name".into(),
                    value: RaValueExpression::Attribute {
                        name: "name".into(),
                        ty: DataType::Text,
                        a_id: AttributeId::new(1)
                    }
                },
                ProjectionAttribute {
                    id: AttributeId::new(5),
                    name: "hash".into(),
                    value: RaValueExpression::Attribute {
                        name: "hash".into(),
                        ty: DataType::Text,
                        a_id: AttributeId::new(3)
                    }
                }
            ]
        },
        ra_expr
    );
}

#[test]
fn group_by_primary_key() {
    // Explaination:
    // This is okay to do, because even tho we are selecting values that are not part of the Groupy
    // By statement and are not Aggregate Expressions, they can clearly be identified as it gets
    // grouped by the primary key of their table and therefore only the values from that one row
    // are present
    let arena = Bump::new();
    let query_str = "
        SELECT dashboard.id, dashboard.uid, dashboard.is_folder, dashboard.org_id, count(dashboard_acl.id) as count
        FROM dashboard
        LEFT JOIN dashboard_acl
        ON dashboard.id = dashboard_acl.dashboard_id
        WHERE dashboard.has_acl IS TRUE
        GROUP BY dashboard.id
        ";

    dbg!(query_str);

    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(u)) => u,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [
        (
            "dashboard".to_string(),
            vec![
                (
                    "id".to_string(),
                    DataType::Integer,
                    vec![TypeModifier::PrimaryKey],
                ),
                ("uid".to_string(), DataType::Integer, vec![]),
                ("org_id".to_string(), DataType::Integer, vec![]),
                ("has_acl".to_string(), DataType::Bool, vec![]),
                ("is_folder".to_string(), DataType::Bool, vec![]),
            ],
        ),
        (
            "dashboard_acl".to_string(),
            vec![("dashboard_id".to_string(), DataType::Integer, vec![])],
        ),
    ]
    .into_iter()
    .collect();

    let (select, parameter_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();
    assert_eq!(HashMap::new(), parameter_types);

    let _ = select;

    // TODO
    /*
    let (inner, attributes, aggregation_condition) = match select {
        RaExpression::Aggregation {
            inner,
            attributes,
            aggregation_condition,
        } => (inner, attributes, aggregation_condition),
        RaExpression::Projection { inner, attributes } => match inner {

        },
        other => panic!("Unexpected RaExpression: {:?}", other),
    };

    dbg!(&inner, &attributes);

    assert_eq!(
        AggregationCondition::GroupBy {
            fields: vec![
                ("id".to_string(), AttributeId::new(0)),
                ("uid".to_string(), AttributeId::new(1)),
                ("org_id".to_string(), AttributeId::new(2)),
                ("has_acl".to_string(), AttributeId::new(3)),
                ("is_folder".to_string(), AttributeId::new(4))
            ]
        },
        aggregation_condition
    );
    */
}

#[test]
fn select_with_lower_paramater() {
    let arena = Bump::new();
    let query_str = "
        SELECT user.name
        FROM user
        WHERE LOWER(user.name) = LOWER($1)
        ";

    dbg!(query_str);

    let select_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(u)) => u,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [(
        "user".to_string(),
        vec![("name".to_string(), DataType::Text)],
    )]
    .into_iter()
    .collect();

    let (select, parameter_types) = RaExpression::parse_select(&select_query, &schemas).unwrap();
    assert_eq!(
        [(1, DataType::Text)].into_iter().collect::<HashMap<_, _>>(),
        parameter_types
    );

    // TODO
    let _ = select;
}

#[test]
#[ignore = "Something"]
fn select_something() {
    let arena = Bump::new();
    let query_str = "SELECT d.uid, d.title FROM \"dashboard\" AS \"d\" WHERE (is_folder = $1) AND (EXISTS (SELECT 1 FROM alert_rule a WHERE d.uid = a.namespace_uid))";

    let tmp = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&tmp);

    todo!()
}

#[test]
fn select_with_subqueries() {
    let arena = Bump::new();
    let query_str = "SELECT
        (SELECT COUNT(*) FROM users) as user_count";

    let query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&query);

    let schemas: Schemas = [(
        "users".to_string(),
        vec![("name".to_string(), DataType::Text)],
    )]
    .into_iter()
    .collect();

    let (select, parameter_types) = RaExpression::parse_select(&query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    dbg!(&select);
}

#[test]
fn parse_with_context_cte() {
    let mut context = ParsingContext::new();

    context.add_cte(CTE {
        name: "cte".into(),
        value: CTEValue::Standard {
            query: CTEQuery::Select(RaExpression::Projection {
                inner: Box::new(RaExpression::EmptyRelation),
                attributes: vec![ProjectionAttribute {
                    id: AttributeId::new(0),
                    name: "name".into(),
                    value: RaValueExpression::Literal(Literal::SmallInteger(1)),
                }],
            }),
        },
    });

    let query = Select {
        values: vec![ValueExpression::ColumnReference(ColumnReference {
            relation: None,
            column: "name".into(),
        })]
        .into(),
        table: Some(sql::TableExpression::Relation("cte".into())),
        where_condition: None,
        order_by: None,
        group_by: None,
        having: None,
        limit: None,
        for_update: None,
        combine: None,
    };

    let schemas: Schemas = Schemas {
        tables: HashMap::new(),
    };

    let (expr, placeholders) =
        RaExpression::parse_select_with_context(&query, &schemas, &context).unwrap();
    assert_eq!(HashMap::new(), placeholders);

    assert_eq!(
        RaExpression::Projection {
            inner: Box::new(RaExpression::CTE {
                name: "cte".into(),
                columns: vec![("name".into(), DataType::SmallInteger, AttributeId::new(0))]
            }),
            attributes: vec![ProjectionAttribute {
                id: AttributeId::new(0),
                name: "name".into(),
                value: RaValueExpression::Attribute {
                    name: "name".into(),
                    ty: DataType::SmallInteger,
                    a_id: AttributeId::new(0)
                }
            }]
        },
        expr
    );
}

#[test]
#[ignore = "Testing"]
fn setval() {
    let arena = Bump::new();
    let query_str = "SELECT setval('org_id_seq', (SELECT max(id) FROM org))";

    let query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&query);

    let schemas: Schemas = [(
        "org".to_string(),
        vec![("id".to_string(), DataType::Integer)],
    )]
    .into_iter()
    .collect();

    let (select, parameter_types) = RaExpression::parse_select(&query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    dbg!(&select);

    todo!()
}

#[test]
#[ignore = "TODO"]
fn something_else() {
    let arena = Bump::new();
    let query_str = "SELECT
        dashboard.id, dashboard.uid, dashboard.title, dashboard.slug, dashboard_tag.term, dashboard.is_folder, dashboard.folder_id, folder.uid AS folder_uid,\n\t\t\n\t\t\tfolder.slug AS folder_slug,\n\t\t\tfolder.title AS folder_title
        FROM ( SELECT dashboard.id FROM dashboard WHERE (NOT dashboard.is_folder OR dashboard.is_folder) AND dashboard.org_id=$1 ORDER BY dashboard.title ASC NULLS FIRST LIMIT 30 OFFSET 0) AS ids\n\t\t
        INNER JOIN dashboard ON ids.id = dashboard.id\n\n\t\t
        LEFT OUTER JOIN dashboard AS folder ON folder.id = dashboard.folder_id\n\t
        LEFT OUTER JOIN dashboard_tag ON dashboard.id = dashboard_tag.dashboard_id\n
        ORDER BY dashboard.title ASC NULLS FIRST";

    let select = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [
        (
            "dashboard".to_string(),
            vec![
                ("id".to_string(), DataType::Integer),
                ("is_folder".to_string(), DataType::Bool),
            ],
        ),
        (
            "folder".to_string(),
            vec![("id".to_string(), DataType::Integer)],
        ),
        (
            "dashboard_tag".to_string(),
            vec![("id".to_string(), DataType::Integer)],
        ),
    ]
    .into_iter()
    .collect();

    let (select, placeholders) = RaExpression::parse_select(&select, &schemas).unwrap();

    dbg!(&select, &placeholders);

    todo!()
}

#[test]
fn select_as_single_value() {
    let arena = Bump::new();
    let query_str = "SELECT
(
    SELECT COUNT(*)
    FROM \"user\"
    WHERE is_service_account = true
) AS serviceaccounts,
(
    SELECT COUNT(*)
    FROM \"api_key\"
    WHERE service_account_id IS NOT NULL
) AS serviceaccount_tokens,
(
    SELECT COUNT(*)
    FROM \"org_user\" AS ou JOIN \"user\" AS u ON u.id = ou.user_id
    WHERE u.is_disabled = false AND u.is_service_account = true AND ou.role=$1
) AS serviceaccounts_with_no_role";

    let select = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [
        (
            "user".to_string(),
            vec![
                ("id".to_string(), DataType::Integer),
                ("is_service_account".to_string(), DataType::Bool),
                ("is_disabled".to_string(), DataType::Bool),
            ],
        ),
        (
            "api_key".to_string(),
            vec![("service_account_id".to_string(), DataType::Integer)],
        ),
        (
            "org_user".to_string(),
            vec![
                ("user_id".to_string(), DataType::Integer),
                ("role".into(), DataType::Text),
            ],
        ),
    ]
    .into_iter()
    .collect();

    let (select, placeholders) = RaExpression::parse_select(&select, &schemas).unwrap();
    assert_eq!(HashMap::new(), placeholders);
    
    // TODO
    let _ = select;
}

#[test]
fn select_group_by_number() {
    let arena = Bump::new();
    let query_str = "SELECT COUNT(balance), plz FROM users GROUP BY 2";

    let select = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [(
        "users".to_string(),
        vec![
            ("plz".to_string(), DataType::Integer),
            ("balance".to_string(), DataType::Integer),
        ],
    )]
    .into_iter()
    .collect();

    let (select, placeholders) = RaExpression::parse_select(&select, &schemas).unwrap();

    dbg!(&select, &placeholders);

    // todo!()
}
