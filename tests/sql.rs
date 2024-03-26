use pretty_assertions::assert_eq;

use s3db::sql::{
    AggregateExpression, BinaryOperator, ColumnReference, Condition, ConflictHandling, Delete,
    FunctionCall, Identifier, Insert, InsertValues, Literal, NullOrdering, OrderBy, Ordering,
    Query, Select, SelectLimit, TableExpression, ValueExpression,
};

#[test]
fn insert_with_on_conflict() {
    let query_str = "\n\t\t\tINSERT INTO folder (uid, org_id, title, created, updated)\n\t\t\tSELECT uid, org_id, title, created, updated FROM dashboard WHERE is_folder = true\n\t\t\tON CONFLICT(uid, org_id) DO UPDATE SET title=excluded.title, updated=excluded.updated\n\t\t";

    let insert_query = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Insert(i)) => i,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Insert {
            table: Identifier("folder".into()),
            fields: vec![
                Identifier("uid".into()),
                Identifier("org_id".into()),
                Identifier("title".into()),
                Identifier("created".into()),
                Identifier("updated".into())
            ],
            values: InsertValues::Select(Select {
                values: vec![
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("uid".into()),
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("org_id".into()),
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("title".into()),
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("created".into()),
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("updated".into()),
                    })
                ],
                table: Some(TableExpression::Relation(Identifier("dashboard".into()))),
                where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("is_folder".into())
                        })),
                        second: Box::new(ValueExpression::Literal(Literal::Bool(true))),
                        operator: BinaryOperator::Equal
                    }
                ))])),
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }),
            returning: None,
            on_conflict: Some(ConflictHandling {
                attributes: vec![Identifier("uid".into()), Identifier("org_id".into())],
                update: vec![
                    (
                        ColumnReference {
                            relation: None,
                            column: Identifier("title".into()),
                        },
                        ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("excluded".into())),
                            column: Identifier("title".into())
                        })
                    ),
                    (
                        ColumnReference {
                            relation: None,
                            column: Identifier("updated".into()),
                        },
                        ValueExpression::ColumnReference(ColumnReference {
                            relation: Some(Identifier("excluded".into())),
                            column: Identifier("updated".into())
                        })
                    )
                ]
            })
        },
        insert_query
    );
}

#[test]
fn delete_test() {
    let query_str = "\n\t\t\tDELETE FROM folder WHERE NOT EXISTS\n\t\t\t\t(SELECT 1 FROM dashboard WHERE dashboard.uid = folder.uid AND dashboard.org_id = folder.org_id AND dashboard.is_folder = true)\n\t";

    let delete_query = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Delete(d)) => d,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Delete {
            table: Identifier("folder".into()),
            condition: Some(Condition::And(vec![Condition::Value(Box::new(
                ValueExpression::Not(Box::new(ValueExpression::FunctionCall(
                    FunctionCall::Exists {
                        query: Box::new(Select {
                            values: vec![ValueExpression::Literal(Literal::SmallInteger(1))],
                            table: Some(TableExpression::Relation(Identifier("dashboard".into()))),
                            where_condition: Some(Condition::And(vec![
                                Condition::Value(Box::new(ValueExpression::Operator {
                                    first: Box::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("dashboard".into())),
                                            column: Identifier("uid".into())
                                        }
                                    )),
                                    second: Box::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("folder".into())),
                                            column: Identifier("uid".into())
                                        }
                                    )),
                                    operator: BinaryOperator::Equal
                                })),
                                Condition::Value(Box::new(ValueExpression::Operator {
                                    first: Box::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("dashboard".into())),
                                            column: Identifier("org_id".into())
                                        }
                                    )),
                                    second: Box::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("folder".into())),
                                            column: Identifier("org_id".into())
                                        }
                                    )),
                                    operator: BinaryOperator::Equal
                                })),
                                Condition::Value(Box::new(ValueExpression::Operator {
                                    first: Box::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("dashboard".into())),
                                            column: Identifier("is_folder".into())
                                        }
                                    )),
                                    second: Box::new(ValueExpression::Literal(Literal::Bool(true))),
                                    operator: BinaryOperator::Equal
                                }))
                            ])),
                            order_by: None,
                            group_by: None,
                            having: None,
                            limit: None,
                            for_update: None,
                            combine: None
                        })
                    }
                )))
            ))])),
        },
        delete_query
    );
}

#[test]
#[ignore = "TODO Comparison"]
fn something() {
    let query_str = "
        SELECT \"id\", \"version\", \"email\", \"name\", \"login\", \"password\", \"salt\", \"rands\", \"company\", \"email_verified\", \"theme\", \"help_flags1\", \"is_disabled\", \"is_admin\", \"is_service_account\", \"org_id\", \"created\", \"updated\", \"last_seen_at\"
        FROM \"user\"
        WHERE (LOWER(email)=LOWER($1) OR LOWER(login)=LOWER($2))
        LIMIT 1";

    let query = match Query::parse(query_str.as_bytes()).unwrap() {
        Query::Select(s) => s,
        other => panic!("{:?}", other),
    };

    /*
    assert_eq!(
        Select {
            values: vec![],
            table: None,
            where_condition: None,
            order_by: None,
            group_by: None,
            limit: None,
        },
        query
    );
    */
}

#[test]
fn select_for_update() {
    let query_str = "SELECT * FROM server_lock WHERE operation_uid = $1 FOR UPDATE";

    let query = match Query::parse(query_str.as_bytes()).unwrap() {
        Query::Select(s) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&query);

    assert_eq!(
        Select {
            values: vec![ValueExpression::All],
            table: Some(TableExpression::Relation(Identifier("server_lock".into()))),
            where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                ValueExpression::Operator {
                    first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("operation_uid".into())
                    })),
                    second: Box::new(ValueExpression::Placeholder(1)),
                    operator: BinaryOperator::Equal
                }
            ))])),
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: Some(()),
            combine: None
        },
        query
    );
}

#[test]
fn select_limit_offset() {
    let query_str = "SELECT \"id\", \"id\", \"alertmanager_configuration\", \"configuration_hash\", \"configuration_version\", \"created_at\", \"default\", \"org_id\", \"last_applied\" FROM \"alert_configuration_history\" WHERE (org_id = $1) ORDER BY \"id\" DESC, id LIMIT 1 OFFSET 99";

    let query = match Query::parse(query_str.as_bytes()).unwrap() {
        Query::Select(s) => s,
        other => panic!("{:?}", other),
    };

    // TODO
    // Also store the OFFSET somewhere

    assert_eq!(
        Select {
            values: vec![
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("alertmanager_configuration".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("configuration_hash".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("configuration_version".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("created_at".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("default".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("org_id".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("last_applied".into())
                })
            ],
            table: Some(TableExpression::Relation(Identifier(
                "alert_configuration_history".into()
            ))),
            where_condition: Some(Condition::And(vec![Condition::And(vec![
                Condition::Value(Box::new(ValueExpression::Operator {
                    first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("org_id".into())
                    })),
                    second: Box::new(ValueExpression::Placeholder(1)),
                    operator: BinaryOperator::Equal
                }))
            ])])),
            order_by: Some(vec![
                Ordering {
                    column: ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    },
                    order: OrderBy::Descending,
                    nulls: NullOrdering::First,
                },
                Ordering {
                    column: ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    },
                    order: OrderBy::Ascending,
                    nulls: NullOrdering::Last
                }
            ]),
            group_by: None,
            having: None,
            limit: Some(SelectLimit {
                limit: 1,
                offset: Some(99)
            }),
            for_update: None,
            combine: None,
        },
        query
    );
}

#[test]
fn select_something() {
    let query_str = "SELECT \"org_id\", \"namespace\", \"key\" FROM \"kv_store\" WHERE (namespace = $1) AND (\"key\" LIKE $2)";

    let query = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Select {
            values: vec![
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("org_id".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("namespace".into())
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("key".into())
                })
            ],
            table: Some(TableExpression::Relation(Identifier("kv_store".into()))),
            where_condition: Some(Condition::And(vec![
                Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("namespace".into())
                        })),
                        second: Box::new(ValueExpression::Placeholder(1)),
                        operator: BinaryOperator::Equal
                    }
                ))]),
                Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("key".into())
                        })),
                        second: Box::new(ValueExpression::Placeholder(2)),
                        operator: BinaryOperator::Like
                    }
                ))])
            ])),
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: None,
            combine: None
        },
        query
    );
}

#[test]
fn select_other() {
    let query_str = "SELECT
            (SELECT COUNT(*) FROM \"user\" WHERE is_service_account = true) AS serviceaccounts,
            (SELECT COUNT(*) FROM \"api_key\"WHERE service_account_id IS NOT NULL ) AS serviceaccount_tokens,
            (SELECT COUNT(*) FROM \"org_user\" AS ou JOIN \"user\" AS u ON u.id = ou.user_id WHERE u.is_disabled = false AND u.is_service_account = true AND ou.role=$1) AS serviceaccounts_with_no_role";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };
    dbg!(&select);
}

#[test]
fn update_something() {
    let query_str = "UPDATE \"alert_configuration_history\" SET \"last_applied\" = $1 WHERE (org_id = $2 AND configuration_hash = $3) AND (CTID IN (SELECT CTID FROM \"alert_configuration_history\" WHERE (org_id = $4 AND configuration_hash = $5) ORDER BY \"id\" DESC LIMIT 1))";

    let update = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Update(u)) => u,
        other => panic!("{:?}", other),
    };

    dbg!(&update);

    // todo!()
}

#[test]
#[ignore = "TODO"]
fn testing() {
    //  let query_str = "\n\t\tSELECT\n\t\t\tpermission.action,\n\t\t\tpermission.scope\n\t\t\tFROM permission\n\t\t\tINNER JOIN role ON role.id = permission.role_id\n\t\tINNER JOIN (\n\t\t\tSELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($1)\n\t\t\tAND (br.org_id = $2 OR br.org_id = $3)\n\t\t) as all_role ON role.id = all_role.role_id WHERE ( role.name LIKE $4 OR role.name LIKE $5 )";

    let query_str = "\n\t\tSELECT\n\t\t\tpermission.action,\n\t\t\tpermission.scope\n\t\t\tFROM permission\n\t\t\tINNER JOIN role ON role.id = permission.role_id\n\t\tINNER JOIN (\n\t\t\tSELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($1)\n\t\t\tAND (br.org_id = $2 OR br.org_id = $3)\n\t\t) as all_role ON role.id = all_role.role_id";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&select);

    todo!()
}

#[test]
fn testing_subquery() {
    let query_str = "SELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($1)\n\t\t\tAND (br.org_id = $2 OR br.org_id = $3)";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&select);

    assert_eq!(
        Select {
            values: vec![ValueExpression::ColumnReference(ColumnReference {
                relation: Some(Identifier("br".into())),
                column: "role_id".into()
            })],
            table: Some(TableExpression::Renamed {
                inner: Box::new(TableExpression::Relation("builtin_role".into())),
                name: "br".into()
            }),
            where_condition: Some(Condition::And(vec![
                Condition::Value(Box::new(ValueExpression::Operator {
                    first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: Some("br".into()),
                        column: "role".into()
                    })),
                    second: Box::new(ValueExpression::List(vec![ValueExpression::Placeholder(1)])),
                    operator: BinaryOperator::In,
                })),
                Condition::Or(vec![
                    Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("br".into()),
                                column: "org_id".into()
                            })),
                            second: Box::new(ValueExpression::Placeholder(2)),
                            operator: BinaryOperator::Equal,
                        }
                    ))]),
                    Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("br".into()),
                                column: "org_id".into()
                            })),
                            second: Box::new(ValueExpression::Placeholder(3)),
                            operator: BinaryOperator::Equal,
                        }
                    ))])
                ])
            ])),
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: None,
            combine: None,
        },
        select
    );
}

#[test]
#[ignore = "TODO"]
fn testing_something() {
    let query_str = "\n\t\tSELECT\n\t\t\tpermission.action,\n\t\t\tpermission.scope\n\t\t\tFROM permission\n\t\t\tINNER JOIN role ON role.id = permission.role_id\n\t\tINNER JOIN (\n\t\t\tSELECT ur.role_id\n\t\t\tFROM user_role AS ur\n\t\t\tWHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\tUNION\n\t\t\tSELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t) as all_role ON role.id = all_role.role_id WHERE ( role.name LIKE $8 OR role.name LIKE $9 )";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&select);

    assert_eq!(
        Select {
            values: vec![],
            table: None,
            where_condition: None,
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: None,
            combine: None
        },
        select
    );
}

#[test]
fn with_select() {
    let query_str = "
WITH regional_sales AS (
    SELECT region, SUM(amount) AS total_sales
    FROM orders
    GROUP BY region
), top_regions AS (
    SELECT region
    FROM regional_sales
    WHERE total_sales > (SELECT SUM(total_sales)/10 FROM regional_sales)
)
SELECT region,
       product,
       SUM(quantity) AS product_units,
       SUM(amount) AS product_sales
FROM orders
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, product;";
    let query_res = Query::parse(query_str.as_bytes()).unwrap();

    dbg!(&query_res);
}

#[test]
fn select_aggregate_with_operator() {
    let query_str = "SELECT SUM(total_sales)/10 FROM regional_sales";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Select {
            values: vec![ValueExpression::Operator {
                first: Box::new(ValueExpression::AggregateExpression(
                    AggregateExpression::Sum(Box::new(ValueExpression::ColumnReference(
                        ColumnReference {
                            relation: None,
                            column: "total_sales".into()
                        }
                    )))
                )),
                second: Box::new(ValueExpression::Literal(Literal::SmallInteger(10))),
                operator: BinaryOperator::Divide,
            }],
            table: Some(TableExpression::Relation("regional_sales".into())),
            where_condition: None,
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: None,
            combine: None
        },
        select
    );
}
