use pretty_assertions::assert_eq;

use sql::{
    AggregateExpression, BinaryOperator, ColumnReference, Condition, ConflictHandling, Delete,
    FunctionCall, GroupAttribute, Identifier, Insert, InsertValues, JoinKind, Literal,
    NullOrdering, OrderAttribute, OrderBy, Ordering, Query, Select, SelectLimit, TableExpression,
    ValueExpression, CompatibleParser, arenas::Boxed
};

#[test]
fn insert_with_on_conflict() {
    let arena = bumpalo::Bump::new();
    let query_str = "\n\t\t\tINSERT INTO folder (uid, org_id, title, created, updated)\n\t\t\tSELECT uid, org_id, title, created, updated FROM dashboard WHERE is_folder = true\n\t\t\tON CONFLICT(uid, org_id) DO UPDATE SET title=excluded.title, updated=excluded.updated\n\t\t";

    let insert_query = match Query::parse(query_str.as_bytes(), &arena) {
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
            ].into(),
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
                ].into(),
                table: Some(TableExpression::Relation(Identifier("dashboard".into()))),
                where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("is_folder".into())
                        })),
                        second: Boxed::new(ValueExpression::Literal(Literal::Bool(true))),
                        operator: BinaryOperator::Equal
                    }
                ).into())].into())),
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }),
            returning: None,
            on_conflict: Some(ConflictHandling {
                attributes: vec![Identifier("uid".into()), Identifier("org_id".into())].into(),
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
                ].into()
            })
        },
        insert_query.to_static()
    );
}

#[test]
fn delete_test() {
    let arena = bumpalo::Bump::new();
    let query_str = "\n\t\t\tDELETE FROM folder WHERE NOT EXISTS\n\t\t\t\t(SELECT 1 FROM dashboard WHERE dashboard.uid = folder.uid AND dashboard.org_id = folder.org_id AND dashboard.is_folder = true)\n\t";

    let delete_query = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Delete(d)) => d,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Delete {
            table: Identifier("folder".into()),
            condition: Some(Condition::And(vec![Condition::Value(Boxed::new(
                ValueExpression::Not(Boxed::new(ValueExpression::FunctionCall(
                    FunctionCall::Exists {
                        query: Boxed::new(Select {
                            values: vec![ValueExpression::Literal(Literal::SmallInteger(1))].into(),
                            table: Some(TableExpression::Relation(Identifier("dashboard".into()))),
                            where_condition: Some(Condition::And(vec![
                                Condition::Value(Box::new(ValueExpression::Operator {
                                    first: Boxed::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("dashboard".into())),
                                            column: Identifier("uid".into())
                                        }
                                    )),
                                    second: Boxed::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("folder".into())),
                                            column: Identifier("uid".into())
                                        }
                                    )),
                                    operator: BinaryOperator::Equal
                                }).into()),
                                Condition::Value(Boxed::new(ValueExpression::Operator {
                                    first: Boxed::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("dashboard".into())),
                                            column: Identifier("org_id".into())
                                        }
                                    )),
                                    second: Boxed::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("folder".into())),
                                            column: Identifier("org_id".into())
                                        }
                                    )),
                                    operator: BinaryOperator::Equal
                                }).into()),
                                Condition::Value(Box::new(ValueExpression::Operator {
                                    first: Boxed::new(ValueExpression::ColumnReference(
                                        ColumnReference {
                                            relation: Some(Identifier("dashboard".into())),
                                            column: Identifier("is_folder".into())
                                        }
                                    )),
                                    second: Boxed::new(ValueExpression::Literal(Literal::Bool(true))),
                                    operator: BinaryOperator::Equal
                                }).into())
                            ].into())),
                            order_by: None,
                            group_by: None,
                            having: None,
                            limit: None,
                            for_update: None,
                            combine: None
                        })
                    }
                )))
            ).into())].into())),
        },
        delete_query.to_static()
    );
}

#[test]
fn something() {
    let arena = bumpalo::Bump::new();
    let query_str = "
        SELECT \"id\", \"version\", \"email\", \"name\", \"login\", \"password\", \"salt\", \"rands\", \"company\", \"email_verified\", \"theme\", \"help_flags1\", \"is_disabled\", \"is_admin\", \"is_service_account\", \"org_id\", \"created\", \"updated\", \"last_seen_at\"
        FROM \"user\"
        WHERE (LOWER(email)=LOWER($1) OR LOWER(login)=LOWER($2))
        LIMIT 1";

    let query = match Query::parse(query_str.as_bytes(), &arena).unwrap() {
        Query::Select(s) => s,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Select {
            values: vec![
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "id".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "version".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "email".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "name".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "login".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "password".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "salt".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "rands".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "company".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "email_verified".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "theme".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "help_flags1".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "is_disabled".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "is_admin".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "is_service_account".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "org_id".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "created".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "updated".into()
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "last_seen_at".into()
                })
            ].into(),
            table: Some(TableExpression::Relation("user".into())),
            where_condition: Some(Condition::And(vec![Condition::Or(vec![
                Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::FunctionCall(FunctionCall::Lower {
                            value: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: "email".into()
                            }))
                        })),
                        second: Boxed::new(ValueExpression::FunctionCall(FunctionCall::Lower {
                            value: Boxed::new(ValueExpression::Placeholder(1))
                        })),
                        operator: BinaryOperator::Equal
                    }
                ).into()),].into()),
                Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::FunctionCall(FunctionCall::Lower {
                            value: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: None,
                                column: "login".into()
                            }))
                        })),
                        second: Boxed::new(ValueExpression::FunctionCall(FunctionCall::Lower {
                            value: Boxed::new(ValueExpression::Placeholder(2))
                        })),
                        operator: BinaryOperator::Equal
                    }
                ).into())].into())
            ].into())].into())),
            having: None,
            order_by: None,
            group_by: None,
            limit: Some(SelectLimit {
                limit: 1,
                offset: None,
            }),
            for_update: None,
            combine: None,
        },
        query.to_static()
    );
}

#[test]
fn select_for_update() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT * FROM server_lock WHERE operation_uid = $1 FOR UPDATE";

    let query = match Query::parse(query_str.as_bytes(), &arena).unwrap() {
        Query::Select(s) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&query);

    assert_eq!(
        Select {
            values: vec![ValueExpression::All].into(),
            table: Some(TableExpression::Relation(Identifier("server_lock".into()))),
            where_condition: Some(Condition::And(vec![Condition::Value(Box::new(
                ValueExpression::Operator {
                    first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("operation_uid".into())
                    })),
                    second: Boxed::new(ValueExpression::Placeholder(1)),
                    operator: BinaryOperator::Equal
                }
            ).into())].into())),
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: Some(()),
            combine: None
        },
        query.to_static()
    );
}

#[test]
fn select_limit_offset() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT \"id\", \"id\", \"alertmanager_configuration\", \"configuration_hash\", \"configuration_version\", \"created_at\", \"default\", \"org_id\", \"last_applied\" FROM \"alert_configuration_history\" WHERE (org_id = $1) ORDER BY \"id\" DESC, id LIMIT 1 OFFSET 99";

    let query = match Query::parse(query_str.as_bytes(), &arena).unwrap() {
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
            ].into(),
            table: Some(TableExpression::Relation(Identifier(
                "alert_configuration_history".into()
            ))),
            where_condition: Some(Condition::And(vec![Condition::And(vec![
                Condition::Value(Box::new(ValueExpression::Operator {
                    first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("org_id".into())
                    })),
                    second: Boxed::new(ValueExpression::Placeholder(1)),
                    operator: BinaryOperator::Equal
                }).into())
            ].into())].into())),
            order_by: Some(vec![
                Ordering {
                    column: OrderAttribute::ColumnRef(ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    }),
                    order: OrderBy::Descending,
                    nulls: NullOrdering::First,
                },
                Ordering {
                    column: OrderAttribute::ColumnRef(ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    }),
                    order: OrderBy::Ascending,
                    nulls: NullOrdering::Last
                }
            ].into()),
            group_by: None,
            having: None,
            limit: Some(SelectLimit {
                limit: 1,
                offset: Some(99)
            }),
            for_update: None,
            combine: None,
        },
        query.to_static()
    );
}

#[test]
fn select_something() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT \"org_id\", \"namespace\", \"key\" FROM \"kv_store\" WHERE (namespace = $1) AND (\"key\" LIKE $2)";

    let query = match Query::parse(query_str.as_bytes(), &arena) {
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
            ].into(),
            table: Some(TableExpression::Relation(Identifier("kv_store".into()))),
            where_condition: Some(Condition::And(vec![
                Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("namespace".into())
                        })),
                        second: Boxed::new(ValueExpression::Placeholder(1)),
                        operator: BinaryOperator::Equal
                    }
                ).into())].into()),
                Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: None,
                            column: Identifier("key".into())
                        })),
                        second: Boxed::new(ValueExpression::Placeholder(2)),
                        operator: BinaryOperator::Like
                    }
                ).into())].into())
            ].into())),
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: None,
            combine: None
        },
        query.to_static()
    );
}

#[test]
fn select_other() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT
            (SELECT COUNT(*) FROM \"user\" WHERE is_service_account = true) AS serviceaccounts,
            (SELECT COUNT(*) FROM \"api_key\"WHERE service_account_id IS NOT NULL ) AS serviceaccount_tokens,
            (SELECT COUNT(*) FROM \"org_user\" AS ou JOIN \"user\" AS u ON u.id = ou.user_id WHERE u.is_disabled = false AND u.is_service_account = true AND ou.role=$1) AS serviceaccounts_with_no_role";

    let select = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };
    dbg!(&select);
}

#[test]
fn update_something() {
    let arena = bumpalo::Bump::new();
    let query_str = "UPDATE \"alert_configuration_history\" SET \"last_applied\" = $1 WHERE (org_id = $2 AND configuration_hash = $3) AND (CTID IN (SELECT CTID FROM \"alert_configuration_history\" WHERE (org_id = $4 AND configuration_hash = $5) ORDER BY \"id\" DESC LIMIT 1))";

    let update = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Update(u)) => u,
        other => panic!("{:?}", other),
    };

    dbg!(&update);

    // todo!()
}

#[test]
fn testing() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT permission.action,permission.scope
FROM permission
INNER JOIN role ON role.id = permission.role_id
INNER JOIN (
    SELECT br.role_id
    FROM builtin_role AS br
    WHERE br.role IN ($1) AND (br.org_id = $2 OR br.org_id = $3)
) as all_role ON role.id = all_role.role_id";

    let select = match Query::parse(query_str.as_bytes(),&arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Select {
            values: vec![
                ValueExpression::ColumnReference(ColumnReference {
                    relation: Some("permission".into()),
                    column: "action".into(),
                }),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: Some("permission".into()),
                    column: "scope".into(),
                })
            ].into(),
            table: Some(TableExpression::Join {
                left: Boxed::new(TableExpression::Join {
                    left: Boxed::new(TableExpression::Relation("permission".into())),
                    right: Boxed::new(TableExpression::Relation("role".into())),
                    kind: JoinKind::Inner,
                    condition: Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("role".into()),
                                column: "id".into(),
                            })),
                            second: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("permission".into()),
                                column: "role_id".into(),
                            })),
                            operator: BinaryOperator::Equal
                        }
                    ).into())].into()), 
                    lateral: false,
                }),
                right: Boxed::new(TableExpression::Renamed {
                    inner: Boxed::new(TableExpression::SubQuery(Boxed::new(Select {
                        values: vec![ValueExpression::ColumnReference(ColumnReference {
                            relation: Some("br".into()),
                            column: "role_id".into(),
                        })].into(),
                        table: Some(TableExpression::Renamed {
                            inner: Boxed::new(TableExpression::Relation("builtin_role".into())),
                            name: "br".into(),
                            column_rename: None,
                        }),
                        where_condition: Some(Condition::And(vec![
                            Condition::Value(Box::new(ValueExpression::Operator {
                                first: Boxed::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: Some("br".into()),
                                        column: "role".into(),
                                    }
                                )),
                                second: Boxed::new(ValueExpression::List(vec![
                                    ValueExpression::Placeholder(1)
                                ].into())),
                                operator: BinaryOperator::In
                            }).into()),
                            Condition::Or(vec![
                                Condition::And(vec![Condition::Value(Box::new(
                                    ValueExpression::Operator {
                                        first: Boxed::new(ValueExpression::ColumnReference(
                                            ColumnReference {
                                                relation: Some("br".into()),
                                                column: "org_id".into()
                                            }
                                        )),
                                        second: Boxed::new(ValueExpression::Placeholder(2)),
                                        operator: BinaryOperator::Equal,
                                    }
                                ).into())].into()),
                                Condition::And(vec![Condition::Value(Box::new(
                                    ValueExpression::Operator {
                                        first: Boxed::new(ValueExpression::ColumnReference(
                                            ColumnReference {
                                                relation: Some("br".into()),
                                                column: "org_id".into()
                                            }
                                        )),
                                        second: Boxed::new(ValueExpression::Placeholder(3)),
                                        operator: BinaryOperator::Equal,
                                    }
                                ).into())].into())
                            ].into())
                        ].into())),
                        having: None,
                        order_by: None,
                        group_by: None,
                        limit: None,
                        for_update: None,
                        combine: None,
                    }))),
                    name: "all_role".into(),
                    column_rename: None,
                }),
                kind: JoinKind::Inner,
                condition: Condition::And(vec![Condition::Value(Box::new(
                    ValueExpression::Operator {
                        first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some("role".into()),
                            column: "id".into(),
                        })),
                        second: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                            relation: Some("all_role".into()),
                            column: "role_id".into(),
                        })),
                        operator: BinaryOperator::Equal
                    }
                ).into())].into()),
                lateral: false
            }),
            where_condition: None,
            having: None,
            order_by: None,
            group_by: None,
            limit: None,
            for_update: None,
            combine: None,
        },
        select.to_static()
    );
}

#[test]
fn testing_subquery() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($1)\n\t\t\tAND (br.org_id = $2 OR br.org_id = $3)";

    let select = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&select);

    assert_eq!(
        Select {
            values: vec![ValueExpression::ColumnReference(ColumnReference {
                relation: Some(Identifier("br".into())),
                column: "role_id".into()
            })].into(),
            table: Some(TableExpression::Renamed {
                inner: Boxed::new(TableExpression::Relation("builtin_role".into())),
                name: "br".into(),
                column_rename: None,
            }),
            where_condition: Some(Condition::And(vec![
                Condition::Value(Box::new(ValueExpression::Operator {
                    first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: Some("br".into()),
                        column: "role".into()
                    })),
                    second: Boxed::new(ValueExpression::List(vec![ValueExpression::Placeholder(1)].into())),
                    operator: BinaryOperator::In,
                }).into()),
                Condition::Or(vec![
                    Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("br".into()),
                                column: "org_id".into()
                            })),
                            second: Boxed::new(ValueExpression::Placeholder(2)),
                            operator: BinaryOperator::Equal,
                        }
                    ).into())].into()),
                    Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some("br".into()),
                                column: "org_id".into()
                            })),
                            second: Boxed::new(ValueExpression::Placeholder(3)),
                            operator: BinaryOperator::Equal,
                        }
                    ).into())].into())
                ].into())
            ].into())),
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: None,
            combine: None,
        },
        select.to_static()
    );
}

#[test]
fn with_select() {
    let arena = bumpalo::Bump::new();
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
    let query_res = Query::parse(query_str.as_bytes(), &arena).unwrap();

    dbg!(&query_res);
}

#[test]
fn select_aggregate_with_operator() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT SUM(total_sales)/10 FROM regional_sales";

    let select = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Select {
            values: vec![ValueExpression::Operator {
                first: Boxed::new(ValueExpression::AggregateExpression(
                    AggregateExpression::Sum(Boxed::new(ValueExpression::ColumnReference(
                        ColumnReference {
                            relation: None,
                            column: "total_sales".into()
                        }
                    )))
                )),
                second: Boxed::new(ValueExpression::Literal(Literal::SmallInteger(10))),
                operator: BinaryOperator::Divide,
            }].into(),
            table: Some(TableExpression::Relation("regional_sales".into())),
            where_condition: None,
            order_by: None,
            group_by: None,
            having: None,
            limit: None,
            for_update: None,
            combine: None
        },
        select.to_static()
    );
}

#[test]
fn select_group_by_number() {
    let arena = bumpalo::Bump::new();
    let query_str = "SELECT MAX(balance), plz FROM users GROUP BY 2";

    let select = match Query::parse(query_str.as_bytes(), &arena) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        Select {
            values: vec![
                ValueExpression::AggregateExpression(AggregateExpression::Max(Boxed::new(
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "balance".into()
                    })
                ))),
                ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: "plz".into()
                })
            ].into(),
            table: Some(TableExpression::Relation("users".into())),
            where_condition: None,
            order_by: None,
            group_by: Some(vec![GroupAttribute::ColumnIndex(2)].into()),
            having: None,
            limit: None,
            for_update: None,
            combine: None
        },
        select.to_static()
    );
}
