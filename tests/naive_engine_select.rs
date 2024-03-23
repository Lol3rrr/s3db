use pretty_assertions::assert_eq;

use s3db::{
    execution::{naive::NaiveEngine, Context, Execute, ExecuteResult},
    sql::{DataType, Query},
    storage::{
        self, inmemory::InMemoryStorage, Data, EntireRelation, PartialRelation, Row, Storage,
    },
};

#[tokio::test]
async fn basic_select() {
    let engine = NaiveEngine::new(InMemoryStorage::new());

    let query = Query::parse("SELECT tablename FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    let content = match res {
        ExecuteResult::Select { content } => content,
        other => panic!("{:?}", other),
    };

    assert_eq!(0, content.parts.iter().flat_map(|p| p.rows.iter()).count());

    let query = Query::parse("CREATE TABLE testing (\"id\" TEXT)".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();
    assert!(matches!(res, ExecuteResult::Create), "{:?}", res);

    let query = Query::parse("SELECT tablename FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    let content = match res {
        ExecuteResult::Select { content } => content,
        other => panic!("{:?}", other),
    };

    assert_eq!(1, content.parts.iter().flat_map(|p| p.rows.iter()).count());
}

#[tokio::test]
async fn select_with_literal_return() {
    let engine = NaiveEngine::new(InMemoryStorage::new());

    let query = Query::parse("SELECT tablename, 1 FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    let content = match res {
        ExecuteResult::Select { content } => content,
        other => panic!("{:?}", other),
    };

    assert_eq!(
        &[
            ("tablename".to_string(), DataType::Name, Vec::new()),
            ("".to_string(), DataType::SmallInteger, Vec::new())
        ],
        content.columns.as_slice()
    );
    assert_eq!(0, content.parts.iter().flat_map(|p| p.rows.iter()).count());

    let query = Query::parse("CREATE TABLE testing (\"id\" TEXT)".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();
    assert!(matches!(res, ExecuteResult::Create), "{:?}", res);

    let query = Query::parse("SELECT tablename, 1 FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();
    dbg!(&res);

    let content = match res {
        ExecuteResult::Select { content } => content,
        other => panic!("{:?}", other),
    };

    let rows: Vec<_> = content.parts.iter().flat_map(|p| p.rows.iter()).collect();
    assert_eq!(1, rows.len());

    assert_eq!(
        &[Data::Name("testing".to_string()), Data::SmallInt(1)],
        rows[0].data.as_slice()
    );

    dbg!(&rows);
}

#[tokio::test]
async fn execute_inner_join() {
    let query = "SELECT user.name, password.hash FROM user JOIN password ON user.id = password.uid";

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "user",
                vec![
                    ("id".into(), DataType::Integer, Vec::new()),
                    ("name".into(), DataType::Text, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .create_relation(
                "password",
                vec![
                    ("uid".into(), DataType::Integer, Vec::new()),
                    ("hash".into(), DataType::Text, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "user",
                &mut vec![
                    vec![Data::Integer(0), Data::Text("first-user".to_string())],
                    vec![Data::Integer(1), Data::Text("second-user".to_string())],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "password",
                &mut vec![vec![Data::Integer(0), Data::Text("12345".to_string())]].into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![
                    ("name".to_string(), DataType::Text, Vec::new()),
                    ("hash".to_string(), DataType::Text, Vec::new())
                ],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(
                        0,
                        vec![
                            Data::Text("first-user".to_string()),
                            Data::Text("12345".to_string())
                        ]
                    )]
                }]
            }
        },
        res
    );
}

#[tokio::test]
#[ignore = "Left Outer Join"]
async fn left_outer_join() {
    let query =
            "SELECT user.name, password.hash FROM user LEFT OUTER JOIN password ON user.id = password.uid";

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "user",
                vec![
                    ("id".into(), DataType::Integer, Vec::new()),
                    ("name".into(), DataType::Text, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .create_relation(
                "password",
                vec![
                    ("uid".into(), DataType::Integer, Vec::new()),
                    ("hash".into(), DataType::Text, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "user",
                &mut vec![
                    vec![Data::Integer(0), Data::Text("first-user".to_string())],
                    vec![Data::Integer(1), Data::Text("second-user".to_string())],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "password",
                &mut vec![vec![Data::Integer(0), Data::Text("12345".to_string())]].into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![
                    ("name".to_string(), DataType::Text, Vec::new()),
                    ("hash".to_string(), DataType::Text, Vec::new())
                ],
                parts: vec![PartialRelation {
                    rows: vec![
                        Row::new(
                            0,
                            vec![
                                Data::Text("first-user".to_string()),
                                Data::Text("12345".to_string())
                            ]
                        ),
                        Row::new(0, vec![Data::Text("second-user".to_string()), Data::Null])
                    ]
                }]
            }
        },
        res
    );
}

#[tokio::test]
async fn group_by_single_attribute() {
    let query_str = "SELECT role FROM user GROUP BY role";

    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "user",
                vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("role".into(), DataType::Text, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "user",
                &mut vec![
                    vec![
                        Data::Text("first-user".to_string()),
                        Data::Text("admin".to_string()),
                    ],
                    vec![
                        Data::Text("second-user".to_string()),
                        Data::Text("viewer".to_string()),
                    ],
                    vec![
                        Data::Text("third-user".to_string()),
                        Data::Text("editor".to_string()),
                    ],
                    vec![
                        Data::Text("forth-user".to_string()),
                        Data::Text("viewer".to_string()),
                    ],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    dbg!(&res);

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("role".to_string(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![
                        Row::new(0, vec![Data::Text("admin".to_string())]),
                        Row::new(0, vec![Data::Text("viewer".to_string())]),
                        Row::new(0, vec![Data::Text("editor".to_string())])
                    ]
                }]
            }
        },
        res
    );
}

#[tokio::test]
async fn select_count_all() {
    let query = "SELECT COUNT(*) FROM table";

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation("table", vec![("id".into(), DataType::Integer, Vec::new())])
            .await
            .unwrap();

        storage
            .insert_rows(
                "table",
                &mut vec![vec![Data::Integer(132)], vec![Data::Integer(1)]].into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("".to_string(), DataType::BigInteger, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::BigInt(2)])]
                }],
            }
        },
        res
    );
}

#[tokio::test]
async fn select_count_attribute() {
    let query = "SELECT COUNT(id) FROM table";

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation("table", vec![("id".into(), DataType::Integer, Vec::new())])
            .await
            .unwrap();

        storage
            .insert_rows(
                "table",
                &mut vec![
                    vec![Data::Integer(132)],
                    vec![Data::Integer(1)],
                    vec![Data::Null],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("".to_string(), DataType::BigInteger, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::BigInt(2)])]
                }],
            }
        },
        res
    );
}

#[tokio::test]
async fn select_with_limit() {
    let query = "SELECT name FROM users LIMIT 1";

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation("users", vec![("name".into(), DataType::Text, Vec::new())])
            .await
            .unwrap();

        storage
            .insert_rows(
                "users",
                &mut vec![
                    vec![Data::Text("first".to_string())],
                    vec![Data::Text("second".to_string())],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".to_string(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("first".to_string())])]
                }],
            }
        },
        res
    );
}

#[tokio::test]
async fn select_with_order() {
    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "users",
                vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("id".into(), DataType::Integer, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "users",
                &mut vec![
                    vec![Data::Text("first".to_string()), Data::Integer(132)],
                    vec![Data::Text("second".to_string()), Data::Integer(12)],
                    vec![Data::Text("third".to_string()), Data::Integer(57)],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = "SELECT name FROM users ORDER BY id ASC";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".to_string(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![
                        Row::new(0, vec![Data::Text("second".to_string())]),
                        Row::new(0, vec![Data::Text("third".to_string())]),
                        Row::new(0, vec![Data::Text("first".to_string())])
                    ]
                }],
            }
        },
        res
    );

    let query = "SELECT name FROM users ORDER BY id DESC";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".to_string(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![
                        Row::new(0, vec![Data::Text("first".to_string())]),
                        Row::new(0, vec![Data::Text("third".to_string())]),
                        Row::new(0, vec![Data::Text("second".to_string())])
                    ]
                }],
            }
        },
        res
    );

    let query = "SELECT name FROM users ORDER BY id";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".to_string(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![
                        Row::new(0, vec![Data::Text("second".to_string())]),
                        Row::new(0, vec![Data::Text("third".to_string())]),
                        Row::new(0, vec![Data::Text("first".to_string())])
                    ]
                }],
            }
        },
        res
    );
}

#[tokio::test]
async fn update_basic() {
    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "orders",
                vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("id".into(), DataType::Integer, Vec::new()),
                    ("completed".into(), DataType::Bool, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .create_relation(
                "deliveries",
                vec![
                    ("order_id".into(), DataType::Integer, Vec::new()),
                    ("delivered".into(), DataType::Bool, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "orders",
                &mut vec![
                    vec![
                        Data::Text("first".to_string()),
                        Data::Integer(132),
                        Data::Boolean(false),
                    ],
                    vec![
                        Data::Text("second".to_string()),
                        Data::Integer(12),
                        Data::Boolean(false),
                    ],
                    vec![
                        Data::Text("third".to_string()),
                        Data::Integer(57),
                        Data::Boolean(false),
                    ],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "deliveries",
                &mut vec![
                    vec![Data::Integer(132), Data::Boolean(false)],
                    vec![Data::Integer(12), Data::Boolean(true)],
                    vec![Data::Integer(57), Data::Boolean(false)],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(&storage);

    let query = "UPDATE orders SET completed = true WHERE orders.id = 12";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(ExecuteResult::Update { updated_rows: 1 }, res);

    let relation_after = storage.get_entire_relation("orders").await.unwrap();
    dbg!(&relation_after);

    let after_rows: Vec<_> = relation_after
        .parts
        .into_iter()
        .flat_map(|p| p.rows.into_iter())
        .collect();

    assert_eq!(
        vec![
            Row::new(
                0,
                vec![
                    Data::Text("first".into()),
                    Data::Integer(132),
                    Data::Boolean(false)
                ]
            ),
            Row::new(
                1,
                vec![
                    Data::Text("second".to_string()),
                    Data::Integer(12),
                    Data::Boolean(true),
                ]
            ),
            Row::new(
                2,
                vec![
                    Data::Text("third".to_string()),
                    Data::Integer(57),
                    Data::Boolean(false),
                ]
            ),
        ],
        after_rows
    );
}

#[tokio::test]
#[ignore = "TODO"]
async fn update_with_from() {
    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "orders",
                vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("id".into(), DataType::Integer, Vec::new()),
                    ("completed".into(), DataType::Bool, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .create_relation(
                "deliveries",
                vec![
                    ("order_id".into(), DataType::Integer, Vec::new()),
                    ("delivered".into(), DataType::Bool, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "orders",
                &mut vec![
                    vec![
                        Data::Text("first".to_string()),
                        Data::Integer(132),
                        Data::Boolean(false),
                    ],
                    vec![
                        Data::Text("second".to_string()),
                        Data::Integer(12),
                        Data::Boolean(false),
                    ],
                    vec![
                        Data::Text("third".to_string()),
                        Data::Integer(57),
                        Data::Boolean(false),
                    ],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "deliveries",
                &mut vec![
                    vec![Data::Integer(132), Data::Boolean(false)],
                    vec![Data::Integer(12), Data::Boolean(true)],
                    vec![Data::Integer(57), Data::Boolean(false)],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = "UPDATE orders SET completed = deliveries.delivered FROM deliveries WHERE orders.id = deliveries.order_id";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(ExecuteResult::Update { updated_rows: 1 }, res);
}

#[tokio::test]
async fn delete_with_placeholders() {
    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "users",
                vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("id".into(), DataType::Integer, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "users",
                &mut vec![
                    vec![Data::Text("first".to_string()), Data::Integer(132)],
                    vec![Data::Text("second".to_string()), Data::Integer(12)],
                    vec![Data::Text("third".to_string()), Data::Integer(57)],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query_str = "DELETE FROM server_lock WHERE operation_uid=$1";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let prepared = engine.prepare(&query, &mut Context::new()).await.unwrap();

    // TODO
}

#[tokio::test]
async fn select_with_standard_cte() {
    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "users",
                vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("id".into(), DataType::Integer, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
    };

    let engine = NaiveEngine::new(storage);

    let query_str =
        "WITH users_cte AS (SELECT name FROM users WHERE id < 100) SELECT name FROM users_cte";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![storage::PartialRelation { rows: vec![] }],
            }
        },
        res
    );
}

#[tokio::test]
async fn select_with_recursive_cte() {
    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "users",
                vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("id".into(), DataType::Integer, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
    };

    let engine = NaiveEngine::new(storage);

    let query_str =
        "WITH RECURSIVE count (n) AS (SELECT 1 UNION SELECT n + 1 FROM count WHERE n < 5) SELECT n FROM count";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("n".into(), DataType::SmallInteger, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![
                        storage::Row::new(0, vec![Data::SmallInt(1)]),
                        storage::Row::new(0, vec![Data::SmallInt(2)]),
                        storage::Row::new(0, vec![Data::SmallInt(3)]),
                        storage::Row::new(0, vec![Data::SmallInt(4)]),
                        storage::Row::new(0, vec![Data::SmallInt(5)]),
                    ]
                }],
            }
        },
        res
    );
}

#[tokio::test]
async fn select_all() {
    let query = "SELECT * FROM user";

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation(
                "user",
                vec![
                    ("id".into(), DataType::Integer, Vec::new()),
                    ("name".into(), DataType::Text, Vec::new()),
                ],
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "user",
                &mut vec![
                    vec![Data::Integer(0), Data::Text("first-user".to_string())],
                    vec![Data::Integer(1), Data::Text("second-user".to_string())],
                ]
                .into_iter(),
            )
            .await
            .unwrap();

        storage
    };
    let engine = NaiveEngine::new(storage);

    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![
                    ("id".to_string(), DataType::Integer, Vec::new()),
                    ("name".to_string(), DataType::Text, Vec::new()),
                ],
                parts: vec![PartialRelation {
                    rows: vec![
                        Row::new(
                            0,
                            vec![Data::Integer(0), Data::Text("first-user".to_string()),]
                        ),
                        Row::new(
                            0,
                            vec![Data::Integer(1), Data::Text("second-user".to_string()),]
                        )
                    ]
                }]
            }
        },
        res
    );
}

#[tokio::test]
async fn select_1() {
    let engine = NaiveEngine::new(InMemoryStorage::new());

    let query_str = "SELECT 1";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("".into(), DataType::SmallInteger, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![storage::Row::new(0, vec![Data::SmallInt(1)])]
                }]
            }
        },
        res
    );
}

#[tokio::test]
async fn setval() {
    let query_str = "SELECT setval('org_id_seq', (SELECT max(id) FROM org))";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = {
        let storage = InMemoryStorage::new();

        storage
            .create_relation("org", vec![("id".into(), DataType::Serial, Vec::new())])
            .await
            .unwrap();

        storage
            .insert_rows(
                "org",
                &mut [vec![Data::Serial(1)], vec![Data::Serial(2)]].into_iter(),
            )
            .await
            .unwrap();

        storage
    };

    let engine = NaiveEngine::new(storage);
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("".into(), DataType::BigInteger, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![storage::Row::new(0, vec![Data::Serial(2)])]
                }]
            }
        },
        res
    );
}
