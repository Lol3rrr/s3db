use pretty_assertions::assert_eq;

use s3db::{
    execution::{naive::NaiveEngine, Context, Execute, ExecuteResult},
    sql::{DataType, Query},
    storage::{
        self, inmemory::InMemoryStorage, Data, EntireRelation, PartialRelation, Row, Storage,
    },
};

macro_rules! storage_setup {
    ($(($table_name:literal, $fields:expr, $rows:expr)),*) => {{
        let storage = InMemoryStorage::new();

        let trans = storage.start_transaction().await.unwrap();

        $(
            storage
            .create_relation(
                $table_name,
                $fields,
                &trans,
            )
            .await
            .unwrap();

            storage
            .insert_rows(
                $table_name,
                &mut $rows.into_iter(),
                &trans,
            )
            .await
            .unwrap();
        )*

        storage.commit_transaction(trans).await.unwrap();

        storage
    }};
}

#[tokio::test]
async fn basic_select() {
    let storage = InMemoryStorage::new();
    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);

    let query = Query::parse("SELECT tablename FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    let content = match res {
        ExecuteResult::Select { content, .. } => content,
        other => panic!("{:?}", other),
    };

    assert_eq!(0, content.parts.iter().flat_map(|p| p.rows.iter()).count());

    let query = Query::parse("CREATE TABLE testing (\"id\" TEXT)".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut ctx).await.unwrap();
    assert!(matches!(res, ExecuteResult::Create), "{:?}", res);

    let query = Query::parse("SELECT tablename FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    let content = match res {
        ExecuteResult::Select { content, .. } => content,
        other => panic!("{:?}", other),
    };

    assert_eq!(1, content.parts.iter().flat_map(|p| p.rows.iter()).count());
}

#[tokio::test]
async fn select_with_literal_return() {
    let storage = InMemoryStorage::new();
    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);

    let query = Query::parse("SELECT tablename, 1 FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    let content = match res {
        ExecuteResult::Select { content, .. } => content,
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
    let res = engine.execute(&query, &mut ctx).await.unwrap();
    assert!(matches!(res, ExecuteResult::Create), "{:?}", res);

    let query = Query::parse("SELECT tablename, 1 FROM pg_tables".as_bytes()).unwrap();
    let res = engine.execute(&query, &mut ctx).await.unwrap();
    dbg!(&res);

    let content = match res {
        ExecuteResult::Select { content, .. } => content,
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

    let storage = storage_setup!(
        (
            "user",
            vec![
                ("id".into(), DataType::Integer, Vec::new()),
                ("name".into(), DataType::Text, Vec::new()),
            ],
            vec![
                vec![Data::Integer(0), Data::Text("first-user".to_string())],
                vec![Data::Integer(1), Data::Text("second-user".to_string())],
            ]
        ),
        (
            "password",
            vec![
                ("uid".into(), DataType::Integer, Vec::new()),
                ("hash".into(), DataType::Text, Vec::new()),
            ],
            vec![vec![Data::Integer(0), Data::Text("12345".to_string())]]
        )
    );

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let query = Query::parse(query.as_bytes()).unwrap();
    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

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
            },
            formats: Vec::new()
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

        let trans = storage.start_transaction().await.unwrap();

        storage
            .create_relation(
                "user",
                vec![
                    ("id".into(), DataType::Integer, Vec::new()),
                    ("name".into(), DataType::Text, Vec::new()),
                ],
                &trans,
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
                &trans,
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
                &trans,
            )
            .await
            .unwrap();

        storage
            .insert_rows(
                "password",
                &mut vec![vec![Data::Integer(0), Data::Text("12345".to_string())]].into_iter(),
                &trans,
            )
            .await
            .unwrap();

        storage.commit_transaction(trans).await;

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
            },
            formats: Vec::new(),
        },
        res
    );
}

#[tokio::test]
async fn group_by_single_attribute() {
    let query_str = "SELECT role FROM user GROUP BY role";

    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!((
        "user",
        vec![
            ("name".into(), DataType::Text, Vec::new()),
            ("role".into(), DataType::Text, Vec::new()),
        ],
        vec![
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
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

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
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_count_all() {
    let query = "SELECT COUNT(*) FROM table";
    let query = Query::parse(query.as_bytes()).unwrap();

    let storage = storage_setup!((
        "table",
        vec![("id".into(), DataType::Integer, Vec::new())],
        vec![vec![Data::Integer(132)], vec![Data::Integer(1)]]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("".to_string(), DataType::BigInteger, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::BigInt(2)])]
                }],
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_count_attribute() {
    let query = "SELECT COUNT(id) FROM table";
    let query = Query::parse(query.as_bytes()).unwrap();

    let storage = storage_setup!((
        "table",
        vec![("id".into(), DataType::Integer, Vec::new())],
        vec![
            vec![Data::Integer(132)],
            vec![Data::Integer(1)],
            vec![Data::Null],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("".to_string(), DataType::BigInteger, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::BigInt(2)])]
                }],
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_with_limit() {
    let query = "SELECT name FROM users LIMIT 1";
    let query = Query::parse(query.as_bytes()).unwrap();

    let storage = storage_setup!((
        "users",
        vec![("name".into(), DataType::Text, Vec::new())],
        vec![
            vec![Data::Text("first".to_string())],
            vec![Data::Text("second".to_string())],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".to_string(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("first".to_string())])]
                }],
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_with_order() {
    let query = "SELECT name FROM users ORDER BY id ASC";
    let query = Query::parse(query.as_bytes()).unwrap();

    let storage = storage_setup!((
        "users",
        vec![
            ("name".into(), DataType::Text, Vec::new()),
            ("id".into(), DataType::Integer, Vec::new()),
        ],
        vec![
            vec![Data::Text("first".to_string()), Data::Integer(132)],
            vec![Data::Text("second".to_string()), Data::Integer(12)],
            vec![Data::Text("third".to_string()), Data::Integer(57)],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

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
            },
            formats: Vec::new()
        },
        res
    );

    let query = "SELECT name FROM users ORDER BY id DESC";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut ctx).await.unwrap();

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
            },
            formats: Vec::new()
        },
        res
    );

    let query = "SELECT name FROM users ORDER BY id";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut ctx).await.unwrap();

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
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn update_basic() {
    let query = "UPDATE orders SET completed = true WHERE orders.id = 12";
    let query = Query::parse(query.as_bytes()).unwrap();

    let storage = storage_setup!(
        (
            "orders",
            vec![
                ("name".into(), DataType::Text, Vec::new()),
                ("id".into(), DataType::Integer, Vec::new()),
                ("completed".into(), DataType::Bool, Vec::new()),
            ],
            vec![
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
        ),
        (
            "deliveries",
            vec![
                ("order_id".into(), DataType::Integer, Vec::new()),
                ("delivered".into(), DataType::Bool, Vec::new()),
            ],
            vec![
                vec![Data::Integer(132), Data::Boolean(false)],
                vec![Data::Integer(12), Data::Boolean(true)],
                vec![Data::Integer(57), Data::Boolean(false)],
            ]
        )
    );

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(&storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(ExecuteResult::Update { updated_rows: 1 }, res);

    let relation_after = storage
        .get_entire_relation("orders", ctx.transaction.as_ref().unwrap())
        .await
        .unwrap();
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
                2,
                vec![
                    Data::Text("third".to_string()),
                    Data::Integer(57),
                    Data::Boolean(false),
                ]
            ),
            Row::new(
                3,
                vec![
                    Data::Text("second".to_string()),
                    Data::Integer(12),
                    Data::Boolean(true),
                ]
            ),
        ],
        after_rows
    );
}

#[tokio::test]
#[ignore = "TODO"]
async fn update_with_from() {
    let storage = storage_setup!(
        (
            "orders",
            vec![
                ("name".into(), DataType::Text, Vec::new()),
                ("id".into(), DataType::Integer, Vec::new()),
                ("completed".into(), DataType::Bool, Vec::new()),
            ],
            vec![
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
        ),
        (
            "deliveries",
            vec![
                ("order_id".into(), DataType::Integer, Vec::new()),
                ("delivered".into(), DataType::Bool, Vec::new()),
            ],
            vec![
                vec![Data::Integer(132), Data::Boolean(false)],
                vec![Data::Integer(12), Data::Boolean(true)],
                vec![Data::Integer(57), Data::Boolean(false)],
            ]
        )
    );
    let engine = NaiveEngine::new(storage);

    let query = "UPDATE orders SET completed = deliveries.delivered FROM deliveries WHERE orders.id = deliveries.order_id";
    let query = Query::parse(query.as_bytes()).unwrap();
    let res = engine.execute(&query, &mut Context::new()).await.unwrap();

    assert_eq!(ExecuteResult::Update { updated_rows: 1 }, res);
}

#[tokio::test]
async fn delete_with_placeholders() {
    let query_str = "DELETE FROM server_lock WHERE operation_uid=$1";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!((
        "users",
        vec![
            ("name".into(), DataType::Text, Vec::new()),
            ("id".into(), DataType::Integer, Vec::new()),
        ],
        vec![
            vec![Data::Text("first".to_string()), Data::Integer(132)],
            vec![Data::Text("second".to_string()), Data::Integer(12)],
            vec![Data::Text("third".to_string()), Data::Integer(57)],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let prepared = engine.prepare(&query, &mut ctx).await.unwrap();

    // TODO
}

#[tokio::test]
async fn select_with_standard_cte() {
    let query_str =
        "WITH users_cte AS (SELECT name FROM users WHERE id < 100) SELECT name FROM users_cte";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!((
        "users",
        vec![
            ("name".into(), DataType::Text, Vec::new()),
            ("id".into(), DataType::Integer, Vec::new()),
        ],
        vec![]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![storage::PartialRelation { rows: vec![] }],
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_with_recursive_cte() {
    let query_str =
        "WITH RECURSIVE count (n) AS (SELECT 1 UNION SELECT n + 1 FROM count WHERE n < 5) SELECT n FROM count";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!();

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

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
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_all() {
    let query = "SELECT * FROM user";
    let query = Query::parse(query.as_bytes()).unwrap();

    let storage = storage_setup!((
        "user",
        vec![
            ("id".into(), DataType::Integer, Vec::new()),
            ("name".into(), DataType::Text, Vec::new()),
        ],
        vec![
            vec![Data::Integer(0), Data::Text("first-user".to_string())],
            vec![Data::Integer(1), Data::Text("second-user".to_string())],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

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
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_1() {
    let query_str = "SELECT 1";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = InMemoryStorage::new();
    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("".into(), DataType::SmallInteger, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![storage::Row::new(0, vec![Data::SmallInt(1)])]
                }]
            },
            formats: Vec::new(),
        },
        res
    );
}

#[tokio::test]
async fn setval() {
    let query_str = "SELECT setval('org_id_seq', (SELECT max(id) FROM org))";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!((
        "org",
        vec![("id".into(), DataType::Serial, Vec::new())],
        vec![vec![Data::Serial(1)], vec![Data::Serial(2)]]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("".into(), DataType::BigInteger, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![storage::Row::new(0, vec![Data::Serial(2)])]
                }]
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn like_operation() {
    let query_str = "SELECT name FROM org WHERE name LIKE 'test%'";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!((
        "org",
        vec![
            ("id".into(), DataType::Serial, Vec::new()),
            ("name".into(), DataType::Text, Vec::new())
        ],
        vec![
            vec![Data::Serial(1), Data::Text("testing".into())],
            vec![Data::Serial(2), Data::Text("other".into())],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![storage::Row::new(0, vec![Data::Text("testing".into())])]
                }]
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_subquery_as_value() {
    let query_str = "SELECT (SELECT count(*) FROM users)";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!((
        "users",
        vec![
            ("id".into(), DataType::Serial, Vec::new()),
            ("name".into(), DataType::Text, Vec::new()),
        ],
        vec![
            vec![Data::Serial(1), Data::Text("testing".into())],
            vec![Data::Serial(2), Data::Text("other".into())],
            vec![Data::Serial(3), Data::Text("something".into())],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    dbg!(&res);

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("".into(), DataType::BigInteger, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![storage::Row::new(0, vec![Data::BigInt(3)])]
                }]
            },
            formats: Vec::new()
        },
        res
    );
}

#[tokio::test]
async fn select_with_in_filter() {
    let query_str = "SELECT name from users WHERE name IN ('other', 'something')";
    let query = Query::parse(query_str.as_bytes()).unwrap();

    let storage = storage_setup!((
        "users",
        vec![
            ("id".into(), DataType::Serial, Vec::new()),
            ("name".into(), DataType::Text, Vec::new()),
        ],
        vec![
            vec![Data::Serial(1), Data::Text("testing".into())],
            vec![Data::Serial(2), Data::Text("other".into())],
            vec![Data::Serial(3), Data::Text("something".into())],
        ]
    ));

    let transaction = storage.start_transaction().await.unwrap();
    let engine = NaiveEngine::new(storage);

    let mut ctx = Context::new();
    ctx.transaction = Some(transaction);
    let res = engine.execute(&query, &mut ctx).await.unwrap();

    dbg!(&res);

    assert_eq!(
        ExecuteResult::Select {
            content: storage::EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![storage::PartialRelation {
                    rows: vec![
                        storage::Row::new(0, vec![Data::Text("other".into())]),
                        storage::Row::new(0, vec![Data::Text("something".into())])
                    ]
                }]
            },
            formats: Vec::new()
        },
        res
    );
}
