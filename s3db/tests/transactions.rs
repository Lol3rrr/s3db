use s3db::{
    execution::{naive::NaiveEngine, Context, Execute, ExecuteResult},
    storage::{inmemory::InMemoryStorage, Data, EntireRelation, PartialRelation, Row},
};
use sql::{DataType, Query};

macro_rules! execute {
    ($engine:expr, $ctx:expr, $query:literal) => {{
        let arena = bumpalo::Bump::new();
        let query = Query::parse($query.as_bytes(), &arena).unwrap();
        $engine
            .execute(&query, $ctx, &bumpalo::Bump::new())
            .await
            .unwrap()
    }};
}

#[tokio::test]
async fn basic() {
    let mut first_ctx = Context::new();
    let mut second_ctx = Context::new();

    let engine = NaiveEngine::new(InMemoryStorage::new());

    let mut prev_ctx = Context::new();
    execute!(&engine, &mut prev_ctx, "BEGIN");

    execute!(&engine, &mut prev_ctx, "CREATE TABLE users(name text)");

    let previous_select = execute!(&engine, &mut prev_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation { rows: vec![] }],
            },
            formats: vec![]
        },
        previous_select
    );

    execute!(&engine, &mut prev_ctx, "COMMIT");

    execute!(&engine, &mut first_ctx, "BEGIN");
    execute!(&engine, &mut second_ctx, "BEGIN");

    let insert_res = execute!(
        &engine,
        &mut first_ctx,
        "INSERT INTO users(name) VALUES('first')"
    );
    pretty_assertions::assert_eq!(
        ExecuteResult::Insert {
            inserted_rows: 1,
            returning: vec![],
            formats: vec![]
        },
        insert_res
    );

    let select_res_1 = execute!(&engine, &mut first_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("first".into())])]
                }],
            },
            formats: vec![]
        },
        select_res_1
    );

    let select_res_2 = execute!(&engine, &mut second_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation { rows: vec![] }],
            },
            formats: vec![]
        },
        select_res_2
    );

    execute!(&engine, &mut first_ctx, "COMMIT");

    let select_res_2 = execute!(&engine, &mut second_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation { rows: vec![] }],
            },
            formats: vec![]
        },
        select_res_2
    );

    execute!(&engine, &mut second_ctx, "COMMIT");
}

#[tokio::test]
async fn delete_during() {
    let mut first_ctx = Context::new();
    let mut second_ctx = Context::new();

    let engine = NaiveEngine::new(InMemoryStorage::new());

    let mut prev_ctx = Context::new();
    execute!(&engine, &mut prev_ctx, "BEGIN");

    execute!(&engine, &mut prev_ctx, "CREATE TABLE users(name text)");

    execute!(
        &engine,
        &mut prev_ctx,
        "INSERT INTO users(name) VALUES('previous')"
    );

    let previous_select = execute!(&engine, &mut prev_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("previous".into())])]
                }],
            },
            formats: vec![]
        },
        previous_select
    );

    execute!(&engine, &mut prev_ctx, "COMMIT");

    execute!(&engine, &mut first_ctx, "BEGIN");
    execute!(&engine, &mut second_ctx, "BEGIN");

    let delete_res = execute!(
        &engine,
        &mut first_ctx,
        "DELETE FROM users WHERE name = 'previous'"
    );
    pretty_assertions::assert_eq!(ExecuteResult::Delete { deleted_rows: 1 }, delete_res);

    let select_res_1 = execute!(&engine, &mut first_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation { rows: vec![] }],
            },
            formats: vec![]
        },
        select_res_1
    );

    let select_res_2 = execute!(&engine, &mut second_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("previous".into())])]
                }],
            },
            formats: vec![]
        },
        select_res_2
    );

    execute!(&engine, &mut first_ctx, "COMMIT");

    let select_res_2 = execute!(&engine, &mut second_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("previous".into())])]
                }],
            },
            formats: vec![]
        },
        select_res_2
    );

    execute!(&engine, &mut second_ctx, "COMMIT");
}

#[tokio::test]
async fn aborted() {
    let mut first_ctx = Context::new();
    let mut second_ctx = Context::new();

    let engine = NaiveEngine::new(InMemoryStorage::new());

    let mut prev_ctx = Context::new();
    execute!(&engine, &mut prev_ctx, "BEGIN");

    execute!(&engine, &mut prev_ctx, "CREATE TABLE users(name text)");

    let previous_select = execute!(&engine, &mut prev_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation { rows: vec![] }],
            },
            formats: vec![]
        },
        previous_select
    );

    execute!(&engine, &mut prev_ctx, "COMMIT");

    execute!(&engine, &mut first_ctx, "BEGIN");

    let insert_res = execute!(
        &engine,
        &mut first_ctx,
        "INSERT INTO users(name) VALUES('first')"
    );
    pretty_assertions::assert_eq!(
        ExecuteResult::Insert {
            inserted_rows: 1,
            returning: vec![],
            formats: vec![]
        },
        insert_res
    );

    let select_res_1 = execute!(&engine, &mut first_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("first".into())])]
                }],
            },
            formats: vec![]
        },
        select_res_1
    );

    execute!(&engine, &mut first_ctx, "ROLLBACK");

    execute!(&engine, &mut second_ctx, "BEGIN");

    let select_res_2 = execute!(&engine, &mut second_ctx, "SELECT name FROM users");
    pretty_assertions::assert_eq!(
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation { rows: vec![] }],
            },
            formats: vec![]
        },
        select_res_2
    );

    execute!(&engine, &mut second_ctx, "COMMIT");
}
