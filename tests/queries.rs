use s3db::{
    execution::{Execute, ExecuteResult},
    sql::DataType,
    storage::{Data, EntireRelation, PartialRelation, Row},
};

use tracing::error_span;

macro_rules! execute {
    ($select:literal, $expected:expr) => {
        execute!($select, $expected,)
    };
    ($select:literal, $expected:expr, $($queries:literal),*) => {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let subscriber = tracing_subscriber::FmtSubscriber::builder().with_writer(std::io::stdout).with_max_level(tracing::Level::DEBUG).with_ansi(false).finish();

        let tmp = tracing::subscriber::with_default(subscriber, || {
            let _span_guard = error_span!("running-test").entered();

            runtime.block_on(async move {
                let engine = s3db::execution::naive::NaiveEngine::new(
                    s3db::storage::inmemory::InMemoryStorage::new(),
                );

                $(
                    {
                        let query = s3db::sql::Query::parse($queries.as_bytes()).unwrap();
                        engine.execute(&query, &mut s3db::execution::Context::new()).await.unwrap();
                    }
                )*

                let query = s3db::sql::Query::parse($select.as_bytes()).unwrap();
                engine
                    .execute(&query, &mut s3db::execution::Context::new())
                    .await
                    .unwrap()
            })
        });

        assert_eq!($expected, tmp);
    };
}

#[test]
fn select_constant() {
    execute!(
        "SELECT 1",
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("".into(), DataType::SmallInteger, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::SmallInt(1)])]
                }]
            },
            formats: Vec::new()
        }
    );
}

#[test]
fn create_table_insert() {
    execute!(
        "INSERT INTO users(name, age) VALUES('testing', 123)",
        ExecuteResult::Insert {
            inserted_rows: 1,
            returning: Vec::new(),
            formats: Vec::new()
        },
        "CREATE TABLE users (name text, age integer)"
    );
}

#[test]
fn create_table_insert_select() {
    execute!(
        "SELECT * FROM users",
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![
                    ("name".into(), DataType::Text, Vec::new()),
                    ("age".into(), DataType::Integer, Vec::new())
                ],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(
                        0,
                        vec![Data::Text("testing".into()), Data::Integer(123)]
                    )],
                }]
            },
            formats: Vec::new()
        },
        "CREATE TABLE users (name text, age integer)",
        "INSERT INTO users(name, age) VALUES('testing', 123)"
    );
}

#[test]
fn single_join_with_same_name_attribute() {
    execute!(
        "SELECT users.name as username, roles.name as rolename FROM users INNER JOIN roles ON users.role = roles.id",
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("username".into(), DataType::Text, Vec::new()), ("rolename".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(
                        0,
                        vec![Data::Text("username".into()), Data::Text("viewer".into())]
                    )]
                }],
            },
            formats: Vec::new()
        },
        "CREATE TABLE users (id integer, name text, age integer, role integer)",
        "CREATE TABLE roles (id integer, name text)",
        "INSERT INTO users(id, name, age, role) VALUES(1, 'username', 25, 23)",
        "INSERT INTO users(id, name, age, role) VALUES(2, 'otheruser', 16, 99)",
        "INSERT INTO roles(id, name) VALUES(11, 'admin')",
        "INSERT INTO roles(id, name) VALUES(23, 'viewer')"
    );
}

#[test]
fn select_with_correlated_subquery() {
    execute!(
        "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM roles WHERE roles.id = users.role)",
        ExecuteResult::Select {
            content: EntireRelation {
                columns: vec![("name".into(), DataType::Text, Vec::new())],
                parts: vec![PartialRelation {
                    rows: vec![Row::new(0, vec![Data::Text("otheruser".into())])]
                }],
            },
            formats: vec![]
        },
        "CREATE TABLE users (id integer, name text, age integer, role integer)",
        "CREATE TABLE roles (id integer, name text)",
        "INSERT INTO users(id, name, age, role) VALUES(1, 'username', 25, 23)",
        "INSERT INTO users(id, name, age, role) VALUES(2, 'otheruser', 16, 99)",
        "INSERT INTO roles(id, name) VALUES(11, 'admin')",
        "INSERT INTO roles(id, name) VALUES(23, 'viewer')"
    );
}

#[test]
#[ignore = "Delete queries are kind of broken at the moment"]
fn delete_with_correlated_subquery() {
    execute!(
        "DELETE FROM users WHERE NOT EXISTS (SELECT 1 FROM roles WHERE roles.id = users.role)",
        ExecuteResult::Delete { deleted_rows: 1 },
        "CREATE TABLE users (id integer, name text, age integer, role integer)",
        "CREATE TABLE roles (id integer, name text)",
        "INSERT INTO users(id, name, age, role) VALUES(1, 'username', 25, 23)",
        "INSERT INTO users(id, name, age, role) VALUES(2, 'otheruser', 16, 99)",
        "INSERT INTO roles(id, name) VALUES(11, 'admin')",
        "INSERT INTO roles(id, name) VALUES(23, 'viewer')"
    );
}
