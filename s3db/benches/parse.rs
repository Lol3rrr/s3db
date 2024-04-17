use divan::{AllocProfiler, Bencher};
use s3db::{ra::RaExpression, storage::Schemas};
use sql::DataType;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench()]
fn simple_select(bencher: Bencher) {
    let query = "SELECT name FROM users";

    let schemas: Schemas = [("users".to_string(), vec![("name".into(), DataType::Text)])]
        .into_iter()
        .collect();

    let query = match sql::Query::parse(query.as_bytes()).unwrap() {
        sql::Query::Select(s) => s,
        other => unreachable!("{:?}", other),
    };

    bencher.bench(|| {
        let res = RaExpression::parse_select(&query, &schemas).unwrap();
        divan::black_box(res)
    });
}

#[divan::bench()]
fn select_join(bencher: Bencher) {
    let query = "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.uid";

    let schemas: Schemas = [
        (
            "users".to_string(),
            vec![
                ("id".into(), DataType::Integer),
                ("name".into(), DataType::Text),
            ],
        ),
        (
            "orders".to_string(),
            vec![
                ("uid".into(), DataType::Integer),
                ("product".into(), DataType::Text),
            ],
        ),
    ]
    .into_iter()
    .collect();

    let query = match sql::Query::parse(query.as_bytes()).unwrap() {
        sql::Query::Select(s) => s,
        other => unreachable!("{:?}", other),
    };

    bencher.bench(|| {
        let res = RaExpression::parse_select(&query, &schemas).unwrap();
        divan::black_box(res)
    });
}
