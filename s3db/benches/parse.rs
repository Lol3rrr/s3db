use s3db::{ra::RaExpression, storage::Schemas};
use sql::DataType;
use criterion::{criterion_group, criterion_main, black_box, Criterion};

fn simple_select(c: &mut Criterion) {
    let query = "SELECT name FROM users";

    let schemas: Schemas = [("users".to_string(), vec![("name".into(), DataType::Text)])]
        .into_iter()
        .collect();

    let query = match sql::Query::parse(query.as_bytes()).unwrap() {
        sql::Query::Select(s) => s,
        other => unreachable!("{:?}", other),
    };

    c.bench_function("simple_select", |b| b.iter(|| black_box(RaExpression::parse_select(&query, &schemas))));
}

fn select_join(c: &mut Criterion) {
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

    c.bench_function("select_join", |b| b.iter(|| black_box(RaExpression::parse_select(&query, &schemas))));
}

criterion_group!(benches, simple_select);
criterion_main!(benches);
