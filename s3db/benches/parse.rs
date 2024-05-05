use criterion::{black_box, criterion_group, criterion_main, Criterion};
use s3db::ra::RaExpression;

use sql::DataType;
use storage::Schemas;

fn simple_select(c: &mut Criterion) {
    let query = "SELECT name FROM users";

    let schemas: Schemas = [("users".to_string(), vec![("name".into(), DataType::Text)])]
        .into_iter()
        .collect();

    let arena = bumpalo::Bump::new();
    let query = match sql::Query::parse(query.as_bytes(), &arena).unwrap() {
        sql::Query::Select(s) => s,
        other => unreachable!("{:?}", other),
    };

    let mut group = c.benchmark_group("ra");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("simple_select", |b| {
        b.iter(|| black_box(RaExpression::parse_select(&query, &schemas)))
    });
    group.finish();
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

    let arena = bumpalo::Bump::new();
    let query = match sql::Query::parse(query.as_bytes(), &arena).unwrap() {
        sql::Query::Select(s) => s,
        other => unreachable!("{:?}", other),
    };

    let mut group = c.benchmark_group("ra");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("select_join", |b| {
        b.iter(|| black_box(RaExpression::parse_select(&query, &schemas)))
    });
    group.finish();
}

#[cfg(not(flamegraph))]
criterion_group!(benches, simple_select, select_join);
#[cfg(not(flamegraph))]
criterion_main!(benches);
#[cfg(flamegraph)]
fn main(){}
