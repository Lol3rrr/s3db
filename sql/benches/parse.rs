use criterion::{black_box, criterion_group, criterion_main, Criterion};

const ARENA_CAPACITY: usize = 512 * 1024;

pub fn simple_select(c: &mut Criterion) {
    let input = "SELECT name FROM users";

    let mut group = c.benchmark_group("sql");
    group.throughput(criterion::Throughput::Elements(1));

    let mut arena = bumpalo::Bump::with_capacity(ARENA_CAPACITY);
    group.bench_function("simple_select", |b| {
        b.iter(|| {
            black_box(sql::Query::parse(input.as_bytes(), &arena));
            arena.reset();
        })
    });

    for parts in [1usize, 3, 5, 10] {
        let mut columns = "test".to_string();
        for i in 0..(parts.saturating_sub(1)) {
            columns.push_str(",test");
        }
        let input = format!("SELECT {} FROM users", columns);

        let mut arena = bumpalo::Bump::with_capacity(ARENA_CAPACITY);
        group.bench_function(criterion::BenchmarkId::new("select_columns", parts), |b| {
            b.iter(|| {
                black_box(sql::Query::parse(input.as_bytes(), &arena));
                arena.reset();
            })
        });
    }

    group.finish();
}

pub fn select_conditions(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql");
    group.throughput(criterion::Throughput::Elements(1));

    for parts in [1usize, 3, 5, 10] {
        let mut input: String = "SELECT * FROM users WHERE true".into();
        for i in 0..(parts.saturating_sub(1)) {
            input.push_str(" AND true");
        }

        let mut arena = bumpalo::Bump::with_capacity(ARENA_CAPACITY);
        group.bench_function(criterion::BenchmarkId::new("and_conditions", parts), |b| {
            b.iter(|| {
                black_box(sql::Query::parse(input.as_bytes(), &arena));
                arena.reset();
            })
        });
    }

    for parts in [1usize, 3, 5, 10] {
        let mut input: String = "SELECT * FROM users WHERE true".into();
        for i in 0..(parts.saturating_sub(1)) {
            input.push_str(" OR true");
        }

        let mut arena = bumpalo::Bump::with_capacity(ARENA_CAPACITY);
        group.bench_function(criterion::BenchmarkId::new("or_conditions", parts), |b| {
            b.iter(|| {
                black_box(sql::Query::parse(input.as_bytes(), &arena));
                arena.reset();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, simple_select, select_conditions);
criterion_main!(benches);
