use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn simple_select(c: &mut Criterion) {
    let input = "SELECT name FROM users";

    let mut group = c.benchmark_group("sql");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("simple_select", |b| {
        let mut arena = bumpalo::Bump::new();
        b.iter(|| {
            black_box(sql::Query::parse(input.as_bytes(), &arena));
            arena.reset();
        })
    });
    group.finish();
}

criterion_group!(benches, simple_select);
criterion_main!(benches);
