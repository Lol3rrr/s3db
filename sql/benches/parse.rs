use criterion::{criterion_main, criterion_group, black_box, Criterion};

pub fn simple_select(c: &mut Criterion) {
    let input = "SELECT name FROM users";
    
    let mut group = c.benchmark_group("sql");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("simple_select", |b| b.iter(|| black_box(sql::Query::parse(input.as_bytes()))));
    group.finish();
}

criterion_group!(benches, simple_select);
criterion_main!(benches);
