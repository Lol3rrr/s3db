use criterion::{criterion_main, criterion_group, black_box, Criterion};

pub fn simple_select(c: &mut Criterion) {
    let input = "SELECT name FROM users";
    c.bench_function("simple_select", |b| b.iter(|| black_box(sql::Query::parse(input.as_bytes()))));
}

criterion_group!(benches, simple_select);
criterion_main!(benches);
