use criterion::{criterion_main, criterion_group, black_box, Criterion, BenchmarkId};

use s3db::{storage::Storage, execution::Execute};
use sql::CompatibleParser;

macro_rules! benchmark_query {
    ($query:expr, $columns:expr, $group:expr, $benchname:literal) => {{
        let query = sql::Query::parse($query.as_bytes(), &bumpalo::Bump::new()).unwrap().to_static();

        let storage = s3db::storage::inmemory::InMemoryStorage::new();

        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let transaction = runtime.block_on(async {
            let transcation = storage.start_transaction().await.unwrap();
            storage.create_relation("users", vec![("id".into(), sql::DataType::Integer, Vec::new()), ("name".into(), sql::DataType::Text, Vec::new())], &transcation).await.unwrap();

            let mut row_iter = (0..$columns).map(|i| vec![s3db::storage::Data::Integer(i), s3db::storage::Data::Text(format!("name-{}", i))]);
            storage.insert_rows("users", &mut row_iter, &transcation).await.unwrap();
            storage.commit_transaction(transcation).await.unwrap();
            storage.start_transaction().await.unwrap()
        });

        let engine = s3db::execution::naive::NaiveEngine::new(storage);

        $group.bench_function(BenchmarkId::new($benchname, $columns), |b| {
            b.to_async(tokio::runtime::Builder::new_current_thread().build().unwrap()).iter_batched(|| (query.to_static(), bumpalo::Bump::with_capacity(16*1024), &engine, transaction.clone()), |(query, arena, engine, tx)| {
                async move {
                    let mut ctx = s3db::execution::Context::new();
                    ctx.transaction = Some(tx);
                    let result = engine.execute(&query, &mut ctx, &arena).await;
                    black_box(result)
                }
            }, criterion::BatchSize::LargeInput)
        });
    }}
}

fn select(c: &mut Criterion) {
    let mut group = c.benchmark_group("execution/naive");

    for size in [10, 100, 1000] {
        benchmark_query!("SELECT name FROM users", size, group, "select_1_column");
    }
}

fn with_conditon(c: &mut Criterion) {
    let mut group = c.benchmark_group("execution/naive");

    for size in [10, 100, 1000] {
        benchmark_query!(format!("SELECT name FROM users WHERE id < {}", size / 2), size, group, "condition_less_than");
    }
}


#[cfg(flamegraph)]
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(pprof::criterion::PProfProfiler::new(100, pprof::criterion::Output::Flamegraph(None)));
    targets = select,with_conditon
);
#[cfg(not(flamegraph))]
criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = select,with_conditon
);

criterion_main!(benches);
