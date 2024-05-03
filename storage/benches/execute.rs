use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use storage::{RelationStorage, Storage};

fn insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_insert");

    for size in [500, 1000, 2000, 5000] {
        group.throughput(criterion::Throughput::Elements(size));

        group.bench_function(BenchmarkId::new("v1", size), |b| {
            b.to_async(
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap(),
            )
            .iter_batched(
                || storage::inmemory::v1::InMemoryStorage::new(),
                |engine| async move {
                    let transaction = engine.start_transaction().await.unwrap();

                    engine
                        .create_relation(
                            "test",
                            vec![("index".to_string(), sql::DataType::Integer, Vec::new())],
                            &transaction,
                        )
                        .await
                        .unwrap();

                    let _ = engine
                        .insert_rows(
                            "test",
                            &mut (0..black_box(size))
                                .map(|idx| vec![storage::Data::Integer(idx as i32)]),
                            &transaction,
                        )
                        .await;
                },
                criterion::BatchSize::LargeInput,
            );
        });

        group.bench_function(BenchmarkId::new("v2", size), |b| {
            b.to_async(
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap(),
            )
            .iter_batched(
                || storage::inmemory::v2::InMemoryStorage::new(),
                |engine| async move {
                    let transaction = engine.start_transaction().await.unwrap();

                    engine
                        .create_relation(
                            "test",
                            vec![("index".to_string(), sql::DataType::Integer, Vec::new())],
                            &transaction,
                        )
                        .await
                        .unwrap();

                    let _ = engine
                        .insert_rows(
                            "test",
                            &mut (0..black_box(size))
                                .map(|idx| vec![storage::Data::Integer(idx as i32)]),
                            &transaction,
                        )
                        .await;
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }
}

#[cfg(flamegraph)]
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(pprof::criterion::PProfProfiler::new(500, pprof::criterion::Output::Flamegraph(None)));
    targets = insert,
);
#[cfg(not(flamegraph))]
criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = insert,
);
criterion_main!(benches);
