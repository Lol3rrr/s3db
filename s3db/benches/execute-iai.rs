use iai_callgrind::{library_benchmark, library_benchmark_group, main, LibraryBenchmarkConfig};

use std::hint::black_box;

use s3db::execution::Execute;
use sql::CompatibleParser;
use storage::RelationStorage;

fn setup(
    query: &str,
    rows: usize,
) -> (
    sql::Query<'static, 'static>,
    s3db::execution::naive::NaiveEngine<storage::inmemory::InMemoryStorage>,
    storage::inmemory::InMemoryTransactionGuard,
) {
    let query = sql::Query::parse(query.as_bytes(), &bumpalo::Bump::new())
        .unwrap()
        .to_static();

    let storage = storage::inmemory::InMemoryStorage::new();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let transaction = runtime.block_on(async {
        let transcation = storage.start_transaction().await.unwrap();
        storage
            .create_relation(
                "users",
                vec![
                    ("id".into(), sql::DataType::Integer, Vec::new()),
                    ("name".into(), sql::DataType::Text, Vec::new()),
                ],
                &transcation,
            )
            .await
            .unwrap();

        let mut row_iter = (0..rows).map(|i| {
            vec![
                storage::Data::Integer(i as i32),
                storage::Data::Text(format!("name-{}", i)),
            ]
        });
        storage
            .insert_rows("users", &mut row_iter, &transcation)
            .await
            .unwrap();
        storage.commit_transaction(transcation).await.unwrap();
        storage.start_transaction().await.unwrap()
    });

    let engine = s3db::execution::naive::NaiveEngine::new(storage);

    (query, engine, transaction)
}

#[library_benchmark]
#[benches::with_setup(args = [("SELECT name FROM users", 10), ("SELECT name FROM users", 100), ("SELECT name FROM users", 1000)], setup = setup)]
fn select_1_column(
    (query, engine, transaction): (
        sql::Query<'static, 'static>,
        s3db::execution::naive::NaiveEngine<storage::inmemory::InMemoryStorage>,
        storage::inmemory::InMemoryTransactionGuard,
    ),
) -> Result<
    s3db::execution::ExecuteResult,
    s3db::execution::ExecuteError<
        s3db::execution::naive::PrepareError<storage::inmemory::LoadingError>,
        (),
        s3db::execution::naive::ExecuteBoundError<storage::inmemory::LoadingError>,
    >,
> {
    let mut ctx = s3db::execution::Context::new();
    ctx.transaction = Some(transaction);
    let arena = bumpalo::Bump::new();
    black_box(futures::executor::block_on(
        engine.execute(&query, &mut ctx, &arena),
    ))
}

#[library_benchmark]
#[benches::with_setup(args = [("SELECT name FROM users WHERE id < 5", 10), ("SELECT name FROM users WHERE id < 50", 100), ("SELECT name FROM users WHERE id < 500", 1000)], setup = setup)]
fn condition_less_than(
    (query, engine, transaction): (
        sql::Query<'static, 'static>,
        s3db::execution::naive::NaiveEngine<storage::inmemory::InMemoryStorage>,
        storage::inmemory::InMemoryTransactionGuard,
    ),
) -> Result<
    s3db::execution::ExecuteResult,
    s3db::execution::ExecuteError<
        s3db::execution::naive::PrepareError<storage::inmemory::LoadingError>,
        (),
        s3db::execution::naive::ExecuteBoundError<storage::inmemory::LoadingError>,
    >,
> {
    let mut ctx = s3db::execution::Context::new();
    ctx.transaction = Some(transaction);
    let arena = bumpalo::Bump::new();
    black_box(futures::executor::block_on(
        engine.execute(&query, &mut ctx, &arena),
    ))
}

library_benchmark_group!(
    name = queries;
    benchmarks = select_1_column, condition_less_than
);

main!(
    config = LibraryBenchmarkConfig::default();
    library_benchmark_groups = queries
);
