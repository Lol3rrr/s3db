use iai_callgrind::{main, library_benchmark_group, library_benchmark};
use std::hint::black_box;

use sql::CompatibleParser;
use s3db::storage::Storage;
use s3db::execution::Execute;

fn setup(query: &str, rows: usize) -> (sql::Query<'static, 'static>, s3db::execution::naive::NaiveEngine<s3db::storage::inmemory::InMemoryStorage>, s3db::storage::inmemory::InMemoryTransactionGuard) {
    let query = sql::Query::parse(query.as_bytes(), &bumpalo::Bump::new())
            .unwrap()
            .to_static();

        let storage = s3db::storage::inmemory::InMemoryStorage::new();

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
                    s3db::storage::Data::Integer(i as i32),
                    s3db::storage::Data::Text(format!("name-{}", i)),
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
fn select_1_column((query, engine, transaction): (sql::Query<'static, 'static>, s3db::execution::naive::NaiveEngine<s3db::storage::inmemory::InMemoryStorage>, s3db::storage::inmemory::InMemoryTransactionGuard)) -> Result<s3db::execution::ExecuteResult, s3db::execution::ExecuteError<s3db::execution::naive::PrepareError<s3db::storage::inmemory::LoadingError>, (), s3db::execution::naive::ExecuteBoundError<s3db::storage::inmemory::LoadingError>>> {
    let mut ctx = s3db::execution::Context::new();
    ctx.transaction = Some(transaction);
    let arena = bumpalo::Bump::new();
    black_box(futures::executor::block_on(engine.execute(&query, &mut ctx, &arena)))
}

library_benchmark_group!(
    name = queries;
    benchmarks = select_1_column
);

main!(library_benchmark_groups = queries);
