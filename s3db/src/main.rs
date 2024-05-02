#![feature(variant_count)]
#![feature(alloc_layout_extra)]

use std::fs::File;

use s3db::endpoint::Endpoint;
use tracing::Level;
use tracing_subscriber::layer::SubscriberExt;

fn main() {
    let log_file = File::create("s3db.log").unwrap();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt::Subscriber::builder()
            .with_max_level(Level::INFO)
            .finish()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(log_file)
                    .with_ansi(false),
            ),
    )
    .unwrap();

    tracing::info!("Starting...");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let rstorage = storage::inmemory::v2::InMemoryStorage::new();
    let sstorage = storage::inmemory::InMemoryStorage::new();
    let engine = s3db::execution::naive::NaiveEngine::new(storage::composed::ComposedStorage::new(
        rstorage, sstorage,
    ));

    let endpoint = s3db::endpoint::postgres::PostgresEndpoint::new("0.0.0.0:5432");

    let local_set = tokio::task::LocalSet::new();
    local_set.spawn_local(async move {
        if let Err(e) = endpoint.run(engine).await {
            tracing::error!("Running Endpoint: {:?}", e);
        }
    });

    runtime.block_on(local_set);
}
