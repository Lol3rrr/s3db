#![feature(variant_count)]
#![feature(alloc_layout_extra)]

use std::fs::File;

use s3db::endpoint::Endpoint;
use tracing::Level;
use tracing_subscriber::layer::{SubscriberExt, Layer};

fn main() {
    let (console_layer, console_server) = console_subscriber::ConsoleLayer::builder().build();

    let log_file = File::create("s3db.log").unwrap();
    let tracing_reg = tracing_subscriber::Registry::default()
        .with(console_layer)
        .with(tracing_subscriber::fmt::layer().with_filter(tracing_subscriber::filter::filter_fn(|metadata| metadata.level() <= &Level::INFO)))
        .with(tracing_subscriber::fmt::Layer::new().with_writer(log_file).with_ansi(false));
    tracing::subscriber::set_global_default(
        tracing_reg
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
    local_set.spawn_local(async move {
        if let Err(e) = console_server.serve().await {
            tracing::error!("Running Console");
        }
    });

    runtime.block_on(local_set);
}
