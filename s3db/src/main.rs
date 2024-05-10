#![feature(variant_count)]
#![feature(alloc_layout_extra)]

use std::fs::File;

use s3db::endpoint::Endpoint;
use tracing::Level;
use tracing_subscriber::layer::{Layer, SubscriberExt};

fn main() {
    #[cfg(debug_assertions)]
    let (console_layer, console_server) = console_subscriber::ConsoleLayer::builder().build();

    let log_file = File::create("s3db.log").unwrap();
    let tracing_reg = tracing_subscriber::Registry::default();
    #[cfg(debug_assertions)]
    let tracing_reg = tracing_reg.with(console_layer);
    let tracing_reg =
        tracing_reg
            .with(tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::filter_fn(|metadata| metadata.level() <= &Level::INFO),
            ))
            .with(
                tracing_subscriber::fmt::Layer::new()
                    .with_writer(log_file)
                    .with_ansi(false)
                    .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                        metadata.level() <= &Level::INFO
                    })),
            );
    tracing::subscriber::set_global_default(tracing_reg).unwrap();

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
    #[cfg(debug_assertions)]
    local_set.spawn_local(async move {
        if let Err(e) = console_server.serve().await {
            tracing::error!("Running Console: {:?}", e);
        }
    });

    #[cfg(profiling)]
    let pprof_guard = pprof::ProfilerGuardBuilder::default()
        .frequency(500)
        .blocklist(&["libc", "pthread"])
        .build()
        .unwrap();

    local_set.spawn_local(async move {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();

        tokio::select! {
            _ = terminate.recv() => tracing::info!("Received Termiante"),
            _ = interrupt.recv() => tracing::info!("Received Interrupt"),
        };

        #[cfg(profiling)]
        {
            tracing::info!("Generating Profiling Report");

            match pprof_guard.report().build() {
                Ok(rep) => match rep.pprof() {
                    Ok(pprof_rep) => {
                        let mut file = match tokio::fs::File::create("s3db.pb").await {
                            Ok(f) => f,
                            Err(e) => {
                                tracing::error!("Creating pprof file: {:?}", e);
                                return;
                            }
                        };

                        let mut buffer = Vec::new();
                        if let Err(e) = pprof_rep.write_to_vec(&mut buffer) {
                            tracing::error!("Writing PPROF to buffer: {:?}", e);
                            return;
                        }

                        if let Err(e) = file.write_all(&buffer).await {
                            tracing::error!("Writing buffer to file: {:?}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Generating PProf Report: {:?}", e);
                    }
                },
                Err(e) => {
                    tracing::error!("Building Report: {:?}", e);
                }
            };
        }

        std::process::exit(0);
    });

    runtime.block_on(local_set);
}
