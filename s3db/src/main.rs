#![feature(variant_count)]
#![feature(alloc_layout_extra)]

use std::{collections::HashMap, fs::File, rc::Rc};

use s3db::execution::{self, PreparedStatement};
use sql::Query;
use tokio::net::{TcpListener, TcpStream};
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

    tracing::info!("Hello, world!");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let local_set = tokio::task::LocalSet::new();
    local_set.spawn_local(postgres());

    runtime.block_on(local_set);
}

async fn postgres() {
    let listener = TcpListener::bind("0.0.0.0:5432").await.unwrap();

    let storage = s3db::storage::inmemory::InMemoryStorage::new();
    let engine = Rc::new(s3db::execution::naive::NaiveEngine::new(storage));

    loop {
        let (c, addr) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("Accepting: {:?}", e);
                continue;
            }
        };

        tokio::task::spawn_local(handle_postgres_connection(c, addr, engine.clone()));
    }
}

#[tracing::instrument(skip(connection, engine))]
async fn handle_postgres_connection<E, T>(
    mut connection: TcpStream,
    addr: std::net::SocketAddr,
    engine: Rc<E>,
) where
    E: s3db::execution::Execute<T>,
{
    let startmsg = match s3db::postgres::StartMessage::parse(&mut connection).await {
        Ok(sm) => sm,
        Err(e) => {
            tracing::error!("Parsing StartMessage: {:?}", e);
            return;
        }
    };

    match startmsg {
        s3db::postgres::StartMessage::Startup { fields } => {
            tracing::info!("Startup Fields: {:#?}", fields);
        }
        s3db::postgres::StartMessage::SSLRequest {} => {
            tracing::info!("Denying SSL Request");

            s3db::postgres::MessageResponse::DenySSL
                .send(&mut connection)
                .await
                .unwrap();

            tracing::info!("Denied");

            match s3db::postgres::StartMessage::parse(&mut connection).await {
                Ok(s3db::postgres::StartMessage::Startup { fields }) => {
                    tracing::info!("Startup Fields: {:#?}", fields);
                }
                Ok(other) => {
                    tracing::error!("Unexpected StartMessage: {:?}", other);
                    return;
                }
                Err(e) => {
                    tracing::error!("Parsing StartMessage: {:?}", e);
                    return;
                }
            };
        }
    };

    s3db::postgres::MessageResponse::AuthenticationOk
        .send(&mut connection)
        .await
        .unwrap();
    s3db::postgres::MessageResponse::ReadyForQuery {
        transaction_state: b'I',
    }
    .send(&mut connection)
    .await
    .unwrap();

    let mut ctx = s3db::execution::Context::new();

    let mut prepared_statements = HashMap::new();
    let mut bound_statements =
        HashMap::<String, <E::Prepared as execution::PreparedStatement>::Bound>::new();

    loop {
        tracing::trace!("Waiting for message");

        let msg = match s3db::postgres::Message::parse(&mut connection).await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Parsing Message: {:?}", e);
                break;
            }
        };

        match msg {
            s3db::postgres::Message::Terminate => {
                let _span = tracing::info_span!("terminate").entered();

                tracing::info!("Terminating");
                break;
            }
            s3db::postgres::Message::Query { query } => {
                let _span = tracing::info_span!("query").entered();

                tracing::info!("Handling Raw-Query: {:?}", query);

                let queries = match sql::Query::parse_multiple(query.as_bytes()) {
                    Ok(q) => q,
                    Err(e) => {
                        tracing::error!("Parsing Query: {:?}", e);
                        tracing::error!("Query: {:?}", query);

                        s3db::postgres::ErrorResponseBuilder::new(
                            s3db::postgres::ErrorSeverities::Fatal,
                            "0A000",
                        )
                        .message("Failed to parse Query")
                        .build()
                        .send(&mut connection)
                        .await
                        .unwrap();

                        return;
                    }
                };

                tracing::debug!("Parsed Queries: {:#?}", queries);

                let query_count = queries.len();
                for (is_last, query) in queries
                    .into_iter()
                    .enumerate()
                    .map(|(i, q)| (i + 1 == query_count, q))
                {
                    if let Query::Prepare(prep) = query {
                        // TODO
                        dbg!(prep);

                        continue;
                    }

                    let result = match engine.execute(&query, &mut ctx).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!("Executing: {:?}", e);

                            s3db::postgres::ErrorResponseBuilder::new(
                                s3db::postgres::ErrorSeverities::Fatal,
                                "0A000",
                            )
                            .message("Failed to execute Query")
                            .build()
                            .send(&mut connection)
                            .await
                            .unwrap();

                            s3db::postgres::MessageResponse::ReadyForQuery {
                                transaction_state: b'I',
                            }
                            .send(&mut connection)
                            .await
                            .unwrap();

                            continue;
                        }
                    };

                    tracing::debug!("Result: {:?}", result);

                    match s3db::postgres::responde_execute_result(
                        &mut connection,
                        result,
                        &mut ctx,
                        s3db::postgres::MessageFlowContext::SimpleQuery,
                        is_last,
                    )
                    .await
                    {
                        Ok(_) => {
                            tracing::info!("Sending Response");
                        }
                        Err(_) => {
                            tracing::error!("Sending Response");
                        }
                    };
                }
            }
            s3db::postgres::Message::Parse {
                destination,
                query,
                data_types,
            } => {
                let _span = tracing::info_span!("parse").entered();

                tracing::info!("Parsing Query as Prepared Statement");

                tracing::debug!("Parameters: {:?} - {:?}", &destination, &data_types);

                tracing::info!("Query: {:?}", query);

                let query = match sql::Query::parse(query.as_bytes()) {
                    Ok(q) => q,
                    Err(e) => {
                        tracing::error!("Parsing Query: {:?}", e);
                        tracing::error!("Query: {:?}", query);
                        return;
                    }
                };

                tracing::debug!("Parsed Query: {:#?}", query);

                let prepared = match engine.prepare(&query, &mut ctx).await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!("Preparing Query: {:?}", e);

                        s3db::postgres::ErrorResponseBuilder::new(
                            s3db::postgres::ErrorSeverities::Fatal,
                            "0A000",
                        )
                        .message("Failed to Prepare Query")
                        .build()
                        .send(&mut connection)
                        .await
                        .unwrap();

                        continue;
                    }
                };
                prepared_statements.insert(destination, prepared);

                s3db::postgres::MessageResponse::ParseComplete
                    .send(&mut connection)
                    .await
                    .unwrap();

                tracing::info!("Send Parse complete");
            }
            s3db::postgres::Message::Describe { kind, name } => {
                let _span = tracing::info_span!("describe").entered();

                tracing::info!("Describing: {:?} - {:?}", name, kind);

                // TODO

                match kind {
                    s3db::postgres::DescribeKind::Statement => {
                        let prepared = match prepared_statements.get(&name) {
                            Some(p) => p,
                            None => {
                                tracing::error!("Unknown Statement: {:?}", name);
                                continue;
                            }
                        };

                        let parameters = prepared.parameters();
                        tracing::trace!("Expecting Parameters: {:?}", parameters);

                        s3db::postgres::MessageResponse::ParameterDescription {
                            parameters: parameters.iter().map(|_| 0).collect(),
                        }
                        .send(&mut connection)
                        .await
                        .unwrap();
                        tracing::trace!("Send ParameterDescription");

                        let row_columns = prepared.row_columns();

                        s3db::postgres::MessageResponse::RowDescription {
                            fields: row_columns
                                .iter()
                                .map(|(name, dtype)| s3db::postgres::RowDescriptionField {
                                    name: name.clone(),
                                    table_id: 0,
                                    column_attribute: 1,
                                    type_id: dtype.type_oid(),
                                    type_size: dtype.size(),
                                    type_modifier: 0,
                                    format_code: 0,
                                })
                                .collect(),
                        }
                        .send(&mut connection)
                        .await
                        .unwrap();
                        tracing::trace!("Send RowDescription");
                    }
                    s3db::postgres::DescribeKind::Portal => {
                        todo!();
                    }
                };
            }
            s3db::postgres::Message::Sync_ => {
                let _span = tracing::info_span!("sync").entered();

                tracing::debug!("Sync");

                s3db::postgres::MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(&mut connection)
                .await
                .unwrap();
            }
            s3db::postgres::Message::Bind {
                destination,
                statement,
                parameter_values,
                parameter_formats,
                result_column_format_codes,
            } => {
                let _span = tracing::info_span!("bind").entered();

                tracing::info!("Binding: {:?} - {:?}", destination, statement,);
                tracing::debug!("Values: {:?}", parameter_values);
                tracing::info!("Parameter Format Codes: {:?}", parameter_formats);
                tracing::info!(
                    "Result-Column Format Codes: {:?}",
                    result_column_format_codes
                );

                let prepared = match prepared_statements.get(&statement) {
                    Some(p) => p,
                    None => {
                        tracing::error!("Unknown Prepared Statement: {:?}", statement);

                        s3db::postgres::ErrorResponseBuilder::new(
                            s3db::postgres::ErrorSeverities::Fatal,
                            "0A000",
                        )
                        .message("Could not find prepated Statement")
                        .build()
                        .send(&mut connection)
                        .await
                        .unwrap();

                        continue;
                    }
                };

                match prepared.bind(parameter_values, result_column_format_codes) {
                    Ok(b) => {
                        bound_statements.insert(destination, b);
                    }
                    Err(e) => {
                        // TODO
                        tracing::error!("Binding: {:?}", e);
                    }
                };

                s3db::postgres::MessageResponse::BindComplete
                    .send(&mut connection)
                    .await
                    .unwrap();
            }
            s3db::postgres::Message::Execute { portal, max_rows } => {
                let _span = tracing::info_span!("execute").entered();

                tracing::info!("Executing Query: {:?} - {}", portal, max_rows);

                let bound_statement = match bound_statements.get(&portal) {
                    Some(b) => b,
                    None => {
                        tracing::error!("Unknown Portal: {:?}", portal);

                        s3db::postgres::ErrorResponseBuilder::new(
                            s3db::postgres::ErrorSeverities::Fatal,
                            "0A000",
                        )
                        .message("Could not find prepated Statement")
                        .build()
                        .send(&mut connection)
                        .await
                        .unwrap();

                        continue;
                    }
                };

                let result = match engine.execute_bound(bound_statement, &mut ctx).await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!("Executing Bound: {:?}", e);

                        s3db::postgres::ErrorResponseBuilder::new(
                            s3db::postgres::ErrorSeverities::Fatal,
                            "0A000",
                        )
                        .message("Could not find prepated Statement")
                        .build()
                        .send(&mut connection)
                        .await
                        .unwrap();

                        continue;
                    }
                };

                match s3db::postgres::responde_execute_result(
                    &mut connection,
                    result,
                    &mut ctx,
                    s3db::postgres::MessageFlowContext::ExtendedQuery,
                    true,
                )
                .await
                {
                    Ok(_) => {
                        tracing::info!("Sending Response");
                    }
                    Err(e) => {
                        tracing::error!("Sending Response: {:?}", e);
                    }
                };
            }
        };
    }
}
