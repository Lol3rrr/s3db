//! # Endpoint
//! An endpoint defines the entrypoint for a specific protocol. For example the [`postgres`]
//! endpoint allows for communication using the Postgres Protocol

use std::fmt::Debug;

use futures::future::LocalBoxFuture;

pub trait Endpoint<E, T>
where
    T: 'static,
{
    fn run<'s, 'f>(&'s self, engine: E) -> LocalBoxFuture<'f, Result<(), Box<dyn Debug>>>
    where
        's: 'f;
}

pub mod postgres {
    //! # Postgres-Endpoint
    //! Allows for communication using the Postgres Protocol

    use std::{
        collections::{HashMap, VecDeque},
        fmt::Debug,
        rc::Rc,
    };

    use futures::FutureExt;
    use sql::Query;
    use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

    use crate::{
        execution::{self, CopyState, ExecutionError, PreparedStatement},
        postgres,
    };

    use super::Endpoint;

    pub struct PostgresEndpoint<A> {
        address: A,
    }

    impl<A> PostgresEndpoint<A>
    where
        A: ToSocketAddrs,
    {
        pub fn new(address: A) -> Self {
            Self { address }
        }
    }

    #[derive(Debug)]
    pub enum RunError {
        Bind(tokio::io::Error),
        LocalAddr(tokio::io::Error),
    }

    pub trait EngineWrapper {
        type Engine;

        fn get(&self) -> Self::Engine;
    }

    impl<A, E, T> Endpoint<E, T> for PostgresEndpoint<A>
    where
        A: ToSocketAddrs + Clone,
        E: execution::Execute<T> + 'static,
        T: 'static,
    {
        fn run<'s, 'f>(
            &'s self,
            engine: E,
        ) -> futures::prelude::future::LocalBoxFuture<'f, Result<(), Box<dyn Debug>>>
        where
            's: 'f,
        {
            async {
                let listener = TcpListener::bind(self.address.clone())
                    .await
                    .map_err(RunError::Bind)
                    .map_err(|e| Box::new(e) as Box<dyn Debug>)?;
                let listener_addr = listener
                    .local_addr()
                    .map_err(RunError::LocalAddr)
                    .map_err(|e| Box::new(e) as Box<dyn Debug>)?;
                tracing::info!("Postgres Interface listening on '{:?}'", listener_addr);

                let engine = Rc::new(engine);

                loop {
                    let (c, addr) = match listener.accept().await {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!("Accepting: {:?}", e);
                            continue;
                        }
                    };

                    tracing::info!("Got new Connection");

                    tokio::task::spawn_local(handle_postgres_connection(c, addr, engine.clone()));
                }
            }
            .boxed_local()
        }
    }

    #[tracing::instrument(skip(connection, engine))]
    async fn handle_postgres_connection<E, T>(
        mut connection: TcpStream,
        addr: std::net::SocketAddr,
        engine: Rc<E>,
    ) where
        E: execution::Execute<T> + 'static,
    {
        let startmsg = match postgres::StartMessage::parse(&mut connection).await {
            Ok(sm) => sm,
            Err(e) => {
                tracing::error!("Parsing StartMessage: {:?}", e);
                return;
            }
        };

        match startmsg {
            postgres::StartMessage::Startup { fields } => {
                tracing::info!("Startup Fields: {:#?}", fields);
            }
            postgres::StartMessage::SSLRequest {} => {
                tracing::info!("Denying SSL Request");
                postgres::MessageResponse::DenySSL
                    .send(&mut connection)
                    .await
                    .unwrap();

                tracing::info!("Denied");

                match postgres::StartMessage::parse(&mut connection).await {
                    Ok(postgres::StartMessage::Startup { fields }) => {
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

        postgres::MessageResponse::AuthenticationOk
            .send(&mut connection)
            .await
            .unwrap();
        postgres::MessageResponse::ReadyForQuery {
            transaction_state: b'I',
        }
        .send(&mut connection)
        .await
        .unwrap();

        let mut ctx = execution::Context::new();

        let mut arena = bumpalo::Bump::with_capacity(512 * 1024);
        let mut execution_arena = bumpalo::Bump::with_capacity(512 * 1024);

        let mut prepared_statements = HashMap::new();
        let mut bound_statements = HashMap::new();

        loop {
            arena.reset();
            execution_arena.reset();

            tracing::trace!("Waiting for message");

            let msg = match postgres::Message::parse(&mut connection).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!("Parsing Message: {:?}", e);
                    break;
                }
            };

            match msg {
                postgres::Message::Terminate => {
                    let _span = tracing::info_span!("terminate").entered();

                    tracing::info!("Terminating");
                    break;
                }
                postgres::Message::Query { query } => {
                    let _span = tracing::info_span!("query").entered();

                    tracing::info!("Handling Raw-Query: {:?}", query);

                    let queries = match sql::Query::parse_multiple(query.as_bytes(), &arena) {
                        Ok(q) => q,
                        Err(e) => {
                            tracing::error!("Parsing Query: {:?}", e);
                            tracing::error!("Query: {:?}", query);

                            postgres::ErrorResponseBuilder::new(
                                postgres::ErrorSeverities::Fatal,
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

                        let mut implicit_transaction = false;
                        if ctx.transaction.is_none() && !matches!(query, Query::BeginTransaction(_))
                        {
                            tracing::info!("Starting implicit query");

                            implicit_transaction = true;
                            engine
                                .execute(
                                    &Query::BeginTransaction(sql::IsolationMode::Standard),
                                    &mut ctx,
                                    &execution_arena,
                                )
                                .await
                                .unwrap();
                        }

                        if let Query::Copy_(c) = query {
                            dbg!(&c);

                            let mut copy_state =
                                engine.start_copy(&c.target.0, &mut ctx).await.unwrap();

                            let tmp = copy_state.columns();

                            postgres::MessageResponse::CopyInResponse {
                                format: postgres::FormatCode::Text,
                                columns: tmp.len() as i16,
                                column_formats: tmp
                                    .into_iter()
                                    .map(|_| postgres::FormatCode::Text)
                                    .collect(),
                            }
                            .send(&mut connection)
                            .await
                            .unwrap();

                            tracing::info!("Send CopyInResponse");

                            let mut data_buffer = VecDeque::with_capacity(1024);
                            loop {
                                let msg = match postgres::Message::parse(&mut connection).await {
                                    Ok(m) => m,
                                    Err(e) => {
                                        tracing::error!("Parsing Message: {:?}", e);
                                        todo!()
                                    }
                                };

                                match msg {
                                    postgres::Message::CopyData { data } => {
                                        data_buffer.extend(data);

                                        while let Some(idx) = data_buffer
                                            .iter()
                                            .enumerate()
                                            .find(|(_, v)| **v == b'\n')
                                            .map(|(i, _)| i)
                                        {
                                            let remaining = data_buffer.split_off(idx);

                                            let mut inner = data_buffer;
                                            data_buffer = remaining;
                                            data_buffer.pop_front();

                                            let inner = inner.make_contiguous();

                                            if inner == [b'\\', b'.'] {
                                                continue;
                                            }

                                            copy_state.insert(inner).await.unwrap();
                                        }
                                    }
                                    postgres::Message::CopyDone => {
                                        // TODO
                                        break;
                                    }
                                    other => todo!("Unexpected Message: {:?}", other),
                                };
                            }

                            drop(copy_state);

                            if implicit_transaction {
                                engine
                                    .execute(&Query::CommitTransaction, &mut ctx, &execution_arena)
                                    .await
                                    .unwrap();
                            }

                            postgres::MessageResponse::CommandComplete { tag: "COPY".into() }
                                .send(&mut connection)
                                .await
                                .unwrap();

                            postgres::MessageResponse::ReadyForQuery {
                                transaction_state: ctx.transaction_state(),
                            }
                            .send(&mut connection)
                            .await
                            .unwrap();

                            continue;
                        }

                        let result =
                            match engine.execute(&query, &mut ctx, &execution_arena).await {
                                Ok(r) => r,
                                Err(e) => {
                                    tracing::error!("Executing: {:?}", e);

                                    if implicit_transaction {
                                        tracing::info!("Commiting implicit query");

                                        engine
                                            .execute(&Query::CommitTransaction, &mut ctx, &arena)
                                            .await
                                            .unwrap();
                                    }

                                    match e {
                                        execution::ExecuteError::Execute(exec)
                                            if exec.is_serialize() =>
                                        {
                                            tracing::error!("Sendind serialization error");

                                            postgres::ErrorResponseBuilder::new(
                                    postgres::ErrorSeverities::Error,
                                    "40001",
                                )
                                .message("could not serialize access due to concurrent update")
                                .build()
                                .send(&mut connection)
                                .await
                                .unwrap();
                                        }
                                        other => {
                                            postgres::ErrorResponseBuilder::new(
                                                postgres::ErrorSeverities::Fatal,
                                                "0A000",
                                            )
                                            .message("Failed to execute Query")
                                            .build()
                                            .send(&mut connection)
                                            .await
                                            .unwrap();
                                        }
                                    };

                                    postgres::MessageResponse::ReadyForQuery {
                                        transaction_state: b'I',
                                    }
                                    .send(&mut connection)
                                    .await
                                    .unwrap();

                                    continue;
                                }
                            };

                        if implicit_transaction {
                            tracing::info!("Commiting implicit query");

                            engine
                                .execute(&Query::CommitTransaction, &mut ctx, &arena)
                                .await
                                .unwrap();
                        }

                        tracing::debug!("Result: {:?}", result);

                        match postgres::responde_execute_result(
                            &mut connection,
                            result,
                            &mut ctx,
                            postgres::MessageFlowContext::SimpleQuery,
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
                postgres::Message::Parse {
                    destination,
                    query,
                    data_types,
                } => {
                    let _span = tracing::info_span!("parse").entered();

                    tracing::info!("Parsing Query as Prepared Statement");

                    tracing::debug!("Parameters: {:?} - {:?}", &destination, &data_types);

                    tracing::info!("Query: {:?}", query);

                    let query = match sql::Query::parse(query.as_bytes(), &arena) {
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

                            postgres::ErrorResponseBuilder::new(
                                postgres::ErrorSeverities::Fatal,
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

                    postgres::MessageResponse::ParseComplete
                        .send(&mut connection)
                        .await
                        .unwrap();

                    tracing::info!("Send Parse complete");
                }
                postgres::Message::Describe { kind, name } => {
                    let _span = tracing::info_span!("describe").entered();

                    tracing::info!("Describing: {:?} - {:?}", name, kind);

                    // TODO

                    match kind {
                        postgres::DescribeKind::Statement => {
                            let prepared = match prepared_statements.get(&name) {
                                Some(p) => p,
                                None => {
                                    tracing::error!("Unknown Statement: {:?}", name);
                                    continue;
                                }
                            };

                            let parameters = prepared.parameters();
                            tracing::trace!("Expecting Parameters: {:?}", parameters);

                            postgres::MessageResponse::ParameterDescription {
                                parameters: parameters.iter().map(|_| 0).collect(),
                            }
                            .send(&mut connection)
                            .await
                            .unwrap();
                            tracing::trace!("Send ParameterDescription");

                            let row_columns = prepared.row_columns();

                            postgres::MessageResponse::RowDescription {
                                fields: row_columns
                                    .iter()
                                    .map(|(name, dtype)| postgres::RowDescriptionField {
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
                        postgres::DescribeKind::Portal => {
                            todo!();
                        }
                    };
                }
                postgres::Message::Sync_ => {
                    let _span = tracing::info_span!("sync").entered();

                    tracing::debug!("Sync");

                    postgres::MessageResponse::ReadyForQuery {
                        transaction_state: ctx.transaction_state(),
                    }
                    .send(&mut connection)
                    .await
                    .unwrap();
                }
                postgres::Message::Bind {
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

                            postgres::ErrorResponseBuilder::new(
                                postgres::ErrorSeverities::Fatal,
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

                    postgres::MessageResponse::BindComplete
                        .send(&mut connection)
                        .await
                        .unwrap();
                }
                postgres::Message::Execute { portal, max_rows } => {
                    let _span = tracing::info_span!("execute").entered();

                    tracing::info!("Executing Query: {:?} - {}", portal, max_rows);

                    let bound_statement = match bound_statements.get(&portal) {
                        Some(b) => b,
                        None => {
                            tracing::error!("Unknown Portal: {:?}", portal);

                            postgres::ErrorResponseBuilder::new(
                                postgres::ErrorSeverities::Fatal,
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

                    let result = match engine
                        .execute_bound(bound_statement, &mut ctx, &execution_arena)
                        .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!("Executing Bound: {:?}", e);

                            postgres::ErrorResponseBuilder::new(
                                postgres::ErrorSeverities::Fatal,
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

                    match postgres::responde_execute_result(
                        &mut connection,
                        result,
                        &mut ctx,
                        postgres::MessageFlowContext::ExtendedQuery,
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
                postgres::Message::CopyData { .. } => {
                    tracing::error!("CopyData message in normal Context");

                    postgres::ErrorResponseBuilder::new(postgres::ErrorSeverities::Fatal, "0A000")
                        .message("CopyData in normal/non-copy context")
                        .build()
                        .send(&mut connection)
                        .await
                        .unwrap();
                }
                postgres::Message::CopyDone => {
                    tracing::error!("CopyDone message in normal Context");

                    postgres::ErrorResponseBuilder::new(postgres::ErrorSeverities::Fatal, "0A000")
                        .message("CopyDone in normal/non-copy context")
                        .build()
                        .send(&mut connection)
                        .await
                        .unwrap();
                }
            };
        }

        drop(prepared_statements);
    }
}
