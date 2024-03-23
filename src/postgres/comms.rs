use tokio::io::AsyncWrite;

use crate::{
    execution,
    postgres::{MessageResponse, RowDescriptionField},
    sql,
};

#[derive(Debug, PartialEq)]
pub enum MessageFlowContext {
    SimpleQuery,
    ExtendedQuery,
}

pub async fn responde_execute_result<W>(
    writer: &mut W,
    result: execution::ExecuteResult,
    ctx: &mut execution::Context,
    flow: MessageFlowContext,
    is_last_response: bool,
) -> Result<(), ()>
where
    W: AsyncWrite + Unpin,
{
    match result {
        execution::ExecuteResult::Select { content } => {
            tracing::info!("Content: {:?}", content);

            let row_count = content.parts.iter().flat_map(|p| p.rows.iter()).count();

            if MessageFlowContext::SimpleQuery == flow {
                MessageResponse::RowDescription {
                    fields: content
                        .columns
                        .iter()
                        .map(|(name, ty, _)| RowDescriptionField {
                            name: name.clone(),
                            table_id: 0,
                            column_attribute: 0,
                            type_size: ty.size(),
                            format_code: match ty {
                                sql::DataType::Serial
                                | sql::DataType::BigSerial
                                | sql::DataType::SmallInteger
                                | sql::DataType::Integer
                                | sql::DataType::BigInteger => 1,
                                _ => 0,
                            },
                            type_id: ty.type_oid(),
                            type_modifier: 0,
                        })
                        .collect(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            for row in content.parts.into_iter().flat_map(|p| p.rows.into_iter()) {
                tracing::trace!("Row: {:?}", row);

                MessageResponse::DataRow {
                    values: row.data.iter().map(|r| r.serialize()).collect(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            MessageResponse::CommandComplete {
                tag: format!("SELECT {}", row_count),
            }
            .send(writer)
            .await
            .unwrap();

            if flow == MessageFlowContext::SimpleQuery && is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Begin => {
            ctx.transaction = Some(());

            MessageResponse::CommandComplete {
                tag: "BEGIN".to_string(),
            }
            .send(writer)
            .await
            .unwrap();

            if is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Create => {
            MessageResponse::CommandComplete {
                tag: "CREATE".to_string(),
            }
            .send(writer)
            .await
            .unwrap();

            if is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Alter => {
            MessageResponse::CommandComplete {
                tag: "ALTER".to_string(),
            }
            .send(writer)
            .await
            .unwrap();

            if is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Insert {
            inserted_rows,
            returning,
        } => {
            tracing::trace!("Insert completed");
            tracing::trace!("Returning: {:?}", returning);

            if returning.len() > 0 {
                for row in returning {
                    MessageResponse::DataRow {
                        values: row.iter().map(|r| r.serialize()).collect(),
                    }
                    .send(writer)
                    .await
                    .unwrap();
                }
            }

            MessageResponse::CommandComplete {
                tag: format!("INSERT 0 {}", inserted_rows),
            }
            .send(writer)
            .await
            .unwrap();

            if flow == MessageFlowContext::SimpleQuery && is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Update { updated_rows } => {
            MessageResponse::CommandComplete {
                tag: format!("UPDATE {}", updated_rows),
            }
            .send(writer)
            .await
            .unwrap();

            if flow == MessageFlowContext::SimpleQuery && is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Delete { deleted_rows } => {
            MessageResponse::CommandComplete {
                tag: format!("DELETE {}", deleted_rows),
            }
            .send(writer)
            .await
            .unwrap();

            if flow == MessageFlowContext::SimpleQuery && is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Commit => {
            ctx.transaction = None;

            MessageResponse::CommandComplete {
                tag: "COMMIT".to_string(),
            }
            .send(writer)
            .await
            .unwrap();

            if is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
        execution::ExecuteResult::Drop_ => {
            MessageResponse::CommandComplete {
                tag: "DROP".to_string(),
            }
            .send(writer)
            .await
            .unwrap();

            if is_last_response {
                MessageResponse::ReadyForQuery {
                    transaction_state: ctx.transaction_state(),
                }
                .send(writer)
                .await
                .unwrap();
            }

            Ok(())
        }
    }
}
