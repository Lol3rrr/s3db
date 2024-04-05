use tokio::io::AsyncWrite;

use crate::{
    execution,
    postgres::{FormatCode, MessageResponse, RowDescriptionField},
    storage,
};
use sql::DataType;

#[derive(Debug, PartialEq)]
pub enum MessageFlowContext {
    SimpleQuery,
    ExtendedQuery,
}

#[derive(Debug)]
pub enum RespondError {
    SerializeField {
        value: storage::Data,
        format: FormatCode,
    },
}

pub async fn responde_execute_result<T, W>(
    writer: &mut W,
    result: execution::ExecuteResult,
    ctx: &mut execution::Context<T>,
    flow: MessageFlowContext,
    is_last_response: bool,
) -> Result<(), RespondError>
where
    W: AsyncWrite + Unpin,
{
    match result {
        execution::ExecuteResult::Set => {
            MessageResponse::CommandComplete {
                tag: "SET".to_string(),
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
        execution::ExecuteResult::Select {
            content,
            mut formats,
        } => {
            tracing::debug!("Content: {:?}", content);

            let row_count = content.parts.iter().flat_map(|p| p.rows.iter()).count();

            if formats.is_empty() {
                formats.extend(core::iter::repeat(FormatCode::Text).take(content.columns.len()));
            } else if formats.len() == 1 {
                let entry = formats.get(0).cloned().unwrap();
                formats.extend(
                    core::iter::repeat(entry).take(content.columns.len().saturating_sub(1)),
                );
            } else {
                assert_eq!(content.columns.len(), formats.len());
            }

            if MessageFlowContext::SimpleQuery == flow {
                MessageResponse::RowDescription {
                    fields: content
                        .columns
                        .iter()
                        .zip(formats.iter())
                        .map(|((name, ty, _), format)| RowDescriptionField {
                            name: name.clone(),
                            table_id: 0,
                            column_attribute: 0,
                            type_size: ty.size(),
                            format_code: match format {
                                FormatCode::Text => 0,
                                FormatCode::Binary => 1,
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

                let values: Vec<_> = row
                    .data
                    .iter()
                    .zip(formats.iter())
                    .zip(content.columns.iter())
                    .map(|((data, format), column)| {
                        data.serialize(format, column.1.clone()).ok_or_else(|| {
                            RespondError::SerializeField {
                                value: data.clone(),
                                format: format.clone(),
                            }
                        })
                    })
                    .collect::<Result<_, _>>()?;
                MessageResponse::DataRow { values }
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
            mut formats,
        } => {
            tracing::trace!("Insert completed");
            tracing::trace!("Returning: {:?}", returning);

            if returning.len() > 0 {
                if formats.is_empty() {
                    formats.extend(core::iter::repeat(FormatCode::Text).take(returning[0].len()));
                } else if formats.len() == 1 {
                    let entry = formats.get(0).cloned().unwrap();
                    formats.extend(
                        core::iter::repeat(entry).take(returning[0].len().saturating_sub(1)),
                    );
                } else {
                    assert_eq!(returning[0].len(), formats.len());
                }

                for row in returning {
                    assert_eq!(row.len(), formats.len());

                    let values: Vec<_> = row
                        .iter()
                        .zip(formats.iter())
                        .map(|(r, format)| {
                            // TODO
                            // Where to get the type for this?
                            r.serialize(format, DataType::BigInteger).ok_or_else(|| {
                                RespondError::SerializeField {
                                    value: r.clone(),
                                    format: format.clone(),
                                }
                            })
                        })
                        .collect::<Result<_, _>>()?;

                    MessageResponse::DataRow { values }
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
        execution::ExecuteResult::Rollback => {
            MessageResponse::CommandComplete {
                tag: "ROLLBACK".to_string(),
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
        execution::ExecuteResult::Truncate => {
            MessageResponse::CommandComplete {
                tag: "TRUNCATE".to_string(),
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
