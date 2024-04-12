use std::{borrow::Cow, io::Write};

use bytes::BufMut;
use nom::AsBytes;
use tokio::io::AsyncWriteExt;

use super::FormatCode;

/// [Reference](https://www.postgresql.org/docs/current/protocol-message-formats.html) all Messages
/// being send by the backend are marked with a `(B)`
#[derive(Debug)]
pub enum MessageResponse<'d> {
    DenySSL,
    Error {
        fields: Vec<ErrorField>,
    },
    AuthenticationOk,
    ReadyForQuery {
        transaction_state: u8,
    },
    ParseComplete,
    ParameterDescription {
        parameters: Vec<i32>,
    },
    RowDescription {
        fields: Vec<RowDescriptionField>,
    },
    BindComplete,
    DataRow {
        values: Vec<Cow<'d, [u8]>>,
    },
    NoData,
    CommandComplete {
        tag: String,
    },
    CopyInResponse {
        format: FormatCode,
        columns: i16,
        column_formats: Vec<FormatCode>,
    },
}

#[derive(Debug)]
pub struct RowDescriptionField {
    pub name: String,
    pub table_id: i32,
    pub column_attribute: i16,
    pub type_id: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

/// [Reference](https://www.postgresql.org/docs/current/protocol-error-fields.html)
#[derive(Debug)]
pub enum ErrorField {
    Severity(&'static str),
    /// [Reference](https://www.postgresql.org/docs/current/errcodes-appendix.html)
    Code(String),
    Message(String),
    DetailedMessage(String),
}

pub struct ErrorResponseBuilder {
    fields: Vec<ErrorField>,
}

#[derive(Debug)]
pub enum SendError {
    Write(std::io::Error),
    Other,
}

impl<'d> MessageResponse<'d> {
    pub async fn send<W>(self, writer: &mut W) -> Result<(), SendError>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        {
            let _span = tracing::trace_span!("sending").entered();

            tracing::trace!("Sending {:?}", self);
        }

        match self {
            Self::DenySSL => {
                writer.write_u8(b'N').await.map_err(SendError::Write)?;
                Ok(())
            }
            Self::Error { fields } => {
                let total_size = 4
                    + 1
                    + fields
                        .iter()
                        .map(|f| match f {
                            ErrorField::Severity(s) => 1 + s.len() + 1,
                            ErrorField::Code(code) => 1 + code.len() + 1,
                            ErrorField::Message(m) => 1 + m.len() + 1,
                            ErrorField::DetailedMessage(m) => 1 + m.len() + 1,
                        })
                        .map(|i| i as i32)
                        .sum::<i32>();

                let mut buffer = bytes::BytesMut::with_capacity(total_size as usize + 1);

                let mut buf_writer = (&mut buffer).writer();
                buf_writer.write(&[b'E']).map_err(SendError::Write)?;
                buf_writer
                    .write(&total_size.to_be_bytes())
                    .map_err(SendError::Write)?;

                for field in fields {
                    match field {
                        ErrorField::Code(code) => {
                            buf_writer.write(&[b'C']).map_err(SendError::Write)?;
                            buf_writer
                                .write(code.as_bytes())
                                .map_err(SendError::Write)?;
                            buf_writer.write(&[0]).map_err(SendError::Write)?;
                        }
                        ErrorField::Severity(s) => {
                            buf_writer.write(&[b'S']).map_err(SendError::Write)?;
                            buf_writer.write(s.as_bytes()).map_err(SendError::Write)?;
                            buf_writer.write(&[0]).map_err(SendError::Write)?;
                        }
                        ErrorField::Message(m) => {
                            buf_writer.write(&[b'M']).map_err(SendError::Write)?;
                            buf_writer.write(m.as_bytes()).map_err(SendError::Write)?;
                            buf_writer.write(&[0]).map_err(SendError::Write)?;
                        }
                        ErrorField::DetailedMessage(m) => {
                            buf_writer.write(&[b'D']).map_err(SendError::Write)?;
                            buf_writer.write(m.as_bytes()).map_err(SendError::Write)?;
                            buf_writer.write(&[0]).map_err(SendError::Write)?;
                        }
                    };
                }
                buf_writer.write(&[0]).map_err(SendError::Write)?;

                writer
                    .write(buffer.as_bytes())
                    .await
                    .map_err(SendError::Write)?;
                writer.flush().await.map_err(SendError::Write)?;

                Ok(())
            }
            Self::AuthenticationOk => {
                writer
                    .write(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0])
                    .await
                    .map_err(SendError::Write)?;
                Ok(())
            }
            Self::ReadyForQuery { transaction_state } => {
                writer
                    .write(&[b'Z', 0, 0, 0, 5, transaction_state])
                    .await
                    .map_err(SendError::Write)?;
                writer.flush().await.map_err(SendError::Write)?;
                Ok(())
            }
            Self::ParseComplete => {
                writer
                    .write(&[b'1', 0, 0, 0, 4])
                    .await
                    .map_err(SendError::Write)?;
                Ok(())
            }
            Self::ParameterDescription { parameters } => {
                let total_length = 6 + parameters.len() as i32 * 4 + 1;

                let mut buffer = bytes::BytesMut::with_capacity(1 + total_length as usize);

                let mut buff_writer = (&mut buffer).writer();

                buff_writer.write(&[b't']).map_err(SendError::Write)?;
                buff_writer
                    .write(&total_length.to_be_bytes())
                    .map_err(SendError::Write)?;

                buff_writer
                    .write(&(parameters.len() as i16).to_be_bytes())
                    .map_err(SendError::Write)?;

                for parameter in parameters {
                    buff_writer
                        .write(&parameter.to_be_bytes())
                        .map_err(SendError::Write)?;
                }
                buff_writer.write(&[0]).map_err(SendError::Write)?;

                // assert_eq!(buffer.len(), total_length as usize + 1);

                writer
                    .write(buffer.as_bytes())
                    .await
                    .map_err(SendError::Write)?;
                writer.flush().await.map_err(SendError::Write)?;

                Ok(())
            }
            Self::RowDescription { fields } => {
                let total_length = 6 + fields
                    .iter()
                    .map(|f| f.name.len() as i32 + 1 + 4 + 2 + 4 + 2 + 4 + 2)
                    .sum::<i32>();

                let mut buffer = bytes::BytesMut::with_capacity(1 + total_length as usize);

                let mut buff_writer = (&mut buffer).writer();

                buff_writer.write(&[b'T']).map_err(SendError::Write)?;
                buff_writer
                    .write(&total_length.to_be_bytes())
                    .map_err(SendError::Write)?;

                buff_writer
                    .write(&(fields.len() as i16).to_be_bytes())
                    .map_err(SendError::Write)?;

                for field in fields {
                    buff_writer
                        .write(field.name.as_bytes())
                        .map_err(SendError::Write)?;
                    buff_writer.write(&[0]).map_err(SendError::Write)?;
                    buff_writer
                        .write(&field.table_id.to_be_bytes())
                        .map_err(SendError::Write)?;
                    buff_writer
                        .write(&field.column_attribute.to_be_bytes())
                        .map_err(SendError::Write)?;
                    buff_writer
                        .write(&field.type_id.to_be_bytes())
                        .map_err(SendError::Write)?;
                    buff_writer
                        .write(&field.type_size.to_be_bytes())
                        .map_err(SendError::Write)?;
                    buff_writer
                        .write(&field.type_modifier.to_be_bytes())
                        .map_err(SendError::Write)?;
                    buff_writer
                        .write(&field.format_code.to_be_bytes())
                        .map_err(SendError::Write)?;
                }

                tracing::trace!("Raw-Buffer: {:?}", buffer.as_bytes());

                writer
                    .write(buffer.as_bytes())
                    .await
                    .map_err(SendError::Write)?;

                Ok(())
            }
            Self::BindComplete => {
                writer
                    .write(&[b'2', 0, 0, 0, 4])
                    .await
                    .map_err(SendError::Write)?;
                Ok(())
            }
            Self::DataRow { values } => {
                let total_length = 6 + values.iter().map(|v| v.len() as i32 + 4).sum::<i32>();

                let mut buffer = bytes::BytesMut::with_capacity(total_length as usize);

                let mut buff_writer = (&mut buffer).writer();

                buff_writer.write(&[b'D']).map_err(SendError::Write)?;
                buff_writer
                    .write(&total_length.to_be_bytes())
                    .map_err(SendError::Write)?;
                buff_writer
                    .write(&(values.len() as i16).to_be_bytes())
                    .map_err(SendError::Write)?;

                for value in values {
                    buff_writer
                        .write(&(value.len() as i32).to_be_bytes())
                        .map_err(SendError::Write)?;
                    buff_writer.write(&value).map_err(SendError::Write)?;
                }

                tracing::trace!("Raw-Buffer: {:?}", buffer.as_bytes());

                writer
                    .write(buffer.as_bytes())
                    .await
                    .map_err(SendError::Write)?;

                Ok(())
            }
            Self::NoData => {
                writer
                    .write(&[b'n', 0, 0, 0, 4])
                    .await
                    .map_err(SendError::Write)?;
                Ok(())
            }
            Self::CommandComplete { tag } => {
                let total_length = 4 + tag.len() as i32 + 1;

                let mut buffer = bytes::BytesMut::with_capacity(total_length as usize);

                let mut buff_writer = (&mut buffer).writer();

                buff_writer.write(&[b'C']).map_err(SendError::Write)?;
                buff_writer
                    .write(&total_length.to_be_bytes())
                    .map_err(SendError::Write)?;
                buff_writer
                    .write(tag.as_bytes())
                    .map_err(SendError::Write)?;
                buff_writer.write(&[0]).map_err(SendError::Write)?;

                writer
                    .write(buffer.as_bytes())
                    .await
                    .map_err(SendError::Write)?;

                Ok(())
            }
            Self::CopyInResponse {
                format,
                columns,
                column_formats,
            } => {
                let total_length = 4 + 1 + 2 + column_formats.iter().map(|_| 2).sum::<i32>();

                let mut buffer = bytes::BytesMut::with_capacity(total_length as usize);

                let mut buff_writer = (&mut buffer).writer();

                buff_writer.write(&[b'G']).map_err(SendError::Write)?;
                buff_writer
                    .write(&total_length.to_be_bytes())
                    .map_err(SendError::Write)?;

                buff_writer
                    .write(match format {
                        FormatCode::Text => &[0],
                        FormatCode::Binary => &[1],
                    })
                    .map_err(SendError::Write)?;

                buff_writer
                    .write(&columns.to_be_bytes())
                    .map_err(SendError::Write)?;

                for column in column_formats {
                    buff_writer
                        .write(match column {
                            FormatCode::Text => &[0, 0],
                            FormatCode::Binary => &[0, 1],
                        })
                        .map_err(SendError::Write)?;
                }

                assert_eq!(buffer.len() - 1, total_length as usize);

                writer
                    .write(buffer.as_bytes())
                    .await
                    .map_err(SendError::Write)?;

                Ok(())
            }
        }
    }
}

impl ErrorResponseBuilder {
    pub fn new<C>(severity: ErrorSeverities, code: C) -> Self
    where
        C: Into<String>,
    {
        let mut fields = Vec::with_capacity(8);

        fields.push(ErrorField::Severity(severity.as_str()));
        fields.push(ErrorField::Code(code.into()));

        Self { fields }
    }

    pub fn message<M>(mut self, msg: M) -> Self
    where
        M: Into<String>,
    {
        self.fields.push(ErrorField::Message(msg.into()));
        self
    }

    pub fn detailed_message<M>(mut self, msg: M) -> Self
    where
        M: Into<String>,
    {
        self.fields.push(ErrorField::DetailedMessage(msg.into()));
        self
    }

    pub fn build(self) -> MessageResponse<'static> {
        MessageResponse::Error {
            fields: self.fields,
        }
    }
}

#[derive(Debug)]
pub enum ErrorSeverities {
    Error,
    Fatal,
    Panic,
}

impl ErrorSeverities {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Panic => "PANIC",
        }
    }
}
