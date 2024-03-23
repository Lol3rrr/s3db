//! [Messages](https://www.postgresql.org/docs/16/protocol-message-formats.html)
//! [Message-Flow](https://www.postgresql.org/docs/current/protocol-flow.html)
//! [Error Fields](https://www.postgresql.org/docs/current/protocol-error-fields.html)
//! [Error Codes](https://www.postgresql.org/docs/current/errcodes-appendix.html)

use std::collections::HashMap;

use bytes::Buf;
use tokio::io::AsyncReadExt;

mod response;
pub use response::*;

mod comms;
pub use comms::{responde_execute_result, MessageFlowContext};

#[derive(Debug)]
pub enum ParseMessageError {
    Receive(std::io::Error),
    UnknownType {
        ty: u8,
        buffer: bytes::BytesMut,
    },
    UnparsedData {
        parsed: Message,
        remaining: bytes::BytesMut,
    },
    UnknownDescribeKind {
        received: u8,
    },
    Other,
}

#[derive(Debug)]
pub enum StartMessage {
    Startup { fields: HashMap<String, String> },
    SSLRequest {},
}

#[derive(Debug)]
pub enum Message {
    Terminate,
    Query {
        query: String,
    },
    Parse {
        destination: String,
        query: String,
        data_types: Vec<()>,
    },
    Describe {
        kind: DescribeKind,
        name: String,
    },
    Sync_,
    Bind {
        destination: String,
        statement: String,
        parameter_values: Vec<Vec<u8>>,
        parameter_formats: Vec<FormatCode>,
        result_column_format_codes: Vec<FormatCode>,
    },
    Execute {
        portal: String,
        max_rows: i32,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FormatCode {
    Text,
    Binary,
}

#[derive(Debug)]
pub enum DescribeKind {
    Statement,
    Portal,
}

fn read_string<I>(iter: &mut I) -> Option<String>
where
    I: Iterator<Item = u8>,
{
    let raw_str: Vec<u8> = iter.take_while(|v| *v != 0).collect();
    String::from_utf8(raw_str).ok()
}

impl StartMessage {
    #[tracing::instrument(skip(reader))]
    pub async fn parse<R>(reader: &mut R) -> Result<Self, ParseMessageError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let length = reader
            .read_i32()
            .await
            .map_err(|e| ParseMessageError::Receive(e))?
            - 4;

        let mut buffer = bytes::BytesMut::with_capacity(length as usize);
        reader
            .read_buf(&mut buffer)
            .await
            .map_err(|e| ParseMessageError::Receive(e))?;

        let protocol_version_major = buffer.get_u16();
        let protocol_version_minor = buffer.get_u16();

        if protocol_version_major == 1234 && protocol_version_minor == 5679 {
            return Ok(Self::SSLRequest {});
        }

        let mut fields = HashMap::new();

        let mut buffer_iter = buffer.into_iter();
        loop {
            let key = match read_string(&mut buffer_iter) {
                Some(k) => k,
                None => break,
            };

            if key.is_empty() {
                break;
            }

            let value = match read_string(&mut buffer_iter) {
                Some(v) => v,
                None => break,
            };

            fields.insert(key, value);
        }

        Ok(Self::Startup { fields })
    }
}

impl Message {
    #[tracing::instrument(skip(reader))]
    pub async fn parse<R>(reader: &mut R) -> Result<Self, ParseMessageError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let raw_type = reader
            .read_u8()
            .await
            .map_err(|e| ParseMessageError::Receive(e))?;
        let length = reader
            .read_i32()
            .await
            .map_err(|e| ParseMessageError::Receive(e))?
            - 4;

        tracing::trace!("Raw-Type: {:?} - Length: {:?}", raw_type, length,);

        let mut buffer = bytes::BytesMut::with_capacity(length as usize);
        if length > 0 {
            reader
                .read_buf(&mut buffer)
                .await
                .map_err(|e| ParseMessageError::Receive(e))?;
        }

        tracing::trace!("Buffer-Len: {:?}", buffer.len());

        let (buffer, result) = match raw_type {
            b'Q' => {
                let mut buffer_iter = buffer.into_iter();
                let query = read_string(&mut buffer_iter).ok_or(ParseMessageError::Other)?;

                (
                    bytes::BytesMut::from_iter(buffer_iter),
                    Self::Query { query },
                )
            }
            b'P' => {
                let mut buffer_iter = buffer.into_iter();

                let destination = read_string(&mut buffer_iter).ok_or(ParseMessageError::Other)?;
                let query = read_string(&mut buffer_iter).ok_or(ParseMessageError::Other)?;

                let mut buffer = bytes::BytesMut::from_iter(buffer_iter);

                let parameter_data_type_count = buffer.get_i16();

                assert_eq!(0, parameter_data_type_count);

                (
                    buffer,
                    Self::Parse {
                        destination,
                        query,
                        data_types: Vec::new(),
                    },
                )
            }
            b'D' => {
                let kind = match buffer.get_u8() {
                    b'S' => DescribeKind::Statement,
                    b'P' => DescribeKind::Portal,
                    other => {
                        return Err(ParseMessageError::UnknownDescribeKind { received: other })
                    }
                };

                let mut buffer_iter = buffer.into_iter();
                let name = read_string(&mut buffer_iter).ok_or(ParseMessageError::Other)?;

                (
                    bytes::BytesMut::from_iter(buffer_iter),
                    Self::Describe { kind, name },
                )
            }
            b'S' => (buffer, Self::Sync_),
            b'B' => {
                let mut buffer_iter = buffer.into_iter();

                let destination = read_string(&mut buffer_iter).ok_or(ParseMessageError::Other)?;
                let name = read_string(&mut buffer_iter).ok_or(ParseMessageError::Other)?;

                let mut buffer = bytes::BytesMut::from_iter(buffer_iter);

                let paramater_format_count = buffer.get_i16();

                let mut parameters_formats = Vec::new();
                for _ in 0..paramater_format_count {
                    todo!()
                }

                let parameter_value_count = buffer.get_i16();
                tracing::trace!("Parameter Value Count: {}", parameter_value_count);

                if parameters_formats.is_empty() {
                    parameters_formats.extend((0..parameter_value_count).map(|_| FormatCode::Text));
                } else if parameters_formats.len() == 1 && parameter_value_count > 1 {
                    let first_format = parameters_formats.first().cloned().unwrap();
                    parameters_formats
                        .extend((1..parameter_value_count).map(|_| first_format.clone()));
                }

                let mut parameter_values = Vec::new();
                for _ in 0..parameter_value_count {
                    let param_length = buffer.get_i32();

                    let tmp: Vec<_> = buffer
                        .iter_mut()
                        .map(|v| *v)
                        .take(param_length as usize)
                        .collect();
                    parameter_values.push(tmp);

                    buffer.advance(param_length as usize);
                }

                let result_column_format_code_count = buffer.get_i16();
                tracing::trace!(
                    "Result Column format count: {}",
                    result_column_format_code_count
                );

                let result_column_format_codes: Vec<_> = (0..result_column_format_code_count)
                    .map(|_| match buffer.get_i16() {
                        0 => Ok(FormatCode::Text),
                        1 => Ok(FormatCode::Binary),
                        _ => Err(ParseMessageError::Other),
                    })
                    .collect::<Result<_, _>>()?;

                (
                    buffer,
                    Self::Bind {
                        destination,
                        statement: name,
                        parameter_values,
                        parameter_formats: parameters_formats,
                        result_column_format_codes,
                    },
                )
            }
            b'E' => {
                let mut buffer_iter = buffer.into_iter();

                let portal_name = read_string(&mut buffer_iter).ok_or(ParseMessageError::Other)?;

                let mut buffer = bytes::BytesMut::from_iter(buffer_iter);

                let rows_max = buffer.get_i32();

                (
                    buffer,
                    Self::Execute {
                        portal: portal_name,
                        max_rows: rows_max,
                    },
                )
            }
            b'X' => (buffer, Self::Terminate),
            other => return Err(ParseMessageError::UnknownType { ty: other, buffer }),
        };

        if buffer.len() > 0 {
            return Err(ParseMessageError::UnparsedData {
                parsed: result,
                remaining: buffer,
            });
        }

        Ok(result)
    }
}
