//! [Messages](https://www.postgresql.org/docs/16/protocol-message-formats.html)
//! [Message-Flow](https://www.postgresql.org/docs/current/protocol-flow.html)
//! [Error Fields](https://www.postgresql.org/docs/current/protocol-error-fields.html)
//! [Error Codes](https://www.postgresql.org/docs/current/errcodes-appendix.html)

use std::collections::HashMap;

use bytes::Buf;
use nom::{error::dbg_dmp, IResult, Parser};
use tokio::io::AsyncReadExt;

mod response;
pub use response::*;

mod message;
pub use message::{DescribeKind, FormatCode, Message};

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

fn read_string<I>(iter: &mut I) -> Option<String>
where
    I: Iterator<Item = u8>,
{
    let raw_str: Vec<u8> = iter.take_while(|v| *v != 0).collect();
    String::from_utf8(raw_str).ok()
}

fn parse_string(data: &[u8]) -> IResult<&[u8], String> {
    let (remaining, (data, _)) = nom::sequence::tuple((
        nom::bytes::streaming::take_till(|d| d == 0),
        nom::bytes::streaming::tag([0]),
    ))(data)?;
    let content = String::from_utf8(data.to_vec()).unwrap();

    Ok((remaining, content))
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
