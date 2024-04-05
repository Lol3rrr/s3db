use bytes::{Buf, BufMut};
use nom::{error::dbg_dmp, IResult, Parser};
use tokio::io::AsyncReadExt;

use super::{parse_string, read_string, ParseMessageError};

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum DescribeKind {
    Statement,
    Portal,
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

        let mut buffer = bytes::BytesMut::with_capacity(length as usize + 1);
        buffer.put_u8(raw_type);
        if length > 0 {
            reader
                .read_buf(&mut buffer)
                .await
                .map_err(|e| ParseMessageError::Receive(e))?;
        }

        tracing::trace!("Buffer-Len: {:?}", buffer.len());

        let (remaining, result) = Self::parse_internal(&buffer).map_err(|e| {
            dbg!(e);
            ParseMessageError::Other
        })?;

        if remaining.len() > 0 {
            return Err(ParseMessageError::UnparsedData {
                parsed: result,
                remaining: buffer,
            });
        }

        Ok(result)
    }

    fn parse_internal(data: &[u8]) -> IResult<&[u8], Self> {
        nom::branch::alt((
            nom::sequence::tuple((nom::bytes::streaming::tag([b'Q']), parse_string))
                .map(|(_, query)| Self::Query { query }),
            nom::sequence::tuple((
                nom::bytes::streaming::tag([b'D']),
                nom::branch::alt((
                    nom::bytes::streaming::tag([b'P']).map(|_| DescribeKind::Portal),
                    nom::bytes::streaming::tag([b'S']).map(|_| DescribeKind::Statement),
                )),
                parse_string,
            ))
            .map(|(_, dkind, name)| Self::Describe { kind: dkind, name }),
            nom::sequence::tuple((
                nom::bytes::streaming::tag([b'E']),
                parse_string,
                nom::number::streaming::be_i32,
            ))
            .map(|(_, portal, rows)| Self::Execute {
                portal,
                max_rows: rows,
            }),
            nom::sequence::tuple((
                nom::bytes::streaming::tag("P"),
                parse_string,
                parse_string,
                nom::number::streaming::be_i16,
            ))
            .map(|(_, dest, query, type_count)| {
                assert_eq!(0, type_count);
                Self::Parse {
                    destination: dest,
                    query,
                    data_types: Vec::new(),
                }
            }),
            nom::sequence::tuple((
                nom::bytes::streaming::tag([b'B']),
                parse_string,
                parse_string,
                Self::parse_bind_parameters,
                Self::parse_bind_values,
                Self::parse_bind_format,
            ))
            .map(
                |(_, destination, statement, mut pformats, pvalues, result_formats)| {
                    if pformats.is_empty() {
                        pformats.extend((0..pvalues.len()).map(|_| FormatCode::Text));
                    } else if pformats.len() == 1 {
                        let first_format = pformats.get(0).cloned().unwrap();
                        pformats.extend((1..pvalues.len()).map(|_| first_format.clone()));
                    }

                    Self::Bind {
                        destination,
                        statement,
                        parameter_formats: Vec::new(),
                        parameter_values: pvalues,
                        result_column_format_codes: result_formats,
                    }
                },
            ),
            nom::bytes::streaming::tag([b'S']).map(|_| Self::Sync_),
            nom::bytes::streaming::tag([b'X']).map(|_| Self::Terminate),
        ))(data)
    }

    fn parse_bind_parameters(i: &[u8]) -> IResult<&[u8], Vec<FormatCode>> {
        let (mut i, count) = nom::number::streaming::be_i16(i)?;

        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            todo!()
        }

        Ok((i, result))
    }

    fn parse_bind_values(i: &[u8]) -> IResult<&[u8], Vec<Vec<u8>>> {
        let (mut i, count) = nom::number::streaming::be_i16(i)?;

        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (rem, length) = nom::number::streaming::be_i32(i)?;
            if length >= 0 {
                result.push(rem[..length as usize].to_vec());
                i = &rem[length as usize..];
            } else {
                result.push(Vec::new());
                i = rem;
            }
        }

        Ok((i, result))
    }

    fn parse_bind_format(i: &[u8]) -> IResult<&[u8], Vec<FormatCode>> {
        let (mut i, count) = nom::number::streaming::be_i16(i)?;

        let mut result = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (remaining, tmp) = nom::number::streaming::be_i16(i)?;
            let value = match tmp {
                0 => FormatCode::Text,
                1 => FormatCode::Binary,
                other => {
                    dbg!(other);
                    todo!()
                }
            };
            result.push(value);

            i = remaining;
        }

        Ok((i, result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query() {
        let (remaining, result) =
            Message::parse_internal(&[b'Q', b'S', b'E', b'L', b'E', b'C', b'T', b' ', b'1', b'\0'])
                .unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Message::Query {
                query: "SELECT 1".into()
            },
            result
        );
    }

    #[test]
    fn parse_terminate() {
        let (remaining, result) = Message::parse_internal(&[b'X']).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(Message::Terminate, result);
    }

    #[test]
    fn parse_sync() {
        let (remaining, result) = Message::parse_internal(&[b'S']).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(Message::Sync_, result);
    }

    #[test]
    fn parse_execute() {
        let (remaining, result) =
            Message::parse_internal(&[b'E', b't', b'e', b's', b't', b'\0', 0, 0, 0, 4]).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Message::Execute {
                portal: "test".into(),
                max_rows: 4
            },
            result
        );
    }

    #[test]
    fn parse_describe() {
        let (remaining, result) =
            Message::parse_internal(&[b'D', b'S', b't', b'e', b's', b't', b'\0']).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Message::Describe {
                kind: DescribeKind::Statement,
                name: "test".into()
            },
            result
        );

        let (remaining, result) =
            Message::parse_internal(&[b'D', b'P', b't', b'e', b's', b't', b'\0']).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Message::Describe {
                kind: DescribeKind::Portal,
                name: "test".into()
            },
            result
        );
    }

    #[test]
    fn parse_parse() {
        let (remaining, result) = Message::parse_internal(&[
            b'P', b'd', b'e', b's', b't', b'\0', b'S', b'E', b'L', b'E', b'C', b'T', b' ', b'1',
            b'\0', 0, 0,
        ])
        .unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Message::Parse {
                destination: "dest".into(),
                query: "SELECT 1".into(),
                data_types: Vec::new()
            },
            result
        );
    }

    #[test]
    fn parse_bind_no_parameters() {
        let (remaining, result) = Message::parse_internal(&[
            b'B', // type indicator
            b'd', b'e', b's', b't', b'\0', // destination
            b'n', b'a', b'm', b'e', b'\0', // name
            0, 0, // parameter format count
            0, 0, // parameter value count
            0, 0, // result column count
        ])
        .unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Message::Bind {
                destination: "dest".into(),
                statement: "name".into(),
                parameter_values: vec![],
                parameter_formats: vec![],
                result_column_format_codes: vec![]
            },
            result
        );
    }

    #[test]
    fn parse_bind_values() {
        let (remaining, result) = Message::parse_internal(&[
            b'B', // type indicator
            b'd', b'e', b's', b't', b'\0', // destination
            b'n', b'a', b'm', b'e', b'\0', // name
            0, 0, // parameter format count
            0, 2, // parameter value count
            0, 0, 0, 4, 1, 2, 3, 4, // First Parametere value
            0, 0, 0, 5, 5, 4, 3, 2, 1, // Second Parametere value
            0, 0, // result column count
        ])
        .unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(
            Message::Bind {
                destination: "dest".into(),
                statement: "name".into(),
                parameter_values: vec![vec![1, 2, 3, 4], vec![5, 4, 3, 2, 1]],
                parameter_formats: vec![],
                result_column_format_codes: vec![]
            },
            result
        );
    }
}
