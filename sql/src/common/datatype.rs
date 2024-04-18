use nom::{IResult, Parser};

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Name,
    Serial,
    BigSerial,
    VarChar { size: usize },
    Char { size: usize },
    Text,
    Bool,
    Timestamp,
    SmallInteger,
    Integer,
    BigInteger,
    ByteA,
    DoublePrecision,
    Real,
}

impl crate::Parser for DataType {
    fn parse<'i>() -> impl Fn(&'i[u8]) -> IResult<&'i [u8], DataType, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            // Only for the current implementation
            #[allow(deprecated)]
            data_type(i)
        }
    }
}

#[deprecated]
pub fn data_type(i: &[u8]) -> IResult<&[u8], DataType, nom::error::VerboseError<&[u8]>> {
    nom::branch::alt((
        nom::bytes::complete::tag_no_case("SERIAL").map(|_| DataType::Serial),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("VARCHAR"),
            nom::bytes::complete::tag("("),
            nom::character::complete::digit1,
            nom::bytes::complete::tag(")"),
        ))
        .map(|(_, _, raw_size, _)| DataType::VarChar {
            size: core::str::from_utf8(raw_size).unwrap().parse().unwrap(),
        }),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("char"),
            nom::bytes::complete::tag("("),
            nom::character::complete::digit1,
            nom::bytes::complete::tag(")"),
        ))
        .map(|(_, _, raw_size, _)| DataType::Char {
            size: core::str::from_utf8(raw_size).unwrap().parse().unwrap(),
        }),
        nom::bytes::complete::tag_no_case("TEXT").map(|_| DataType::Text),
        nom::bytes::complete::tag_no_case("BOOL").map(|_| DataType::Bool),
        nom::bytes::complete::tag_no_case("TIMESTAMP").map(|_| DataType::Timestamp),
        nom::bytes::complete::tag_no_case("SMALLINT").map(|_| DataType::SmallInteger),
        nom::bytes::complete::tag_no_case("INTEGER").map(|_| DataType::Integer),
        nom::bytes::complete::tag_no_case("INT").map(|_| DataType::Integer),
        nom::bytes::complete::tag_no_case("BIGINT").map(|_| DataType::BigInteger),
        nom::bytes::complete::tag_no_case("BYTEA").map(|_| DataType::ByteA),
        nom::bytes::complete::tag_no_case("Double Precision").map(|_| DataType::DoublePrecision),
        nom::bytes::complete::tag_no_case("Real").map(|_| DataType::Real),
    ))(i)
}

impl DataType {
    pub fn size(&self) -> i16 {
        match self {
            Self::Name => -1,
            Self::VarChar { .. } => -1,
            Self::Char { .. } => -1,
            Self::ByteA => -1,
            Self::Text => -1,
            Self::Timestamp => -1,
            Self::SmallInteger => 2,
            Self::Serial => 4,
            Self::Integer => 4,
            Self::BigInteger => 8,
            Self::BigSerial => 8,
            Self::Bool => 1,
            Self::Real => todo!(),
            Self::DoublePrecision => todo!(),
        }
    }

    /// [Reference](https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat)
    pub fn type_oid(&self) -> i32 {
        match self {
            Self::Bool => 16,
            Self::Name => 19,
            Self::BigInteger => 20,
            Self::SmallInteger => 21,
            Self::Serial | Self::Integer => 23,
            Self::Text => 25,
            Self::Char { .. } => 1042,
            Self::VarChar { .. } => 1043,
            Self::Timestamp => 1114,
            other => todo!("{:?}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datatypes() {
        let (remaining, dtype) = data_type("SERIAL".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Serial, dtype);

        let (remaining, dtype) = data_type("VARCHAR(123)".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::VarChar { size: 123 }, dtype);

        let (remaining, dtype) = data_type("TEXT".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Text, dtype);

        let (remaining, dtype) = data_type("BOOL".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Bool, dtype);

        let (remaining, dtype) = data_type("TIMESTAMP".as_bytes()).unwrap();
        assert_eq!(
            &[] as &[u8],
            remaining,
            "{:?}",
            core::str::from_utf8(remaining)
        );
        assert_eq!(DataType::Timestamp, dtype);
    }
}
