use sql::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Data {
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Serial(u32),
    Boolean(bool),
    Char(Vec<char>),
    Varchar(Vec<char>),
    Text(String),
    Name(String),
    Timestamp(String),
    Null,
    List(Vec<Data>),
}

#[derive(Debug)]
pub enum RealizeError {
    ConvertToString(std::str::Utf8Error),
    ParseStrToInt(std::num::ParseIntError),
    NotImplemented,
}

impl Data {
    pub fn realize<'t, 'd>(
        ty: &'t sql::DataType,
        raw: &'d [u8],
    ) -> Result<Self, (RealizeError, &'t sql::DataType, &'d [u8])> {
        match ty {
            sql::DataType::Name => {
                let name = core::str::from_utf8(raw)
                    .map(|s| s.to_owned())
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;
                Ok(Self::Name(name))
            }
            sql::DataType::Text => {
                let name = core::str::from_utf8(raw)
                    .map(|s| s.to_owned())
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;
                Ok(Self::Text(name))
            }
            sql::DataType::VarChar { size } => {
                let name: Vec<_> = core::str::from_utf8(raw)
                    .map(|s| s.chars().collect())
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;

                if name.len() > *size {
                    return Err((RealizeError::NotImplemented, ty, raw));
                }

                Ok(Self::Varchar(name))
            }
            sql::DataType::Char { size } => {
                let name: Vec<_> = core::str::from_utf8(raw)
                    .map(|s| s.chars().collect())
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;

                if name.len() > *size {
                    return Err((RealizeError::NotImplemented, ty, raw));
                }

                Ok(Self::Char(name))
            }
            sql::DataType::Bool => {
                let result = if raw.len() == 1 {
                    dbg!(&raw);
                    false
                } else {
                    let name = core::str::from_utf8(raw)
                        .map(|s| s.to_owned())
                        .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;

                    name.eq_ignore_ascii_case("true")
                        || name.eq_ignore_ascii_case("yes")
                        || name.eq_ignore_ascii_case("on")
                };

                Ok(Self::Boolean(result))
            }
            sql::DataType::Timestamp => {
                let name = core::str::from_utf8(raw)
                    .map(|s| s.to_owned())
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;

                Ok(Self::Timestamp(name))
            }
            sql::DataType::SmallInteger => {
                let raw_str = core::str::from_utf8(raw)
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;
                let val = raw_str
                    .parse::<i16>()
                    .map_err(|e| (RealizeError::ParseStrToInt(e), ty, raw))?;
                Ok(Self::SmallInt(val))
            }
            sql::DataType::Integer => {
                let raw_str = core::str::from_utf8(raw)
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;
                let val = raw_str
                    .parse::<i32>()
                    .map_err(|e| (RealizeError::ParseStrToInt(e), ty, raw))?;
                Ok(Self::Integer(val))
            }
            sql::DataType::BigInteger => {
                let raw_str = core::str::from_utf8(raw)
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;
                let val = raw_str
                    .parse::<i64>()
                    .map_err(|e| (RealizeError::ParseStrToInt(e), ty, raw))?;
                Ok(Self::BigInt(val))
            }
            sql::DataType::Serial => {
                let raw_str = core::str::from_utf8(raw)
                    .map_err(|e| (RealizeError::ConvertToString(e), ty, raw))?;
                let val = raw_str
                    .parse::<u32>()
                    .map_err(|e| (RealizeError::ParseStrToInt(e), ty, raw))?;
                Ok(Self::Serial(val))
            }
            other => {
                dbg!(other, raw);
                Err((RealizeError::NotImplemented, ty, raw))
            }
        }
    }

    pub fn from_literal(lit: &sql::Literal<'_>) -> Self {
        match lit {
            sql::Literal::Null => Self::Null,
            sql::Literal::Str(s) => Self::Text(s.to_string()),
            sql::Literal::Name(n) => Self::Name(n.to_string()),
            sql::Literal::SmallInteger(v) => Self::SmallInt(*v),
            sql::Literal::Integer(v) => Self::Integer(*v),
            sql::Literal::BigInteger(v) => Self::BigInt(*v),
            sql::Literal::Bool(b) => Self::Boolean(*b),
        }
    }

    pub fn as_null(ty: &DataType) -> Self {
        match ty {
            DataType::Serial => Self::Serial(0),
            DataType::BigSerial => todo!(),
            DataType::SmallInteger => Self::SmallInt(0),
            DataType::Integer => Self::Integer(0),
            DataType::BigInteger => Self::BigInt(0),
            DataType::Bool => Self::Boolean(false),
            DataType::Name => Self::Null,
            DataType::Text => Self::Null,
            DataType::VarChar { .. } => Self::Null,
            DataType::Char { .. } => Self::Null,
            DataType::Timestamp => Self::Null,
            DataType::ByteA => Self::Null,
            DataType::DoublePrecision => todo!(),
            DataType::Real => todo!(),
        }
    }

    pub fn try_cast(self, target: &sql::DataType) -> Result<Self, (Self, &sql::DataType)> {
        match (self, target) {
            (Self::Serial(s), sql::DataType::Serial) => Ok(Self::Serial(s)),
            (Self::Serial(s), sql::DataType::BigInteger) => Ok(Self::BigInt(s as i64)),
            (Self::Name(v), sql::DataType::Name) => Ok(Self::Name(v)),
            (Self::Varchar(d), sql::DataType::VarChar { .. }) => Ok(Self::Varchar(d)),
            (Self::Varchar(d), sql::DataType::Text) => Ok(Self::Text(d.into_iter().collect())),
            (Self::Text(d), sql::DataType::Text) => Ok(Self::Text(d)),
            (Self::Text(d), sql::DataType::VarChar { .. }) => {
                Ok(Self::Varchar(d.chars().collect()))
            }
            (Self::Text(d), sql::DataType::BigInteger) => {
                let val = d.parse::<i64>().map_err(|_| (Self::Text(d), target))?;
                Ok(Self::BigInt(val))
            }
            (Self::Text(d), sql::DataType::Integer) => {
                let val = d.parse::<i32>().map_err(|_| (Self::Text(d), target))?;
                Ok(Self::Integer(val))
            }
            (Self::Text(d), sql::DataType::SmallInteger) => {
                let val = d.parse::<i16>().map_err(|_| (Self::Text(d), target))?;
                Ok(Self::SmallInt(val))
            }
            (Self::Text(d), sql::DataType::Timestamp) => {
                // TODO
                // Validate

                Ok(Self::Timestamp(d))
            }
            (Self::SmallInt(d), sql::DataType::SmallInteger) => Ok(Self::SmallInt(d)),
            (Self::SmallInt(d), sql::DataType::Integer) => Ok(Self::Integer(d as i32)),
            (Self::SmallInt(d), sql::DataType::BigInteger) => Ok(Self::BigInt(d as i64)),
            (Self::Integer(d), sql::DataType::Integer) => Ok(Self::Integer(d)),
            (Self::Integer(d), sql::DataType::BigInteger) => Ok(Self::BigInt(d as i64)),
            (Self::BigInt(d), sql::DataType::BigInteger) => Ok(Self::BigInt(d)),
            (Self::Boolean(b), sql::DataType::Bool) => Ok(Self::Boolean(b)),
            (Self::Timestamp(d), sql::DataType::Timestamp) => Ok(Self::Timestamp(d)),
            (v, _) => Err((v, target)),
        }
    }
}

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::SmallInt(v1), Self::SmallInt(v2)) => Some(v1.cmp(v2)),
            (Self::Integer(v1), Self::Integer(v2)) => Some(v1.cmp(v2)),
            (Self::BigInt(v1), Self::BigInt(v2)) => Some(v1.cmp(v2)),
            (Self::Serial(v1), Self::Serial(v2)) => Some(v1.cmp(v2)),
            (Self::Boolean(v1), Self::Boolean(v2)) => Some(v1.cmp(v2)),
            (Self::Char(v1), Self::Char(v2)) => Some(v1.cmp(v2)),
            (Self::Varchar(v1), Self::Varchar(v2)) => Some(v1.cmp(v2)),
            (Self::Text(v1), Self::Text(v2)) => Some(v1.cmp(v2)),
            (Self::Name(v1), Self::Name(v2)) => Some(v1.cmp(v2)),
            (Self::Timestamp(v1), Self::Timestamp(v2)) => Some(v1.cmp(v2)),
            (Self::List(v1), Self::List(v2)) => v1.partial_cmp(v2),
            _ => None,
        }
    }
}
