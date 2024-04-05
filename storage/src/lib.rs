#![feature(alloc_layout_extra)]

mod inmemory;

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
