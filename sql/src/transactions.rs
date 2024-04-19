use nom::{IResult, Parser};

pub struct BeginTransaction {
    pub isolation: IsolationMode,
}

pub struct CommitTransaction {
}

pub struct AbortTransaction {
}

#[derive(Debug, PartialEq, Clone)]
pub enum IsolationMode {
    Standard,
    ReadWrite,
    ReadOnly,
    Deferrable,
    NotDeferrable,
    Serializable,
    RepeatableRead,
    ReadCommitted,
    ReadUncommitted,
}

impl<'i> crate::Parser<'i> for BeginTransaction {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            begin_transaction(i).map(|(first, im)| (first, BeginTransaction {
                isolation: im,
            }))
        }
    }
}

/// [Reference](https://www.postgresql.org/docs/current/sql-begin.html)
pub fn begin_transaction(
    i: &[u8],
) -> IResult<&[u8], IsolationMode, nom::error::VerboseError<&[u8]>> {
    let (remaining, (_, _, isomode)) = nom::sequence::tuple((
        nom::bytes::complete::tag_no_case("BEGIN"),
        nom::combinator::opt(nom::sequence::tuple((
            nom::character::complete::multispace1,
            nom::branch::alt((
                nom::bytes::complete::tag_no_case("WORK"),
                nom::bytes::complete::tag_no_case("TRANSACTION"),
            )),
        ))),
        nom::branch::alt((
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::branch::alt((
                    nom::bytes::complete::tag_no_case("READ WRITE")
                        .map(|_| IsolationMode::ReadWrite),
                    nom::bytes::complete::tag_no_case("READ ONLY").map(|_| IsolationMode::ReadOnly),
                )),
            ))
            .map(|(_, i)| i),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::bytes::complete::tag("ISOLATION LEVEL"),
                nom::character::complete::multispace1,
                nom::branch::alt((
                    nom::bytes::complete::tag_no_case("SERIALIZABLE")
                        .map(|_| IsolationMode::Serializable),
                    nom::bytes::complete::tag_no_case("REPEATABLE READ")
                        .map(|_| IsolationMode::RepeatableRead),
                    nom::bytes::complete::tag_no_case("READ COMMITTED")
                        .map(|_| IsolationMode::ReadCommitted),
                    nom::bytes::complete::tag_no_case("READ UNCOMMITTED")
                        .map(|_| IsolationMode::ReadUncommitted),
                )),
            ))
            .map(|(_, _, _, i)| i),
            nom::sequence::tuple((
                nom::character::complete::multispace1,
                nom::branch::alt((
                    nom::bytes::complete::tag_no_case("DEFERRABLE")
                        .map(|_| IsolationMode::Deferrable),
                    nom::bytes::complete::tag_no_case("NOT DEFERRABLE")
                        .map(|_| IsolationMode::NotDeferrable),
                )),
            ))
            .map(|(_, i)| i),
            nom::combinator::success(IsolationMode::Standard),
        )),
    ))(i)?;

    Ok((remaining, isomode))
}

impl<'i> crate::Parser<'i> for CommitTransaction {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            commit_transaction(i).map(|(f, _)| (f, CommitTransaction {}))
        }
    }
}

pub fn commit_transaction(i: &[u8]) -> IResult<&[u8], (), nom::error::VerboseError<&[u8]>> {
    let (remaining, _) = nom::branch::alt((
        nom::bytes::complete::tag_no_case("COMMIT"),
        // This is a Postgres Extension
        nom::bytes::complete::tag_no_case("END"),
    ))(i)?;

    Ok((remaining, ()))
}

impl<'i> crate::Parser<'i> for AbortTransaction {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| {
            rollback_transaction(i).map(|(f, _)| (f, AbortTransaction{}))
        }
    }
}

pub fn rollback_transaction(i: &[u8]) -> IResult<&[u8], (), nom::error::VerboseError<&[u8]>> {
    let (remaining, _) = nom::bytes::complete::tag_no_case("ROLLBACK")(i)?;
    Ok((remaining, ()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_read_write() {
        let (_, level) = begin_transaction("BEGIN".as_bytes()).unwrap();
        assert_eq!(IsolationMode::Standard, level);

        let (_, level) = begin_transaction("BEGIN READ WRITE".as_bytes()).unwrap();
        assert_eq!(IsolationMode::ReadWrite, level);

        let (_, level) = begin_transaction("BEGIN READ ONLY".as_bytes()).unwrap();
        assert_eq!(IsolationMode::ReadOnly, level);

        let (_, level) =
            begin_transaction("BEGIN ISOLATION LEVEL SERIALIZABLE".as_bytes()).unwrap();
        assert_eq!(IsolationMode::Serializable, level);

        let (_, level) =
            begin_transaction("BEGIN ISOLATION LEVEL REPEATABLE READ".as_bytes()).unwrap();
        assert_eq!(IsolationMode::RepeatableRead, level);

        let (_, level) =
            begin_transaction("BEGIN ISOLATION LEVEL READ COMMITTED".as_bytes()).unwrap();
        assert_eq!(IsolationMode::ReadCommitted, level);

        let (_, level) =
            begin_transaction("BEGIN ISOLATION LEVEL READ UNCOMMITTED".as_bytes()).unwrap();
        assert_eq!(IsolationMode::ReadUncommitted, level);

        let (_, level) = begin_transaction("BEGIN DEFERRABLE".as_bytes()).unwrap();
        assert_eq!(IsolationMode::Deferrable, level);

        let (_, level) = begin_transaction("BEGIN NOT DEFERRABLE".as_bytes()).unwrap();
        assert_eq!(IsolationMode::NotDeferrable, level);
    }
}
