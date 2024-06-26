//! [Reference](https://www.postgresql.org/docs/16/sql.html)
#![deny(warnings)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::todo)]

use nom::IResult;

pub mod arenas;

mod common;
pub use common::{
    AggregateExpression, BinaryOperator, ColumnReference, DataType, FunctionCall, Identifier,
    Literal, TypeModifier, ValueExpression,
};

mod create;
pub use create::{CreateIndex, CreateSequence, CreateTable, TableField};

mod alter;
pub use alter::AlterTable;

mod select;
pub use select::{
    Combination, GroupAttribute, JoinKind, NullOrdering, OrderAttribute, OrderBy, Ordering, Select,
    SelectLimit, TableExpression,
};

mod insert;
pub use insert::{ConflictHandling, Insert, InsertValues};

mod update;
pub use update::{Update, UpdateFrom};

mod delete;
pub use delete::Delete;

mod condition;
pub use condition::Condition;

mod transactions;
pub use transactions::{AbortTransaction, BeginTransaction, CommitTransaction, IsolationMode};

mod drop;
pub use drop::{DependentHandling, DropIndex, DropTable};

mod truncate;
pub use truncate::TruncateTable;

mod with_cte;
use with_cte::with_ctes;
pub use with_cte::{WithCTE, WithCTEs};

mod set_config;
pub use set_config::Configuration;

mod prepare;
pub use prepare::Prepare;

mod copy_;
pub use copy_::Copy_;

mod vacuum;
pub use vacuum::Vacuum;

pub(crate) mod nom_util;

pub mod dialects {
    /// Indicates that the parser should be compatible with the Postgres Dialect of SQL
    pub struct Postgres;
}

/// Should be implemented by parser to indicate that it is compatible with the given dialect `D`
pub trait CompatibleParser {
    type StaticVersion: 'static;

    fn to_static(&self) -> Self::StaticVersion;

    fn parameter_count(&self) -> usize;
}

pub trait Parser<'i>: Sized {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>>;
}

pub trait ArenaParser<'i, 'a>: Sized + CompatibleParser {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>>;
}

#[derive(Debug, PartialEq)]
pub enum Query<'s, 'a> {
    WithCTE {
        cte: WithCTEs<'s, 'a>,
        query: crate::arenas::Boxed<'a, Self>,
    },
    Prepare(prepare::Prepare<'s, 'a>),
    Select(select::Select<'s, 'a>),
    Insert(insert::Insert<'s, 'a>),
    Update(update::Update<'s, 'a>),
    Copy_(copy_::Copy_<'s>),
    Delete(delete::Delete<'s, 'a>),
    CreateTable(create::CreateTable<'s, 'a>),
    CreateIndex(create::CreateIndex<'s, 'a>),
    CreateSequence(arenas::Boxed<'a, create::CreateSequence<'s>>),
    AlterTable(alter::AlterTable<'s, 'a>),
    DropIndex(drop::DropIndex<'s>),
    DropTable(drop::DropTable<'s, 'a>),
    TruncateTable(truncate::TruncateTable<'s, 'a>),
    Configuration(set_config::Configuration),
    BeginTransaction(transactions::IsolationMode),
    CommitTransaction,
    RollbackTransaction,
    Vacuum(vacuum::Vacuum),
}

#[derive(Debug)]
pub enum ParseQueryError<'i> {
    NotEverythingWasParsed {
        remaining: &'i [u8],
    },
    ParserError {
        error: nom::Err<nom::error::VerboseError<&'i [u8]>>,
    },
    Other,
}

impl<'i, 'a> ArenaParser<'i, 'a> for Query<'i, 'a> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| nom::combinator::complete(move |i| query(i, a))(i)
    }
}

impl<'s, 'a> CompatibleParser for Query<'s, 'a> {
    type StaticVersion = Query<'static, 'static>;

    fn parameter_count(&self) -> usize {
        match self {
            Self::WithCTE { cte, query } => {
                core::cmp::max(cte.parameter_count(), query.parameter_count())
            }
            Self::Prepare(p) => p.params.len(),
            Self::Select(s) => s.parameter_count(),
            Self::Insert(i) => i.parameter_count(),
            Self::Update(u) => u.parameter_count(),
            Self::Copy_(c) => c.parameter_count(),
            Self::Delete(d) => d.max_parameter(),
            Self::CreateTable(_) => 0,
            Self::CreateIndex(_) => 0,
            Self::CreateSequence(_) => 0,
            Self::AlterTable(_) => 0,
            Self::DropIndex(_) => 0,
            Self::TruncateTable(_) => 0,
            Self::DropTable(_) => 0,
            Self::Configuration(_) => 0,
            Self::BeginTransaction(_) => 0,
            Self::CommitTransaction => 0,
            Self::RollbackTransaction => 0,
            Self::Vacuum(_) => 0,
        }
    }

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::WithCTE { cte, query } => Query::WithCTE {
                cte: cte.to_static(),
                query: query.to_static(),
            },
            Self::Prepare(prepare) => Query::Prepare(prepare.to_static()),
            Self::Select(s) => Query::Select(s.to_static()),
            Self::Insert(ins) => Query::Insert(ins.to_static()),
            Self::Update(ups) => Query::Update(ups.to_static()),
            Self::Copy_(c) => Query::Copy_(c.to_static()),
            Self::Delete(d) => Query::Delete(d.to_static()),
            Self::CreateTable(ct) => Query::CreateTable(ct.to_static()),
            Self::CreateIndex(ci) => Query::CreateIndex(ci.to_static()),
            Self::CreateSequence(cs) => Query::CreateSequence(cs.to_static()),
            Self::AlterTable(at) => Query::AlterTable(at.to_static()),
            Self::DropIndex(di) => Query::DropIndex(di.to_static()),
            Self::DropTable(dt) => Query::DropTable(dt.to_static()),
            Self::TruncateTable(t) => Query::TruncateTable(t.to_static()),
            Self::Configuration(c) => Query::Configuration(c.clone()),
            Self::BeginTransaction(iso) => Query::BeginTransaction(iso.clone()),
            Self::CommitTransaction => Query::CommitTransaction,
            Self::RollbackTransaction => Query::RollbackTransaction,
            Self::Vacuum(v) => Query::Vacuum(v.to_static()),
        }
    }
}

impl<'i, 'a> ArenaParser<'i, 'a> for arenas::Vec<'a, Query<'i, 'a>> {
    fn parse_arena(
        a: &'a bumpalo::Bump,
    ) -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>> {
        move |i| crate::nom_util::bump_many1(a, move |i| query(i, a))(i)
    }
}

impl<'s, 'a> Query<'s, 'a> {
    pub fn parse<'r, 'outer_a>(
        raw: &'r [u8],
        arena: &'outer_a bumpalo::Bump,
    ) -> Result<Self, ParseQueryError<'s>>
    where
        'r: 's,
        'outer_a: 'a,
    {
        let (remaining, query) = Query::parse_arena(arena)(raw)
            .map_err(|e| ParseQueryError::ParserError { error: e })?;

        if !remaining.is_empty() {
            return Err(ParseQueryError::NotEverythingWasParsed { remaining });
        }

        Ok(query)
    }

    pub fn parse_multiple<'r, 'outer_a>(
        raw: &'r [u8],
        arena: &'outer_a bumpalo::Bump,
    ) -> Result<crate::arenas::Vec<'outer_a, Self>, ParseQueryError<'s>>
    where
        'r: 's,
        'outer_a: 'a,
    {
        let (remaining, queries) =
            crate::nom_util::bump_many1(arena, move |i| query(i, arena))(raw)
                .map_err(|e| ParseQueryError::ParserError { error: e })?;

        if !remaining.is_empty() {
            return Err(ParseQueryError::NotEverythingWasParsed { remaining });
        }

        Ok(queries)
    }
}

fn query<'i, 'a>(
    raw: &'i [u8],
    arena: &'a bumpalo::Bump,
) -> IResult<&'i [u8], Query<'i, 'a>, nom::error::VerboseError<&'i [u8]>> {
    let (raw, ctes) = nom::combinator::opt(move |i| with_ctes(i, arena))(raw)?;

    let (rem, inner) = nom::combinator::map(
        nom::sequence::tuple((
            nom::character::complete::multispace0,
            nom::branch::alt((
                nom::combinator::map(Select::parse_arena(arena), Query::Select),
                nom::combinator::map(Insert::parse_arena(arena), Query::Insert),
                nom::combinator::map(Update::parse_arena(arena), Query::Update),
                nom::combinator::map(Copy_::parse(), Query::Copy_),
                nom::combinator::map(Delete::parse_arena(arena), Query::Delete),
                nom::combinator::map(BeginTransaction::parse(), |iso| {
                    Query::BeginTransaction(iso.isolation)
                }),
                nom::combinator::map(CommitTransaction::parse(), |_| Query::CommitTransaction),
                nom::combinator::map(AbortTransaction::parse(), |_| Query::RollbackTransaction),
                nom::combinator::map(CreateTable::parse_arena(arena), Query::CreateTable),
                nom::combinator::map(CreateIndex::parse_arena(arena), Query::CreateIndex),
                nom::combinator::map(CreateSequence::parse_arena(arena), |cs| {
                    Query::CreateSequence(arenas::Boxed::arena(arena, cs))
                }),
                nom::combinator::map(AlterTable::parse_arena(arena), Query::AlterTable),
                nom::combinator::map(DropIndex::parse(), Query::DropIndex),
                nom::combinator::map(DropTable::parse_arena(arena), Query::DropTable),
                nom::combinator::map(TruncateTable::parse_arena(arena), Query::TruncateTable),
                nom::combinator::map(Configuration::parse(), Query::Configuration),
                nom::combinator::map(Prepare::parse_arena(arena), Query::Prepare),
                nom::combinator::map(Vacuum::parse(), Query::Vacuum),
            )),
            nom::character::complete::multispace0,
            nom::combinator::opt(nom::bytes::complete::tag(";")),
            nom::character::complete::multispace0,
        )),
        |(_, q, _, _, _)| q,
    )(raw)?;

    let result = match ctes {
        Some(cte) => Query::WithCTE {
            cte,
            query: crate::arenas::Boxed::arena(arena, inner),
        },
        None => inner,
    };

    Ok((rem, result))
}

#[cfg(test)]
pub(crate) mod macros {
    macro_rules! arena_parser_parse {
        ($target_ty:ty, $input:expr) => {{
            use crate::ArenaParser as _;
            let arena = bumpalo::Bump::new();
            {
                let (remaining, _) = <$target_ty>::parse_arena(&arena)($input.as_bytes()).unwrap();
                assert_eq!(
                    &[] as &[u8],
                    remaining,
                    "{:?}",
                    core::str::from_utf8(remaining).unwrap()
                );
            }
        }};
        ($target_ty:ty, $input:expr, $expected:expr) => {
            arena_parser_parse!($target_ty, $input, $expected, &[])
        };
        ($target_ty:ty, $input:expr, $expected:expr, $remaining:expr) => {{
            use crate::ArenaParser as _;
            let arena = bumpalo::Bump::new();
            let (remaining, result) = <$target_ty>::parse_arena(&arena)($input.as_bytes()).unwrap();
            assert_eq!(
                $remaining as &[u8],
                remaining,
                "{:?}",
                core::str::from_utf8(remaining).unwrap()
            );
            assert_eq!(<$target_ty>::from($expected), result.to_static());
        }};
    }
    pub(crate) use arena_parser_parse;

    macro_rules! arena_parser_parse_err {
        ($target_ty:ty, $input:expr) => {{
            use crate::ArenaParser as _;
            let arena = bumpalo::Bump::new();
            let _tmp = <$target_ty>::parse_arena(&arena)($input.as_bytes()).unwrap_err();
        }};
    }
    pub(crate) use arena_parser_parse_err;

    macro_rules! parser_parse {
        ($target_ty:ty, $input:expr) => {{
            use crate::Parser as _;
            let (remaining, _) = <$target_ty>::parse()($input.as_bytes()).unwrap();
            assert_eq!(
                &[] as &[u8],
                remaining,
                "{:?}",
                core::str::from_utf8(remaining).unwrap()
            );
        }};
        ($target_ty:ty, $input:expr, $expected:expr) => {
            parser_parse!($target_ty, $input, $expected, &[])
        };
        ($target_ty:ty, $input:expr, $expected:expr, $remaining:expr) => {{
            use crate::Parser as _;
            let (remaining, result) = <$target_ty>::parse()($input.as_bytes()).unwrap();
            assert_eq!(
                $remaining as &[u8],
                remaining,
                "{:?}",
                core::str::from_utf8(remaining).unwrap()
            );
            assert_eq!($expected, result);
        }};
    }
    pub(crate) use parser_parse;
}

#[cfg(test)]
mod tests {
    use crate::{
        arenas::Boxed,
        macros::arena_parser_parse,
        select::{GroupAttribute, OrderAttribute},
    };

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn select_with_where() {
        arena_parser_parse!(Query, "select * from table where first = 'value'");
    }

    #[test]
    fn select_with_where_multiple() {
        arena_parser_parse!(
            Query,
            "select * from table where first = 'value' AND second = 'other'"
        );
    }

    #[test]
    fn select_test() {
        arena_parser_parse!(
            Query,
            "SELECT \"id\", \"migration_id\", \"sql\", \"success\", \"error\", \"timestamp\" FROM \"migration_log\"",
            Query::Select(Select {
                values: vec![
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "id".into(),
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "migration_id".into(),
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "sql".into()
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "success".into()
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "error".into()
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: "timestamp".into()
                    })
                ].into(),
                table: Some(TableExpression::Relation("migration_log".into())),
                combine:None,
                limit:None,
                order_by:None,
                having:None,
                group_by:None,
                for_update:None,
                where_condition:None,
            })
        );
    }

    #[test]
    fn create_table() {
        arena_parser_parse!(
            Query,
            "CREATE TABLE IF NOT EXISTS \"migration_log\" (\n\"id\" SERIAL PRIMARY KEY  NOT NULL\n, \"migration_id\" VARCHAR(255) NOT NULL\n, \"sql\" TEXT NOT NULL\n, \"success\" BOOL NOT NULL\n, \"error\" TEXT NOT NULL\n, \"timestamp\" TIMESTAMP NOT NULL\n);"
        );
        arena_parser_parse!(
            Query,
            "CREATE TABLE IF NOT EXISTS \"user\" (\n\"id\" SERIAL PRIMARY KEY  NOT NULL\n, \"version\" INTEGER NOT NULL\n, \"login\" VARCHAR(190) NOT NULL\n, \"email\" VARCHAR(190) NOT NULL\n, \"name\" VARCHAR(255) NULL\n, \"password\" VARCHAR(255) NULL\n, \"salt\" VARCHAR(50) NULL\n, \"rands\" VARCHAR(50) NULL\n, \"company\" VARCHAR(255) NULL\n, \"account_id\" BIGINT NOT NULL\n, \"is_admin\" BOOL NOT NULL\n, \"created\" TIMESTAMP NOT NULL\n, \"updated\" TIMESTAMP NOT NULL\n);"
        );
    }

    #[test]
    fn insert_query() {
        arena_parser_parse!(
            Query,
            "INSERT INTO \"migration_log\" (\"migration_id\",\"sql\",\"success\",\"error\",\"timestamp\") VALUES ($1, $2, $3, $4, $5) RETURNING \"id\""
        );
    }

    #[test]
    fn alter_table() {
        arena_parser_parse!(Query, "ALTER TABLE \"user\" RENAME TO \"user_v1\"");
    }

    #[test]
    fn insert_with_select() {
        arena_parser_parse!(
            Query,
            "INSERT INTO \"user\" (\"name\"\n, \"company\"\n, \"org_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\") SELECT \"name\"\n, \"company\"\n, \"account_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\" FROM \"user_v1\""
        );
    }

    #[test]
    fn drop_index() {
        arena_parser_parse!(Query, "DROP INDEX \"UQE_user_login\" CASCADE");
    }

    #[test]
    fn multiple_queries() {
        arena_parser_parse!(
            crate::arenas::Vec<'_, Query>,
            "ALTER TABLE alert_rule ALTER COLUMN is_paused SET DEFAULT false;\nUPDATE alert_rule SET is_paused = false;"
        );
    }

    #[test]
    fn max_parameter_select() {
        arena_parser_parse!(
            Query,
            "SELECT \"id\", \"role_id\", \"action\", \"scope\", \"kind\", \"attribute\", \"identifier\", \"updated\", \"created\" FROM \"permission\" WHERE \"action\" IN ($1,$2,$3,$4)"
        );
    }

    #[test]
    fn select_rename_table() {
        arena_parser_parse!(
            Query,
            "SELECT p.name FROM permission AS p",
            Query::Select(Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: Some(Identifier("p".into())),
                    column: Identifier("name".into())
                })]
                .into(),
                table: Some(TableExpression::Renamed {
                    inner: Boxed::new(TableExpression::Relation(Identifier("permission".into()))),
                    name: Identifier("p".into()),
                    column_rename: None,
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None,
            })
        );
    }

    #[test]
    fn select_rename_attribute() {
        arena_parser_parse!(
            Query,
            "SELECT name AS pname FROM permission",
            Query::Select(Select {
                values: vec![ValueExpression::Renamed {
                    inner: Boxed::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("name".into())
                    })),
                    name: Identifier("pname".into())
                }]
                .into(),
                table: Some(TableExpression::Relation(Identifier("permission".into()))),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            })
        );
    }

    #[test]
    fn select_with_basic_join() {
        arena_parser_parse!(
            Query,
            "SELECT user.name, password.hash FROM user JOIN password ON user.id = password.uid",
            Query::Select(Select {
                values: vec![
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("user".into())),
                        column: Identifier("name".into())
                    }),
                    ValueExpression::ColumnReference(ColumnReference {
                        relation: Some(Identifier("password".into())),
                        column: Identifier("hash".into())
                    })
                ]
                .into(),
                table: Some(TableExpression::Join {
                    left: Boxed::new(TableExpression::Relation(Identifier("user".into()))),
                    right: Boxed::new(TableExpression::Relation(Identifier("password".into()))),
                    kind: JoinKind::Inner,
                    condition: Condition::And(
                        vec![Condition::Value(
                            Box::new(ValueExpression::Operator {
                                first: Boxed::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: Some(Identifier("user".into())),
                                        column: Identifier("id".into()),
                                    }
                                )),
                                second: Boxed::new(ValueExpression::ColumnReference(
                                    ColumnReference {
                                        relation: Some(Identifier("password".into())),
                                        column: Identifier("uid".into())
                                    }
                                )),
                                operator: BinaryOperator::Equal
                            })
                            .into()
                        )]
                        .into()
                    ),
                    lateral: false,
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            })
        );
    }

    #[test]
    fn select_with_renaming_table() {
        arena_parser_parse!(
            Query,
            "SELECT r.name as role_name, r.id as role_id, r.org_id as org_id,p.action, p.scope\n\tFROM permission AS p\n\tINNER JOIN role AS r ON p.role_id = r.id\n\tWHERE r.name LIKE $1"
        );
    }

    #[test]
    fn select_large() {
        arena_parser_parse!(
            Query,
            "SELECT \"id\", \"role_id\", \"action\", \"scope\", \"kind\", \"attribute\", \"identifier\", \"updated\", \"created\" FROM \"permission\" WHERE ( action IN ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16))"
        );
    }

    #[test]
    fn select_with_order_by() {
        arena_parser_parse!(
            Query,
            "SELECT \"id\" FROM \"alert_rule\" ORDER BY \"id\" ASC",
            Query::Select(Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                })]
                .into(),
                table: Some(TableExpression::Relation(Identifier("alert_rule".into()))),
                where_condition: None,
                order_by: Some(
                    vec![Ordering {
                        column: OrderAttribute::ColumnRef(ColumnReference {
                            relation: None,
                            column: Identifier("id".into())
                        }),
                        order: OrderBy::Ascending,
                        nulls: NullOrdering::Last
                    }]
                    .into()
                ),
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            })
        );
    }

    #[test]
    fn select_with_group_by() {
        arena_parser_parse!(
            Query,
            "SELECT \"id\" FROM \"alert_rule\" GROUP BY \"id\"",
            Query::Select(Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                })]
                .into(),
                table: Some(TableExpression::Relation(Identifier("alert_rule".into()))),
                where_condition: None,
                order_by: None,
                group_by: Some(
                    vec![GroupAttribute::ColumnRef(ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    })]
                    .into()
                ),
                having: None,
                limit: None,
                for_update: None,
                combine: None
            })
        );
    }

    #[test]
    fn select_complex() {
        arena_parser_parse!(
            Query,
            "SELECT dashboard.id, dashboard.uid, dashboard.is_folder, dashboard.org_id, count(dashboard_acl.id) as count\n\t\t  FROM dashboard\n\t\t\t\tLEFT JOIN dashboard_acl ON dashboard.id = dashboard_acl.dashboard_id\n\t\t  WHERE dashboard.has_acl IS TRUE\n\t\t  GROUP BY dashboard.id"
        );
    }

    #[test]
    fn select_complex2() {
        arena_parser_parse!(
            Query,
            "\n\tSELECT res.uid, res.is_folder, res.org_id\n\tFROM (SELECT dashboard.id, dashboard.uid, dashboard.is_folder, dashboard.org_id, count(dashboard_acl.id) as count\n\t\t  FROM dashboard\n\t\t\t\tLEFT JOIN dashboard_acl ON dashboard.id = dashboard_acl.dashboard_id\n\t\t  WHERE dashboard.has_acl IS TRUE\n\t\t  GROUP BY dashboard.id) as res\n\tWHERE res.count = 0\n\t"
        );
    }
}
