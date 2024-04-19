//! [Reference](https://www.postgresql.org/docs/16/sql.html)

use nom::IResult;

mod common;
pub use common::{
    AggregateExpression, BinaryOperator, ColumnReference, DataType, FunctionCall, Identifier,
    Literal, TypeModifier, ValueExpression,
};

mod create;
pub use create::{CreateIndex, CreateTable, TableField};

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
pub use transactions::{IsolationMode, BeginTransaction, CommitTransaction, AbortTransaction};

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

pub mod dialects {
    /// Indicates that the parser should be compatible with the Postgres Dialect of SQL
    pub struct Postgres;
}

/// Should be implemented by parser to indicate that it is compatible with the given dialect `D`
pub trait CompatibleParser<D> {
    type StaticVersion: 'static;

    fn to_static(&self) -> Self::StaticVersion;

    fn parameter_count(&self) -> usize;
}

pub trait Parser<'i>: Sized {
    fn parse() -> impl Fn(&'i [u8]) -> IResult<&'i [u8], Self, nom::error::VerboseError<&'i [u8]>>;
}

#[derive(Debug, PartialEq)]
pub enum Query<'s> {
    WithCTE { cte: WithCTEs<'s>, query: Box<Self> },
    Prepare(prepare::Prepare<'s>),
    Select(select::Select<'s>),
    Insert(insert::Insert<'s>),
    Update(update::Update<'s>),
    Copy_(copy_::Copy_<'s>),
    Delete(delete::Delete<'s>),
    CreateTable(create::CreateTable<'s>),
    CreateIndex(create::CreateIndex<'s>),
    AlterTable(alter::AlterTable<'s>),
    DropIndex(drop::DropIndex<'s>),
    DropTable(drop::DropTable<'s>),
    TruncateTable(truncate::TruncateTable<'s>),
    Configuration(set_config::Configuration),
    BeginTransaction(transactions::IsolationMode),
    CommitTransaction,
    RollbackTransaction,
    Vacuum(vacuum::Vacuum),
}

#[derive(Debug)]
pub struct ParseQueryError {}

impl<'s> Query<'s> {
    pub fn parse<'r>(raw: &'r [u8]) -> Result<Self, ParseQueryError>
    where
        'r: 's,
    {
        let (remaining, query) = nom::combinator::complete(query)(raw).map_err(|e| {
            dbg!(&e);
            ParseQueryError{}
        })?;

        if !remaining.is_empty() {
            let _ = dbg!(core::str::from_utf8(remaining));
            return Err(ParseQueryError {});
        }

        Ok(query)
    }

    pub fn parse_multiple<'r>(raw: &'r [u8]) -> Result<Vec<Self>, ParseQueryError>
    where
        'r: 's,
    {
        let (remaining, queries) = nom::multi::many1(query)(raw).map_err(|e| {
            dbg!(e);
            ParseQueryError {}
        })?;

        if !remaining.is_empty() {
            let _ = dbg!(core::str::from_utf8(remaining));
            return Err(ParseQueryError {});
        }

        Ok(queries)
    }

    pub fn to_static(&self) -> Query<'static> {
        match self {
            Self::WithCTE { cte, query } => Query::WithCTE {
                cte: cte.to_static(),
                query: Box::new(query.to_static()),
            },
            Self::Prepare(prepare) => Query::Prepare(prepare.to_static()),
            Self::Select(s) => Query::Select(s.to_static()),
            Self::Insert(ins) => Query::Insert(ins.to_static()),
            Self::Update(ups) => Query::Update(ups.to_static()),
            Self::Copy_(c) => Query::Copy_(c.to_static()),
            Self::Delete(d) => Query::Delete(d.to_static()),
            Self::CreateTable(ct) => Query::CreateTable(ct.to_static()),
            Self::CreateIndex(ci) => Query::CreateIndex(ci.to_static()),
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

    pub fn parameter_count(&self) -> usize {
        match self {
            Self::WithCTE { cte, query } => {
                core::cmp::max(cte.parameter_count(), query.parameter_count())
            }
            Self::Prepare(p) => p.params.len(),
            Self::Select(s) => s.max_parameter(),
            Self::Insert(i) => i.max_parameter(),
            Self::Update(u) => u.max_parameter(),
            Self::Copy_(c) => c.parameter_count(),
            Self::Delete(d) => d.max_parameter(),
            Self::CreateTable(_) => 0,
            Self::CreateIndex(_) => 0,
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
}

fn query(raw: &[u8]) -> IResult<&[u8], Query<'_>, nom::error::VerboseError<&[u8]>> {
    let (raw, ctes) = nom::combinator::opt(with_ctes)(raw)?;

    let (rem, inner) = nom::combinator::map(
        nom::sequence::tuple((
            nom::character::complete::multispace0,
            nom::branch::alt((
                nom::combinator::map(Select::parse(), Query::Select),
                nom::combinator::map(Insert::parse(), Query::Insert),
                nom::combinator::map(Update::parse(), Query::Update),
                nom::combinator::map(Copy_::parse(), Query::Copy_),
                nom::combinator::map(Delete::parse(), Query::Delete),
                nom::combinator::map(BeginTransaction::parse(), |iso| {
                    Query::BeginTransaction(iso.isolation)
                }),
                nom::combinator::map(CommitTransaction::parse(), |_| {
                    Query::CommitTransaction
                }),
                nom::combinator::map(AbortTransaction::parse(), |_| {
                    Query::RollbackTransaction
                }),
                nom::combinator::map(CreateTable::parse(), Query::CreateTable),
                nom::combinator::map(CreateIndex::parse(), Query::CreateIndex),
                nom::combinator::map(AlterTable::parse(), Query::AlterTable),
                nom::combinator::map(DropIndex::parse(), Query::DropIndex),
                nom::combinator::map(DropTable::parse(), Query::DropTable),
                nom::combinator::map(TruncateTable::parse(), Query::TruncateTable),
                nom::combinator::map(Configuration::parse(), Query::Configuration),
                nom::combinator::map(Prepare::parse(), Query::Prepare),
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
            query: Box::new(inner),
        },
        None => inner,
    };

    Ok((rem, result))
}

#[cfg(test)]
pub(crate) mod macros {
    macro_rules! parser_parse {
        ($target_ty:ty, $input:expr) => {{
            use crate::Parser as _;
            let (remaining, _) = <$target_ty>::parse()($input.as_bytes()).unwrap();
            assert_eq!(&[] as &[u8], remaining, "{:?}", core::str::from_utf8(remaining).unwrap());
        }};
        ($target_ty:ty, $input:expr, $expected:expr) => {
            parser_parse!($target_ty, $input, $expected, &[])
        };
        ($target_ty:ty, $input:expr, $expected:expr, $remaining:expr) => {
            {
                use crate::Parser as _;
                let (remaining, result) = <$target_ty>::parse()($input.as_bytes()).unwrap();
                assert_eq!($remaining as &[u8], remaining, "{:?}", core::str::from_utf8(remaining).unwrap());
                assert_eq!($expected, result);
            }
        } 
    }
    pub (crate) use parser_parse;

    macro_rules! parser_parse_err {
        ($target_ty:ty, $input:expr) => {{
            use crate::Parser as _;
            let _tmp = <$target_ty>::parse()($input.as_bytes()).unwrap_err();
        }};
    }
    pub(crate) use parser_parse_err;
}

#[cfg(test)]
mod tests {
    use crate::select::{GroupAttribute, OrderAttribute};

    use super::*;

    use pretty_assertions::assert_eq;

    #[test]
    fn select_with_where() {
        let query = Query::parse("select * from table where first = 'value'".as_bytes()).unwrap();
        dbg!(&query);
    }

    #[test]
    fn select_with_where_multiple() {
        let query = Query::parse(
            "select * from table where first = 'value' AND second = 'other'".as_bytes(),
        )
        .unwrap();
        dbg!(&query);
    }

    #[test]
    fn select_test() {
        let query = Query::parse(
            "SELECT \"id\", \"migration_id\", \"sql\", \"success\", \"error\", \"timestamp\" FROM \"migration_log\""
                .as_bytes(),
        )
        .unwrap();

        let select = match query {
            Query::Select(s) => s,
            other => {
                dbg!(other);
                panic!();
            }
        };
        dbg!(&select);

        assert_eq!(
            vec![
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
            ],
            select.values
        );
        assert_eq!(
            TableExpression::Relation("migration_log".into()),
            select.table.unwrap()
        );
        assert_eq!(None, select.where_condition);
    }

    #[test]
    fn create_table() {
        let query = Query::parse("CREATE TABLE IF NOT EXISTS \"migration_log\" (\n\"id\" SERIAL PRIMARY KEY  NOT NULL\n, \"migration_id\" VARCHAR(255) NOT NULL\n, \"sql\" TEXT NOT NULL\n, \"success\" BOOL NOT NULL\n, \"error\" TEXT NOT NULL\n, \"timestamp\" TIMESTAMP NOT NULL\n);".as_bytes()).unwrap();

        assert!(matches!(query, Query::CreateTable(_)), "{:?}", query);

        let query = Query::parse("CREATE TABLE IF NOT EXISTS \"user\" (\n\"id\" SERIAL PRIMARY KEY  NOT NULL\n, \"version\" INTEGER NOT NULL\n, \"login\" VARCHAR(190) NOT NULL\n, \"email\" VARCHAR(190) NOT NULL\n, \"name\" VARCHAR(255) NULL\n, \"password\" VARCHAR(255) NULL\n, \"salt\" VARCHAR(50) NULL\n, \"rands\" VARCHAR(50) NULL\n, \"company\" VARCHAR(255) NULL\n, \"account_id\" BIGINT NOT NULL\n, \"is_admin\" BOOL NOT NULL\n, \"created\" TIMESTAMP NOT NULL\n, \"updated\" TIMESTAMP NOT NULL\n);".as_bytes()).unwrap();

        assert!(matches!(query, Query::CreateTable(_)), "{:?}", query);
    }

    #[test]
    fn insert_query() {
        let query = Query::parse(
            "INSERT INTO \"migration_log\" (\"migration_id\",\"sql\",\"success\",\"error\",\"timestamp\") VALUES ($1, $2, $3, $4, $5) RETURNING \"id\""
                .as_bytes(),
        )
        .unwrap();

        assert!(matches!(query, Query::Insert(_)), "{:?}", query);
    }

    #[test]
    fn alter_table() {
        let query = Query::parse("ALTER TABLE \"user\" RENAME TO \"user_v1\"".as_bytes()).unwrap();

        assert!(matches!(query, Query::AlterTable(_)), "{:?}", query);
    }

    #[test]
    fn insert_with_select() {
        let query = Query::parse("INSERT INTO \"user\" (\"name\"\n, \"company\"\n, \"org_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\") SELECT \"name\"\n, \"company\"\n, \"account_id\"\n, \"is_admin\"\n, \"created\"\n, \"updated\"\n, \"version\"\n, \"email\"\n, \"password\"\n, \"rands\"\n, \"salt\"\n, \"id\"\n, \"login\" FROM \"user_v1\"".as_bytes()).unwrap();

        assert!(matches!(query, Query::Insert(_)), "{:?}", query);
    }

    #[test]
    fn drop_index() {
        let query = Query::parse("DROP INDEX \"UQE_user_login\" CASCADE".as_bytes()).unwrap();

        assert!(matches!(query, Query::DropIndex(_)), "{:?}", query);
    }

    #[test]
    fn multiple_queries() {
        let queries = Query::parse_multiple("ALTER TABLE alert_rule ALTER COLUMN is_paused SET DEFAULT false;\nUPDATE alert_rule SET is_paused = false;".as_bytes()).unwrap();

        dbg!(&queries);

        assert_eq!(2, queries.len(), "{:?}", queries);
    }

    #[test]
    fn max_parameter_select() {
        let query = "SELECT \"id\", \"role_id\", \"action\", \"scope\", \"kind\", \"attribute\", \"identifier\", \"updated\", \"created\" FROM \"permission\" WHERE \"action\" IN ($1,$2,$3,$4)";

        let query = Query::parse(query.as_bytes()).unwrap();

        assert_eq!(4, query.parameter_count());
    }

    #[test]
    fn select_rename_table() {
        let query_str = "SELECT p.name FROM permission AS p";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        assert_eq!(
            Query::Select(Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: Some(Identifier("p".into())),
                    column: Identifier("name".into())
                })],
                table: Some(TableExpression::Renamed {
                    inner: Box::new(TableExpression::Relation(Identifier("permission".into()))),
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
            }),
            query
        );
    }

    #[test]
    fn select_rename_attribute() {
        let query_str = "SELECT name AS pname FROM permission";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        assert_eq!(
            Query::Select(Select {
                values: vec![ValueExpression::Renamed {
                    inner: Box::new(ValueExpression::ColumnReference(ColumnReference {
                        relation: None,
                        column: Identifier("name".into())
                    })),
                    name: Identifier("pname".into())
                }],
                table: Some(TableExpression::Relation(Identifier("permission".into()))),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }),
            query
        );
    }

    #[test]
    fn select_with_basic_join() {
        let query_str =
            "SELECT user.name, password.hash FROM user JOIN password ON user.id = password.uid";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        dbg!(&query);

        assert_eq!(
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
                ],
                table: Some(TableExpression::Join {
                    left: Box::new(TableExpression::Relation(Identifier("user".into()))),
                    right: Box::new(TableExpression::Relation(Identifier("password".into()))),
                    kind: JoinKind::Inner,
                    condition: Condition::And(vec![Condition::Value(Box::new(
                        ValueExpression::Operator {
                            first: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some(Identifier("user".into())),
                                column: Identifier("id".into()),
                            })),
                            second: Box::new(ValueExpression::ColumnReference(ColumnReference {
                                relation: Some(Identifier("password".into())),
                                column: Identifier("uid".into())
                            })),
                            operator: BinaryOperator::Equal
                        }
                    ))]),
                    lateral: false,
                }),
                where_condition: None,
                order_by: None,
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }),
            query
        );
    }

    #[test]
    fn select_with_renaming_table() {
        let query_str = "SELECT r.name as role_name, r.id as role_id, r.org_id as org_id,p.action, p.scope\n\tFROM permission AS p\n\tINNER JOIN role AS r ON p.role_id = r.id\n\tWHERE r.name LIKE $1";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        dbg!(&query);

        // todo!()
    }

    #[test]
    fn select_large() {
        let query_str = "SELECT \"id\", \"role_id\", \"action\", \"scope\", \"kind\", \"attribute\", \"identifier\", \"updated\", \"created\" FROM \"permission\" WHERE ( action IN ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16))";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        dbg!(&query);

        // todo!()
    }

    #[test]
    fn select_with_order_by() {
        let query_str = "SELECT \"id\" FROM \"alert_rule\" ORDER BY \"id\" ASC";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        dbg!(&query);

        assert_eq!(
            Query::Select(Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                })],
                table: Some(TableExpression::Relation(Identifier("alert_rule".into()))),
                where_condition: None,
                order_by: Some(vec![Ordering {
                    column: OrderAttribute::ColumnRef(ColumnReference {
                        relation: None,
                        column: Identifier("id".into())
                    }),
                    order: OrderBy::Ascending,
                    nulls: NullOrdering::Last
                }]),
                group_by: None,
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }),
            query
        );
    }

    #[test]
    fn select_with_group_by() {
        let query_str = "SELECT \"id\" FROM \"alert_rule\" GROUP BY \"id\"";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        dbg!(&query);

        assert_eq!(
            Query::Select(Select {
                values: vec![ValueExpression::ColumnReference(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                })],
                table: Some(TableExpression::Relation(Identifier("alert_rule".into()))),
                where_condition: None,
                order_by: None,
                group_by: Some(vec![GroupAttribute::ColumnRef(ColumnReference {
                    relation: None,
                    column: Identifier("id".into())
                })]),
                having: None,
                limit: None,
                for_update: None,
                combine: None
            }),
            query
        );
    }

    #[test]
    fn select_complex() {
        let query_str = "SELECT dashboard.id, dashboard.uid, dashboard.is_folder, dashboard.org_id, count(dashboard_acl.id) as count\n\t\t  FROM dashboard\n\t\t\t\tLEFT JOIN dashboard_acl ON dashboard.id = dashboard_acl.dashboard_id\n\t\t  WHERE dashboard.has_acl IS TRUE\n\t\t  GROUP BY dashboard.id";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        dbg!(&query);

        // todo!()
    }

    #[test]
    fn select_complex2() {
        let query_str = "\n\tSELECT res.uid, res.is_folder, res.org_id\n\tFROM (SELECT dashboard.id, dashboard.uid, dashboard.is_folder, dashboard.org_id, count(dashboard_acl.id) as count\n\t\t  FROM dashboard\n\t\t\t\tLEFT JOIN dashboard_acl ON dashboard.id = dashboard_acl.dashboard_id\n\t\t  WHERE dashboard.has_acl IS TRUE\n\t\t  GROUP BY dashboard.id) as res\n\tWHERE res.count = 0\n\t";

        let query = Query::parse(query_str.as_bytes()).unwrap();

        dbg!(&query);

        // todo!()
    }
}
