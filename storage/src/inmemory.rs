pub mod v1;

pub mod v2;

pub type LoadingError = v1::LoadingError;
pub type InMemoryStorage = v1::InMemoryStorage;
pub type InMemorySequence<'a> = v1::InMemorySequence<'a>;
pub type InMemoryTransactionGuard = v1::InMemoryTransactionGuard;

fn postgres_tables() -> impl Iterator<Item = (String, crate::TableSchema, Vec<crate::Row>)> {
    [
        (
            "pg_tables".to_string(),
            crate::TableSchema {
                rows: vec![
                    crate::ColumnSchema {
                        name: "schemaname".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "tablename".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "tableowner".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "tablespace".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "hasindexes".into(),
                        ty: sql::DataType::Bool,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "hasrules".into(),
                        ty: sql::DataType::Bool,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "hastriggers".into(),
                        ty: sql::DataType::Bool,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "rowsecurity".into(),
                        ty: sql::DataType::Bool,
                        mods: Vec::new(),
                    },
                ],
            },
            Vec::new(),
        ),
        (
            "pg_indexes".to_string(),
            crate::TableSchema {
                rows: vec![
                    crate::ColumnSchema {
                        name: "schemaname".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "tablename".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "indexname".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "tablespace".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "indexdef".into(),
                        ty: sql::DataType::Text,
                        mods: Vec::new(),
                    },
                ],
            },
            Vec::new(),
        ),
        (
            "pg_class".to_string(),
            crate::TableSchema {
                rows: vec![
                    crate::ColumnSchema {
                        name: "oid".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "relname".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "relnamespace".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                ],
            },
            Vec::new(),
        ),
        (
            "pg_namespace".to_string(),
            crate::TableSchema {
                rows: vec![
                    crate::ColumnSchema {
                        name: "oid".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "nspname".into(),
                        ty: sql::DataType::Name,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "nspowner".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                ],
            },
            vec![crate::Row::new(
                0,
                vec![
                    crate::Data::Integer(0),
                    crate::Data::Name("default".to_string()),
                    crate::Data::Integer(0),
                ],
            )],
        ),
        (
            "pg_partitioned_table".to_string(),
            crate::TableSchema {
                rows: vec![
                    crate::ColumnSchema {
                        name: "partrelid".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "partstrat".into(),
                        ty: sql::DataType::Text,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "partdefid".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                ],
            },
            Vec::new(),
        ),
        (
            "pg_inherits".to_string(),
            crate::TableSchema {
                rows: vec![
                    crate::ColumnSchema {
                        name: "inhrelid".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                    crate::ColumnSchema {
                        name: "inhparent".into(),
                        ty: sql::DataType::Integer,
                        mods: Vec::new(),
                    },
                ],
            },
            Vec::new(),
        ),
    ]
    .into_iter()
}
