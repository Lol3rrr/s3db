use std::collections::HashMap;

use s3db::{
    ra::{RaDelete, RaExpression, RaUpdate},
    storage::Schemas,
};
use sql::{DataType, Query};

/// Returns an iterator to generate the Grafana Schema for testing
fn grafana_schema() -> impl Iterator<Item = (String, Vec<(String, DataType)>)> {
    [
        (
            "dashboard".into(),
            vec![
                ("id".into(), DataType::Integer),
                ("uid".into(), DataType::Integer),
                ("title".into(), DataType::Text),
                ("slug".into(), DataType::Text),
                ("is_folder".into(), DataType::Bool),
                ("folder_id".into(), DataType::Integer),
                ("folder_uid".into(), DataType::Integer),
                ("org_id".into(), DataType::Integer),
            ],
        ),
        (
            "dashboard_tag".into(),
            vec![
                ("term".into(), DataType::Text),
                ("dashboard_id".into(), DataType::Integer),
            ],
        ),
        (
            "folder".into(),
            vec![
                ("uid".into(), DataType::Integer),
                ("slug".into(), DataType::Text),
                ("title".into(), DataType::Text),
            ],
        ),
        (
            "permission".into(),
            vec![
                ("role_id".into(), DataType::Integer),
                ("scope".into(), DataType::Text),
                ("action".into(), DataType::Text),
            ],
        ),
        ("role".into(), vec![("id".into(), DataType::Integer)]),
        (
            "user_role".into(),
            vec![
                ("id".into(), DataType::Integer),
                ("user_id".into(), DataType::Integer),
                ("org_id".into(), DataType::Integer),
                ("role_id".into(), DataType::Integer),
            ],
        ),
        (
            "builtin_role".into(),
            vec![
                ("role_id".into(), DataType::Integer),
                ("org_id".into(), DataType::Integer),
                ("role".into(), DataType::Text),
            ],
        ),
    ]
    .into_iter()
}

#[test]
fn grafana_query_1() {
    let query_str = "SELECT
        dashboard.id, dashboard.uid, dashboard.title, dashboard.slug, dashboard_tag.term, dashboard.is_folder, dashboard.folder_id, folder.uid AS folder_uid,\n\t\t\n\t\t\tfolder.slug AS folder_slug,\n\t\t\tfolder.title AS folder_title
        FROM (
            SELECT dashboard.id
            FROM dashboard
            WHERE (NOT dashboard.is_folder OR dashboard.is_folder) AND dashboard.org_id=$1
            ORDER BY dashboard.title ASC NULLS FIRST
            LIMIT 30 OFFSET 0
        ) AS ids\n\t\t
        INNER JOIN dashboard ON ids.id = dashboard.id\n\n\t\t
        LEFT OUTER JOIN dashboard AS folder ON folder.id = dashboard.folder_id\n\t
        LEFT OUTER JOIN dashboard_tag ON dashboard.id = dashboard_tag.dashboard_id\n
        ORDER BY dashboard.title ASC NULLS FIRST";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = grafana_schema().collect();

    let result = RaExpression::parse_select(&select, &schemas).unwrap();

    let _ = result;
}

#[test]
fn grafana_query_2() {
    let query_str = "SELECT
    dashboard.id,dashboard.uid,dashboard.title,dashboard.slug,dashboard_tag.term,dashboard.is_folder,dashboard.folder_id,folder.uid AS folder_uid,folder.slug AS folder_slug,\n\t\t\tfolder.title AS folder_title
    FROM (
        SELECT dashboard.id
        FROM dashboard
        WHERE (
            (dashboard.uid IN (
                SELECT substr(scope, 16)
                FROM permission
                WHERE scope LIKE 'dashboards:uid:%' AND role_id IN (
                    SELECT id
                    FROM role
                    INNER JOIN (
                        SELECT ur.role_id\n\t\t\t
                        FROM user_role AS ur\n\t\t\t
                        WHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\t
                        UNION\n\t\t\t
                        SELECT br.role_id
                        FROM builtin_role AS br\n\t\t\t
                        WHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t
                    ) as all_role ON role.id = all_role.role_id
                ) AND action = $8
            ) AND NOT dashboard.is_folder
            ) OR (dashboard.folder_id IN (
                SELECT d.id
                FROM dashboard as d
                WHERE d.org_id = $9 AND d.uid IN (
                    SELECT substr(scope, 13)
                    FROM permission
                    WHERE scope LIKE 'folders:uid:%' AND role_id IN (
                        SELECT id
                        FROM role
                        INNER JOIN (
                            SELECT ur.role_id\n\t\t\t
                            FROM user_role AS ur\n\t\t\t
                            WHERE ur.user_id = $10\n\t\t\tAND (ur.org_id = $11 OR ur.org_id = $12)\n\t\t
                            UNION\n\t\t\t
                            SELECT br.role_id
                            FROM builtin_role AS br\n\t\t\t
                            WHERE br.role IN ($13, $14)\n\t\t\tAND (br.org_id = $15 OR br.org_id = $16)\n\t\t
                        ) as all_role ON role.id = all_role.role_id
                    ) AND action = $17
                    )
                ) AND NOT dashboard.is_folder
            )
        ) AND dashboard.org_id=$18 AND dashboard.title ILIKE $19 AND dashboard.is_folder = false AND dashboard.folder_id = $20
        ORDER BY dashboard.title ASC NULLS FIRST LIMIT 1000 OFFSET 0
    ) AS ids\n\t\t
    INNER JOIN dashboard ON ids.id = dashboard.id\n\n\t\t
    LEFT OUTER JOIN dashboard AS folder ON folder.id = dashboard.folder_id\n\t
    LEFT OUTER JOIN dashboard_tag ON dashboard.id = dashboard_tag.dashboard_id\n
    ORDER BY dashboard.title ASC NULLS FIRST";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    dbg!(&select);

    let schemas: Schemas = [
        (
            "dashboard".into(),
            vec![
                ("id".into(), DataType::Integer),
                ("uid".into(), DataType::Integer),
                ("title".into(), DataType::Text),
                ("slug".into(), DataType::Text),
                ("is_folder".into(), DataType::Bool),
                ("folder_id".into(), DataType::Integer),
                ("org_id".into(), DataType::Integer),
            ],
        ),
        (
            "dashboard_tag".into(),
            vec![
                ("term".into(), DataType::Text),
                ("dashboard_id".into(), DataType::Integer),
            ],
        ),
        (
            "permission".into(),
            vec![
                ("scope".into(), DataType::Text),
                ("id".into(), DataType::Integer),
                ("role_id".into(), DataType::Integer),
                ("action".into(), DataType::Text),
            ],
        ),
        ("role".into(), vec![("id".into(), DataType::Integer)]),
        (
            "builtin_role".into(),
            vec![
                ("role_id".into(), DataType::Integer),
                ("role".into(), DataType::Text),
                ("org_id".into(), DataType::Integer),
            ],
        ),
        (
            "user_role".into(),
            vec![
                ("id".into(), DataType::Integer),
                ("user_id".into(), DataType::Integer),
                ("org_id".into(), DataType::Integer),
                ("role_id".into(), DataType::Integer),
            ],
        ),
    ]
    .into_iter()
    .collect();

    let result = RaExpression::parse_select(&select, &schemas).unwrap();

    let _ = result;
}

#[test]
fn grafana_query_4() {
    let query_str = "SELECT p.* FROM permission as p INNER JOIN role r on r.id = p.role_id WHERE r.id = $1 AND p.scope = $2";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [
        (
            "permission".into(),
            vec![
                ("role_id".into(), DataType::Integer),
                ("scope".into(), DataType::Text),
            ],
        ),
        ("role".into(), vec![("id".into(), DataType::Integer)]),
    ]
    .into_iter()
    .collect();

    let result = RaExpression::parse_select(&select, &schemas).unwrap();

    let _ = result;
}

#[test]
fn grafana_query_5() {
    let query_str = "SELECT
    dashboard.id,
    dashboard.uid,
    dashboard.title,
    dashboard.slug,
    dashboard_tag.term,
    dashboard.is_folder,
    dashboard.folder_id,
    folder.uid AS folder_uid,
    folder.slug AS folder_slug,
    folder.title AS folder_title
FROM (
    SELECT
        dashboard.id
    FROM dashboard
    WHERE (
        (dashboard.uid IN (
            SELECT substr(scope, 13)
            FROM permission
            WHERE scope LIKE 'folders:uid:%' AND role_id IN(
                SELECT id
                FROM role
                INNER JOIN (
                    SELECT ur.role_id
                    FROM user_role AS ur
                    WHERE ur.user_id = $1 AND (ur.org_id = $2 OR ur.org_id = $3)
                    UNION
                    SELECT br.role_id
                    FROM builtin_role AS br
                    WHERE br.role IN ($4, $5) AND (br.org_id = $6 OR br.org_id = $7)
                ) as all_role ON role.id = all_role.role_id) AND action IN ($8, $9)
            GROUP BY role_id, scope HAVING COUNT(action) = $10
        ) AND dashboard.is_folder)
    ) AND dashboard.org_id=$11 AND dashboard.is_folder = true
    ORDER BY dashboard.title ASC NULLS FIRST LIMIT 1000 OFFSET 0
) AS ids
INNER JOIN dashboard ON ids.id = dashboard.id
LEFT OUTER JOIN dashboard AS folder ON folder.id = dashboard.folder_id
LEFT OUTER JOIN dashboard_tag ON dashboard.id = dashboard_tag.dashboard_id
ORDER BY dashboard.title ASC NULLS FIRST";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = grafana_schema().collect();

    let (result, placeholders) = RaExpression::parse_select(&select, &schemas).unwrap();

    assert_eq!(11, placeholders.len(), "{:?}", placeholders);

    let _ = result;
}

#[test]
fn grafana_update_from_1() {
    let query_str = "UPDATE dashboard\n\tSET folder_uid = folder.uid\n\tFROM dashboard folder\n\tWHERE dashboard.folder_id = folder.id\n\t  AND dashboard.is_folder = $1";

    let update = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Update(u)) => u,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = grafana_schema().collect();

    let (result, placeholders) = RaUpdate::parse(&update, &schemas).unwrap();

    assert_eq!(1, placeholders.len(), "{:?}", placeholders);

    let _ = result;
}

#[test]
fn specific() {
    let query_str = "SELECT substr(name, 2) FROM users GROUP BY name";

    let query = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [(
        "users".to_string(),
        vec![("name".to_string(), DataType::Text)],
    )]
    .into_iter()
    .collect();

    let (select, parameter_types) = RaExpression::parse_select(&query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    dbg!(&select);
}

#[test]
#[ignore = "Fixing some other time"]
fn delete_with_correlated_subquery() {
    let query_str =
        "DELETE FROM users WHERE NOT EXISTS (SELECT 1 FROM roles WHERE roles.id = users.role)";

    let query = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Delete(d)) => d,
        other => panic!("{:?}", other),
    };

    let schemas: Schemas = [
        (
            "users".to_string(),
            vec![
                ("name".to_string(), DataType::Text),
                ("role".into(), DataType::Integer),
            ],
        ),
        ("roles".into(), vec![("id".into(), DataType::Integer)]),
    ]
    .into_iter()
    .collect();

    let (delete, parameter_types) = RaDelete::parse(&query, &schemas).unwrap();

    assert_eq!(HashMap::new(), parameter_types);

    dbg!(&delete);

    todo!()
}
