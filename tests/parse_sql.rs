use s3db::sql::Query;

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

    let _ = select;
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

    let _ = select;
}
