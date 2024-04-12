use sql::Query;

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

#[test]
fn tmp() {
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
            SELECT
            substr(scope, 13)
            FROM permission
            WHERE scope LIKE 'folders:uid:%' AND role_id IN(SELECT id FROM role INNER JOIN (\n\t\t\tSELECT ur.role_id\n\t\t\tFROM user_role AS ur\n\t\t\tWHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\tUNION\n\t\t\tSELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t) as all_role ON role.id = all_role.role_id)  AND action IN ($8, $9)
            GROUP BY role_id, scope
            HAVING COUNT(action) = $10
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

    let _ = select;
}

#[test]
fn grafana_query_4() {
    let query_str = "SELECT p.* FROM permission as p INNER JOIN role r on r.id = p.role_id WHERE r.id = $1 AND p.scope = $2";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let _ = select;
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

    assert_eq!(11, select.max_parameter());

    let _ = select;
}

#[test]
fn grafana_query_6() {
    let query_str = "SELECT
role AS bitrole, active, COUNT(role) AS count
FROM (
    SELECT
        last_seen_at>$1 AS active, last_seen_at>$2 AS daily_active, SUM(role) AS role
        FROM (
            SELECT u.id, CASE org_user.role WHEN 'Admin' THEN 4 WHEN 'Editor' THEN 2 ELSE 1 END AS role, u.last_seen_at
            FROM \"user\" AS u
            INNER JOIN org_user ON org_user.user_id = u.id
            GROUP BY u.id, u.last_seen_at, org_user.role
        ) AS t2
        GROUP BY id, last_seen_at
) AS t1
GROUP BY active, daily_active, role;";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let _ = select;
}

#[test]
fn grafana_query_7() {
    let query_str = "SELECT\n\t\t\tdashboard.id,\n\t\t\tdashboard.uid,\n\t\t\tdashboard.title,\n\t\t\tdashboard.slug,\n\t\t\tdashboard_tag.term,\n\t\t\tdashboard.is_folder,\n\t\t\tdashboard.folder_id,\n\t\t\tfolder.uid AS folder_uid,\n\t\t\n\t\t\tfolder.slug AS folder_slug,\n\t\t\tfolder.title AS folder_title  FROM ( SELECT dashboard.id FROM dashboard WHERE ((dashboard.uid IN (SELECT substr(scope, 13) FROM permission WHERE scope LIKE 'folders:uid:%' AND role_id IN(SELECT id FROM role INNER JOIN (\n\t\t\tSELECT ur.role_id\n\t\t\tFROM user_role AS ur\n\t\t\tWHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\tUNION\n\t\t\tSELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t) as all_role ON role.id = all_role.role_id)  AND action IN ($8, $9) GROUP BY role_id, scope HAVING COUNT(action) = $10) AND dashboard.is_folder)) AND dashboard.org_id=$11 AND dashboard.is_folder = true ORDER BY dashboard.title ASC NULLS FIRST LIMIT 1000 OFFSET 0) AS ids\n\t\tINNER JOIN dashboard ON ids.id = dashboard.id\n\n\t\tLEFT OUTER JOIN dashboard AS folder ON folder.id = dashboard.folder_id\n\tLEFT OUTER JOIN dashboard_tag ON dashboard.id = dashboard_tag.dashboard_id\n ORDER BY dashboard.title ASC NULLS FIRST";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    assert_eq!(11, select.max_parameter());

    let _ = select;
}

#[test]
#[ignore = "Not sure how to handle this"]
fn prepare() {
    let query_str = "prepare neword (INTEGER, INTEGER, INTEGER, INTEGER, INTEGER) as select neword($1,$2,$3,$4,$5,0)";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Prepare(p)) => p,
        other => panic!("{:?}", other),
    };

    let _ = select;
}

#[test]
fn copy_query() {
    let query_str = "copy pgbench_accounts from stdin";

    let copy_ = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Copy_(c)) => c,
        other => panic!("{:?}", other),
    };

    let _ = copy_;
}

#[test]
fn vacuum() {
    let query_str = "vacuum analyze pgbench_branches";

    let vac = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Vacuum(v)) => v,
        other => panic!("{:?}", other),
    };

    let _ = vac;
}

#[test]
#[ignore = "I dont even know the exact behaviour and meaning of this query"]
fn pgbench_1() {
    let query_str = "
select o.n, p.partstrat, pg_catalog.count(i.inhparent)
from pg_catalog.pg_class as c
join pg_catalog.pg_namespace as n on (n.oid = c.relnamespace)
cross join lateral (
    select pg_catalog.array_position(pg_catalog.current_schemas(true), n.nspname)
) as o(n)
left join pg_catalog.pg_partitioned_table as p on (p.partrelid = c.oid)
left join pg_catalog.pg_inherits as i on (c.oid = i.inhparent)
where c.relname = 'pgbench_accounts' and o.n is not null
group by 1, 2 order by 1 asc limit 1";

    let select = match Query::parse(query_str.as_bytes()) {
        Ok(Query::Select(s)) => s,
        other => panic!("{:?}", other),
    };

    let _ = select;
}

#[test]
fn alter_add_primary_key() {
    let query_str = "alter table pgbench_branches add primary key (bid)";

    let alter = match Query::parse(query_str.as_bytes()) {
        Ok(Query::AlterTable(a)) => a,
        other => panic!("{:?}", other),
    };

    let _ = alter;
}
