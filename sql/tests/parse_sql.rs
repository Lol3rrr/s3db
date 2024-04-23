use sql::Query;

macro_rules! try_parse {
    ($content:literal, $expected:pat) => {{
        let arena = bumpalo::Bump::new();
        match Query::parse($content.as_bytes(), &arena) {
            Ok($expected) => {}
            other => {
                panic!("{:?}", other);
            }
        };
    }};
}

#[test]
fn grafana_query_1() {
    try_parse!(
        "SELECT
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
        ORDER BY dashboard.title ASC NULLS FIRST",
        Query::Select(_)
    );
}

#[test]
fn grafana_query_2() {
    try_parse!("SELECT
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
    ORDER BY dashboard.title ASC NULLS FIRST",
        Query::Select(_)
    );
}

#[test]
fn tmp() {
    try_parse!("SELECT
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
ORDER BY dashboard.title ASC NULLS FIRST", Query::Select(_));
}

#[test]
fn grafana_query_4() {
    try_parse!("SELECT p.* FROM permission as p INNER JOIN role r on r.id = p.role_id WHERE r.id = $1 AND p.scope = $2", Query::Select(_));
}

#[test]
fn grafana_query_5() {
    try_parse!(
        "SELECT
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
ORDER BY dashboard.title ASC NULLS FIRST",
        Query::Select(_)
    );
}

#[test]
fn grafana_query_6() {
    try_parse!("SELECT
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
GROUP BY active, daily_active, role;", Query::Select(_));
}

#[test]
fn grafana_query_7() {
    try_parse!("SELECT\n\t\t\tdashboard.id,\n\t\t\tdashboard.uid,\n\t\t\tdashboard.title,\n\t\t\tdashboard.slug,\n\t\t\tdashboard_tag.term,\n\t\t\tdashboard.is_folder,\n\t\t\tdashboard.folder_id,\n\t\t\tfolder.uid AS folder_uid,\n\t\t\n\t\t\tfolder.slug AS folder_slug,\n\t\t\tfolder.title AS folder_title  FROM ( SELECT dashboard.id FROM dashboard WHERE ((dashboard.uid IN (SELECT substr(scope, 13) FROM permission WHERE scope LIKE 'folders:uid:%' AND role_id IN(SELECT id FROM role INNER JOIN (\n\t\t\tSELECT ur.role_id\n\t\t\tFROM user_role AS ur\n\t\t\tWHERE ur.user_id = $1\n\t\t\tAND (ur.org_id = $2 OR ur.org_id = $3)\n\t\tUNION\n\t\t\tSELECT br.role_id FROM builtin_role AS br\n\t\t\tWHERE br.role IN ($4, $5)\n\t\t\tAND (br.org_id = $6 OR br.org_id = $7)\n\t\t) as all_role ON role.id = all_role.role_id)  AND action IN ($8, $9) GROUP BY role_id, scope HAVING COUNT(action) = $10) AND dashboard.is_folder)) AND dashboard.org_id=$11 AND dashboard.is_folder = true ORDER BY dashboard.title ASC NULLS FIRST LIMIT 1000 OFFSET 0) AS ids\n\t\tINNER JOIN dashboard ON ids.id = dashboard.id\n\n\t\tLEFT OUTER JOIN dashboard AS folder ON folder.id = dashboard.folder_id\n\tLEFT OUTER JOIN dashboard_tag ON dashboard.id = dashboard_tag.dashboard_id\n ORDER BY dashboard.title ASC NULLS FIRST", Query::Select(_));
}

#[test]
#[ignore = "Not sure how to handle this"]
fn prepare() {
    try_parse!("prepare neword (INTEGER, INTEGER, INTEGER, INTEGER, INTEGER) as select neword($1,$2,$3,$4,$5,0)", Query::Prepare(_));
}

#[test]
fn copy_query() {
    try_parse!("copy pgbench_accounts from stdin", Query::Copy_(_));
}

#[test]
fn vacuum() {
    try_parse!("vacuum analyze pgbench_branches", Query::Vacuum(_));
}

#[test]
fn pgbench_1() {
    try_parse!(
        "
select o.n, p.partstrat, pg_catalog.count(i.inhparent)
from pg_catalog.pg_class as c
join pg_catalog.pg_namespace as n on (n.oid = c.relnamespace)
cross join lateral (
    select pg_catalog.array_position(pg_catalog.current_schemas(true), n.nspname)
) as o(n)
left join pg_catalog.pg_partitioned_table as p on (p.partrelid = c.oid)
left join pg_catalog.pg_inherits as i on (c.oid = i.inhparent)
where c.relname = 'pgbench_accounts' and o.n is not null
group by 1, 2
order by 1 asc
limit 1",
        Query::Select(_)
    );
}

#[test]
fn pgbench_1_altered() {
    try_parse!(
        "
select o.n, p.partstrat, pg_catalog.count(i.inhparent)
from pg_catalog.pg_class as c
join pg_catalog.pg_namespace as n on (n.oid = c.relnamespace)
cross join lateral (
    select pg_catalog.array_position(pg_catalog.current_schemas(true), n.nspname)
) as o(n)
left join pg_catalog.pg_partitioned_table as p on (p.partrelid = c.oid)
left join pg_catalog.pg_inherits as i on (c.oid = i.inhparent)
where c.relname = 'pgbench_accounts' and o.n is not null
group by 1, 2
order by 1 asc",
        Query::Select(_)
    );
}

#[test]
fn alter_add_primary_key() {
    try_parse!(
        "alter table pgbench_branches add primary key (bid)",
        Query::AlterTable(_)
    );
}

#[test]
fn truncate() {
    try_parse!("truncate pgbench_history", Query::TruncateTable(_));
}
