use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum Visibility {
    NotVisible,
    Visible,
}

pub fn check(
    t_current: u64,
    t_active: &HashSet<u64>,
    t_aborted: &HashSet<u64>,
    t_latest: u64,
    row_created: u64,
    row_expired: u64,
) -> Visibility {
    if (t_active.contains(&row_created)
        || row_created > t_latest
        || t_aborted.contains(&row_created))
        && row_created != t_current
    {
        return Visibility::NotVisible;
    }
    if row_expired != 0
        && (!t_active.contains(&row_expired) || row_expired == t_current || row_expired < t_latest)
        && !t_aborted.contains(&row_expired)
    {
        return Visibility::NotVisible;
    }

    Visibility::Visible
}
