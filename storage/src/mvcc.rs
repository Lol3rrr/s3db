use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum Visibility {
    NotVisible,
    Visible,
}

pub fn check<TAC, TAB>(
    t_current: u64,
    t_active: TAC,
    t_aborted: TAB,
    t_latest: u64,
    row_created: u64,
    row_expired: u64,
) -> Visibility
where
    TAC: TransactionSet,
    TAB: TransactionSet,
{
    // Check if the row was created either by us or before our transaction was started
    if (t_active.contains(row_created) || row_created > t_latest || t_aborted.contains(row_created))
        && row_created != t_current
    {
        return Visibility::NotVisible;
    }

    // Check if the row was deleted either by us or before our transaction started before us
    if row_expired != 0
        && (!t_active.contains(row_expired) || row_expired == t_current || row_expired < t_latest)
        && !t_aborted.contains(row_expired)
    {
        return Visibility::NotVisible;
    }

    Visibility::Visible
}

pub trait TransactionSet {
    fn contains(&self, id: u64) -> bool;
}
impl TransactionSet for &HashSet<u64> {
    fn contains(&self, id: u64) -> bool {
        HashSet::contains(self, &id)
    }
}
impl TransactionSet for &[u64] {
    fn contains(&self, id: u64) -> bool {
        self.iter().any(|v| *v == id)
    }
}
impl<const N: usize> TransactionSet for &[u64; N] {
    fn contains(&self, id: u64) -> bool {
        self.iter().any(|v| *v == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visible() {
        assert_eq!(Visibility::Visible, check(10, &[10], &[], 8, 2, 0));
    }

    #[test]
    fn invisible_from_other_transaction() {
        assert_eq!(Visibility::NotVisible, check(10, &[10, 11], &[], 8, 11, 0));
    }

    #[test]
    fn visible_own_transaction() {
        assert_eq!(Visibility::Visible, check(10, &[10, 11], &[], 8, 10, 0));
    }

    #[test]
    fn something() {
        assert_eq!(Visibility::Visible, check(3, &[3, 4], &[], 2, 1, 0));
        assert_eq!(Visibility::Visible, check(4, &[3, 4], &[], 3, 1, 0));
    }
}
