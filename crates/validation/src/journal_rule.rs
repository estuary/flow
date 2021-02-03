use super::Error;
use itertools::Itertools;
use models::tables;

pub fn walk_all_journal_rules(journal_rules: &[tables::JournalRule], errors: &mut tables::Errors) {
    for (lhs, rhs) in journal_rules
        .iter()
        .sorted_by_key(|r| &r.rule)
        .tuple_windows()
    {
        if lhs.rule == rhs.rule {
            Error::Duplicate {
                entity: "journal rule",
                lhs: lhs.rule.to_string(),
                rhs_scope: rhs.scope.clone(),
            }
            .push(&lhs.scope, errors);
        }
    }
}
