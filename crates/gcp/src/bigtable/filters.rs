use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::RowFilter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Chain;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Interleave;

pub fn and(filters: Vec<RowFilter>) -> RowFilter {
    rf(Filter::Chain(Chain { filters }))
}

pub fn or(filters: Vec<RowFilter>) -> RowFilter {
    rf(Filter::Interleave(Interleave { filters }))
}

/// Returns a Rowfilter that matches only the most recent N cells within each column.
pub fn cells_per_column(n: i32) -> RowFilter {
    rf(Filter::CellsPerColumnLimitFilter(n))
}

pub fn columns(family: &str, columns: &[&str]) -> RowFilter {
    let family_regex = format!("^{}$", regex::escape(family));
    let column_regexes: Vec<String> = columns
        .iter()
        .map(|column| format!("^{}$", regex::escape(column)))
        .collect();
    let columns_regex = column_regexes.join("|").into_bytes();

    and(vec![
        rf(Filter::FamilyNameRegexFilter(family_regex)),
        rf(Filter::ColumnQualifierRegexFilter(columns_regex)),
    ])
}

fn rf(filter: Filter) -> RowFilter {
    RowFilter {
        filter: Some(filter),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variant(filter: &RowFilter) -> &Filter {
        filter
            .filter
            .as_ref()
            .expect("every filter here sets a variant")
    }

    fn assert_filter_eq(rf: &RowFilter, f: &Filter) {
        let filter = rf.filter.as_ref().unwrap();
        assert_eq!(filter, f);
    }

    /// Extracts the chain of RowFilters that makes up the given Chain. If
    /// `filter` is not a Chain, then this function will panic, failing the test.
    fn assert_chained(filter: &RowFilter) -> &[RowFilter] {
        let Filter::Chain(chain) = variant(filter) else {
            panic!("expected a Chain, got {:?}", variant(filter));
        };
        &chain.filters
    }

    #[test]
    fn and_chains_filters_in_order() {
        let filter = and(vec![cells_per_column(1), cells_per_column(2)]);

        assert_eq!(
            assert_chained(&filter),
            &[cells_per_column(1), cells_per_column(2)]
        );
    }

    #[test]
    fn or_interleaves_filters_in_order() {
        let filter = or(vec![cells_per_column(1), cells_per_column(2)]);

        let Filter::Interleave(interleave) = variant(&filter) else {
            panic!("expected an Interleave, got {:?}", variant(&filter));
        };
        assert_eq!(
            interleave.filters,
            vec![cells_per_column(1), cells_per_column(2)]
        );
    }

    #[test]
    fn cells_per_column_sets_the_limit() {
        assert_eq!(
            variant(&cells_per_column(3)),
            &Filter::CellsPerColumnLimitFilter(3)
        );
    }

    #[test]
    fn columns_anchors_the_family_and_column_names() {
        let filter = columns("f", &["flow_document"]);
        // expect the filter to be a chain of two filters
        let chain = assert_chained(&filter);
        assert_eq!(chain.len(), 2);
        // expect the first filter to be the "family" filter
        let expected = &Filter::FamilyNameRegexFilter("^f$".to_owned());
        assert_filter_eq(&chain[0], expected);
        // expect the second filter to be the "qualifier" filter
        let expected = &Filter::ColumnQualifierRegexFilter(b"^flow_document$".to_vec());
        assert_filter_eq(&chain[1], expected);
    }

    #[test]
    fn columns_joins_several_column_names_with_or() {
        let filter = columns("f", &["catalog_name", "ts", "flow_document"]);
        // expect the filter to be a chain of two filters
        let chain = assert_chained(&filter);
        assert_eq!(chain.len(), 2);
        // expect the "qualifier" filter to join columns into one regex
        let expected =
            &Filter::ColumnQualifierRegexFilter(b"^catalog_name$|^ts$|^flow_document$".to_vec());
        assert_filter_eq(&chain[1], expected);
    }

    #[test]
    fn columns_escapes_regex_characters_in_names() {
        let filter = columns("f.1", &["a|b", "c$"]);
        // expect the filter to be a chain of two filters
        let chain = assert_chained(&filter);
        assert_eq!(chain.len(), 2);
        // expect the "family" filter to escape the `.`
        let expected = &Filter::FamilyNameRegexFilter(r"^f\.1$".to_owned());
        assert_filter_eq(&chain[0], expected);
        // expect the "qualifier" filter to escape the `|` and the `$`
        let expected = &Filter::ColumnQualifierRegexFilter(br"^a\|b$|^c\$$".to_vec());
        assert_filter_eq(&chain[1], expected);
    }

    #[test]
    fn a_nested_filter_stays_nested() {
        let filter = and(vec![cells_per_column(1), columns("f", &["flow_document"])]);
        // expect the filter to be a chain of two filters
        let chain = assert_chained(&filter);
        assert_eq!(chain.len(), 2);
        // expect the first filter to be the "limit" filter
        let expected = &Filter::CellsPerColumnLimitFilter(1);
        assert_filter_eq(&chain[0], expected);
        // expect the second filter to be a nested chain of "family" and "qualifier" filters
        let nested = assert_chained(&chain[1]);
        assert_eq!(nested.len(), 2);
        // expect the first nested filter to be the "family" filter
        let expected = &Filter::FamilyNameRegexFilter("^f$".to_owned());
        assert_filter_eq(&nested[0], expected);
        // expect the second nested filter to be the "qualifier" filter
        let expected = &Filter::ColumnQualifierRegexFilter(b"^flow_document$".to_vec());
        assert_filter_eq(&nested[1], expected);
    }
}
