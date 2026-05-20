#[cfg(test)]
mod backend_tls_loader_tests {
    use super::super::backend_tls::backend_tls_section_applies;

    #[test]
    fn section_none_applies_to_all_named_service_ports() {
        assert!(backend_tls_section_applies(None, "http"));
        assert!(backend_tls_section_applies(None, "https"));
    }

    #[test]
    fn section_name_matches_only_that_service_port_name() {
        assert!(backend_tls_section_applies(Some("https"), "https"));
        assert!(!backend_tls_section_applies(Some("https"), "http"));
    }

    #[test]
    fn backend_tls_winner_sort_prefers_older_creation_timestamp() {
        let mut candidates = vec![
            ("2024-06-02T10:00:00Z", "newer"),
            ("2024-06-01T10:00:00Z", "older"),
        ];
        candidates.sort_by(|a, b| a.0.cmp(b.0));
        assert_eq!(candidates[0].1, "older");
    }

    #[test]
    fn backend_tls_winner_sort_tie_breaker_lexicographic_policy_identity() {
        let mut candidates = vec![
            ("2024-06-01T10:00:00Z", "ns-a", "policy-z"),
            ("2024-06-01T10:00:00Z", "ns-a", "policy-a"),
        ];
        candidates.sort_by(|a, b| a.0.cmp(b.0).then_with(|| (a.1, a.2).cmp(&(b.1, b.2))));
        assert_eq!(candidates[0].2, "policy-a");
    }
}
