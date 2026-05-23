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

    #[test]
    fn empty_configmap_ca_crt_yields_no_valid_ca_certificate() {
        use k8s_openapi::api::core::v1::ConfigMap;
        use kube::runtime::watcher::Event;

        use super::super::backend_tls::policy_outcome_from_policy_stores;
        use crate::resources::{
            BackendTlsPolicySpec, BackendTlsPolicyTargetRefs, BackendTlsPolicyValidation,
            BackendTlsPolicyValidationCaCertificateRefs,
        };
        use crate::stores::SharedStores;

        let mut cm_writer = kube::runtime::reflector::store::Writer::<ConfigMap>::default();
        let cm_store = cm_writer.as_reader();
        cm_writer.apply_watcher_event(&Event::Apply(ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some("ca".into()),
                namespace: Some("ns".into()),
                ..Default::default()
            },
            data: Some(std::collections::BTreeMap::from([(
                "ca.crt".into(),
                String::new(),
            )])),
            ..Default::default()
        }));

        let mut btp_writer =
            kube::runtime::reflector::store::Writer::<crate::resources::BackendTLSPolicy>::default();
        let btp_store = btp_writer.as_reader();
        btp_writer.apply_watcher_event(&Event::Apply(
            crate::resources::BackendTLSPolicy {
                metadata: kube::api::ObjectMeta {
                    name: Some("pol".into()),
                    namespace: Some("ns".into()),
                    ..Default::default()
                },
                spec: BackendTlsPolicySpec {
                    target_refs: vec![BackendTlsPolicyTargetRefs {
                        group: String::new(),
                        kind: "Service".into(),
                        name: "svc".into(),
                        section_name: None,
                    }],
                    validation: BackendTlsPolicyValidation {
                        hostname: "host".into(),
                        ca_certificate_refs: Some(vec![
                            BackendTlsPolicyValidationCaCertificateRefs {
                                group: String::new(),
                                kind: "ConfigMap".into(),
                                name: "ca".into(),
                            },
                        ]),
                        ..Default::default()
                    },
                    options: None,
                },
                ..Default::default()
            },
        ));

        let ep = kube::runtime::reflector::store::Writer::<
            k8s_openapi::api::discovery::v1::EndpointSlice,
        >::default();
        let svc = kube::runtime::reflector::store::Writer::<
            k8s_openapi::api::core::v1::Service,
        >::default();
        let sec = kube::runtime::reflector::store::Writer::<
            k8s_openapi::api::core::v1::Secret,
        >::default();
        let ns = kube::runtime::reflector::store::Writer::<
            k8s_openapi::api::core::v1::Namespace,
        >::default();
        let stores = SharedStores::new(
            ep.as_reader(),
            svc.as_reader(),
            sec.as_reader(),
            cm_store,
            ns.as_reader(),
        );
        stores.set_backend_tls_policy_store(btp_store);

        let pol = stores.get_backend_tls_policy("ns", "pol").unwrap();
        let outcome = policy_outcome_from_policy_stores(&stores, &pol, &[], false);
        assert!(!outcome.accepted);
        assert_eq!(
            outcome.accepted_condition_reason.as_deref(),
            Some("NoValidCACertificate")
        );
    }
}
