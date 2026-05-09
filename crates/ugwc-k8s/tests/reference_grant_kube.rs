//! Optional Kubernetes checks for ReferenceGrant handling.
//!
//! Run manually against a cluster that has Gateway API CRDs (and ugwc CRDs if applicable):
//!
//! ```text
//! cargo test -p ugwc-k8s --test reference_grant_kube -- --ignored
//! ```

#[tokio::test]
#[ignore = "requires kubeconfig and Gateway API CRDs; validate cross-namespace HTTPRoute parentRef + ReferenceGrant end-to-end"]
async fn reference_grant_cross_namespace_attachment_placeholder() {
    kube::Client::try_default()
        .await
        .expect("kubeconfig required for this ignored test");
}
