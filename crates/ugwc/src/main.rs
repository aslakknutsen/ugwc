//! Gateway API control plane entry point. Wired in later phases; see
//! `design/implementation-plan.md` Phase 0–1.

fn main() {
    // Phase 0: keep workspace library crates linked in the build graph.
    let _ = (
        ugwc_core::placeholder(),
        ugwc_k8s::kube_placeholder(),
        ugwc_xds::xds_placeholder(),
    );

    let mut args = std::env::args();
    let bin = args.next().unwrap_or_default();
    if args.next().as_deref() == Some("--version") {
        println!(
            "ugwc {} (commit {})",
            env!("CARGO_PKG_VERSION"),
            option_env!("UGWC_GIT_COMMIT").unwrap_or("unknown")
        );
        return;
    }
    eprintln!(
        "{bin}: Gateway API control plane — scaffold only; use --version"
    );
}
