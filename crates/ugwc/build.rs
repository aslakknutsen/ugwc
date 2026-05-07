fn main() {
    let commit = std::env::var("UGWC_GIT_COMMIT").unwrap_or_else(|_| {
        std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "unknown".into())
    });
    println!("cargo:rustc-env=UGWC_BUILD_COMMIT={commit}");
    println!("cargo:rerun-if-env-changed=UGWC_GIT_COMMIT");
}
