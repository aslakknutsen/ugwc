//! Encode ugwc IR into Praxis native YAML.
//!
//! See [`encode::encode`] and the crate-level design notes in
//! `design/output-distribution.md`.

pub mod encode;
pub mod model;

pub use encode::{
    ADMIN_METRICS_ADDRESS, CONFIG_FILE_NAME, CONFIG_MOUNT_DIR, ConfigFile, EncodeOutput,
    TLS_MOUNT_DIR, Unsupported,
};
pub use model::PraxisConfig;
