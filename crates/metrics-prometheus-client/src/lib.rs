mod collector;
pub mod metrics;
mod utils;

use ::metrics::set_global_recorder;

pub use self::collector::*;

/// To be called at the beginning of the program to install the metrics collector.
pub fn install() -> MetricsCollector {
    let collector = MetricsCollector::default();
    set_global_recorder(collector.clone()).unwrap();
    collector
}

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
