#![cfg(feature = "embedded-database")]

#[allow(dead_code, unused_imports)]
mod suite {
    include!("support/synchronization/common.rs");

    mod tests {
        use super::*;
        include!("support/synchronization/activity_retention.rs");
    }
}
