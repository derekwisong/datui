//! The User-Agent header on every request datui makes: ureq's, and the object
//! stores' (ours and the ones Polars builds for a scan).
//!
//! It names datui, its version and where to read about it, so a publisher whose
//! logs show datui can find out what it is. Nothing about the user or the machine.
//! `[http] user_agent` replaces it.

use std::sync::RwLock;

/// What datui sends unless `[http] user_agent` says otherwise.
pub const DEFAULT: &str = concat!(
    "datui/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/derekwisong/datui)"
);

/// The run's `[http] user_agent`, set once its configuration is read. A process
/// global because the clients are built deep in workers that hold no config.
static CONFIGURED: RwLock<Option<String>> = RwLock::new(None);

/// The header for a `[http] user_agent` setting: the setting, or the default when it
/// is blank.
pub fn resolve(setting: &str) -> String {
    let setting = setting.trim();
    if setting.is_empty() {
        DEFAULT.to_string()
    } else {
        setting.to_string()
    }
}

/// Whether `value` can be sent as a header: printable ASCII, so neither ureq nor
/// object_store refuses it at request time.
pub fn is_valid(value: &str) -> bool {
    value.chars().all(|c| (' '..='~').contains(&c))
}

/// Use `setting` (`[http] user_agent`) from now on in this process.
pub fn configure(setting: &str) {
    let value = setting.trim();
    *CONFIGURED.write().unwrap_or_else(|e| e.into_inner()) =
        (!value.is_empty()).then(|| value.to_string());
}

/// The header to send now.
pub fn get() -> String {
    match CONFIGURED
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .as_deref()
    {
        Some(value) => value.to_string(),
        None => DEFAULT.to_string(),
    }
}

/// A ureq agent configuration with the header set; each client adds its own limits.
#[cfg(any(feature = "http", feature = "cloud"))]
pub fn ureq_config() -> ureq::config::ConfigBuilder<ureq::typestate::AgentScope> {
    ureq::Agent::config_builder().user_agent(get())
}

/// The object_store client key that carries the header, for a builder's `with_config`
/// or Polars' `CloudOptions`.
#[cfg(feature = "cloud")]
pub const CLIENT_KEY: object_store::ClientConfigKey = object_store::ClientConfigKey::UserAgent;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_names_datui_its_version_and_home_and_nothing_else() {
        assert_eq!(
            DEFAULT,
            format!(
                "datui/{} (+https://github.com/derekwisong/datui)",
                env!("CARGO_PKG_VERSION")
            )
        );
        assert!(is_valid(DEFAULT));
    }

    #[test]
    fn a_blank_setting_is_the_default_and_anything_else_replaces_it() {
        assert_eq!(resolve(""), DEFAULT);
        assert_eq!(resolve("   "), DEFAULT);
        assert_eq!(resolve(" research-crawler/2 "), "research-crawler/2");
    }

    #[test]
    fn control_characters_are_not_a_header() {
        assert!(is_valid("my-tool/1.0 (+https://example.com)"));
        assert!(!is_valid("a\r\nX-Injected: 1"));
        assert!(!is_valid("café"));
    }
}
