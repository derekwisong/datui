/// This test binary is one cargo built into `deps`, so the refusal in
/// `CacheManager::new` is armed here. It guards the integration tests, which link
/// the library without `cfg(test)`: one that reaches `new` without
/// `DATUI_CACHE_DIR` set stops instead of writing its fixtures into the
/// developer's own recents.
#[test]
fn a_cargo_test_binary_is_recognized() {
    assert!(super::running_as_a_cargo_test());
}

/// A unit test that builds a manager with nothing set up, alone in its process
/// as nextest runs it, gets scratch directories: not the developer's own, and not
/// the refusal.
#[test]
fn a_unit_test_needs_no_setup_to_isolate() {
    let cache = super::CacheManager::new(crate::APP_NAME).unwrap();
    let config = crate::config::ConfigManager::new(crate::APP_NAME).unwrap();
    let scratch = std::env::temp_dir();
    assert!(cache.cache_dir().starts_with(&scratch), "{cache:?}");
    let config = config.config_dir();
    assert!(config.starts_with(&scratch), "{config:?}");
}

/// Only cargo's own layout counts. A program someone installed under a directory
/// called `deps` must not refuse to start with a message about the test harness.
#[test]
fn only_cargos_layout_is_a_test() {
    use std::ffi::OsStr;
    use std::path::Path;
    let layout = |exe: &str| super::cargo_test_layout(Path::new(exe), None);
    assert!(layout(
        "/home/x/src/datui/target/debug/deps/home_test-1a2b3c"
    ));
    assert!(layout(
        "/home/x/src/datui/target/x86_64-unknown-linux-gnu/release/deps/datui-1a2b"
    ));
    assert!(!layout("/home/x/src/datui/target/debug/datui"));
    assert!(!layout("/opt/deps/bin/datui"));
    assert!(!layout("/home/x/deps/datui-0.4/bin/datui"));
    assert!(!layout("/home/x/src/datui/target/debug/examples/demo"));
    // A target directory of another name, when cargo was told about it.
    assert!(!layout("/home/x/build/datui/debug/deps/home_test-1a2b3c"));
    assert!(super::cargo_test_layout(
        Path::new("/home/x/build/datui/debug/deps/home_test-1a2b3c"),
        Some(OsStr::new("/home/x/build/datui"))
    ));
    assert!(!super::cargo_test_layout(
        Path::new("/home/x/build/deps/datui"),
        Some(OsStr::new("/home/x/other"))
    ));
}
