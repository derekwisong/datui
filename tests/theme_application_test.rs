use datui::config::{AppConfig, Theme};
use datui::{App, AppEvent};
use std::sync::mpsc::channel;

#[test]
fn test_app_accepts_theme() {
    let config = AppConfig::default();
    let theme =
        Theme::from_config(&config.theme).expect("Failed to create theme from default config");

    let (tx, _rx) = channel::<AppEvent>();
    let app = App::new_with_theme(tx, theme);

    // Just verify the app was created successfully with a theme
    assert!(app.data_table_state.is_none()); // No data loaded yet
}

#[test]
fn test_theme_with_custom_colors() {
    let mut config = AppConfig::default();
    config.theme.colors.primary = "#ff0000".to_string();
    config.theme.colors.error = "bright_red".to_string();
    config.theme.colors.success = "bright_green".to_string();

    let theme =
        Theme::from_config(&config.theme).expect("Failed to create theme with custom colors");

    let (tx, _rx) = channel::<AppEvent>();
    let _app = App::new_with_theme(tx, theme);

    // App created successfully with custom theme
}

#[test]
fn test_default_app_has_theme() {
    let (tx, _rx) = channel::<AppEvent>();
    let app = App::new(tx);

    // App should have been created with default theme
    assert!(app.data_table_state.is_none());
}

#[test]
fn test_theme_color_retrieval() {
    let config = AppConfig::default();
    let theme = Theme::from_config(&config.theme).unwrap();

    // Test that we can retrieve colors
    let primary = theme.get("primary");
    let error = theme.get("error");
    let success = theme.get("success");

    // Colors should not be Reset (unless NO_COLOR is set)
    use ratatui::style::Color;
    if std::env::var("NO_COLOR").is_err() {
        assert_ne!(primary, Color::Reset);
        assert_ne!(error, Color::Reset);
        assert_ne!(success, Color::Reset);
    }
}
