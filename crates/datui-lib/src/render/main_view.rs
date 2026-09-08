/// Determines which full-screen content is active in the main view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MainViewContent {
    /// Data table with optional sidebar and input strip.
    Datatable,
    /// Full-screen analysis modal.
    Analysis,
    /// Full-screen chart view.
    Chart,
    /// Full-screen home screen: pick a dataset.
    Home,
}

impl MainViewContent {
    /// Determine active main-view content from app state.
    pub fn from_app_state(analysis_active: bool, input_mode_chart: bool) -> Self {
        if analysis_active {
            MainViewContent::Analysis
        } else if input_mode_chart {
            MainViewContent::Chart
        } else {
            MainViewContent::Datatable
        }
    }
}

/// Control bar configuration provided by the active main view.
/// The main render loop uses this to build the Controls widget.
#[derive(Debug, Clone)]
pub enum ControlBarSpec {
    /// Default datatable controls (row count, etc.) with optional dimmed and query-active state.
    Datatable { dimmed: bool, query_active: bool },
    /// Custom keybinding list for this view (e.g. analysis or chart).
    Custom(Vec<(&'static str, &'static str)>),
}

/// Returns the control bar keybindings and options for the current main view content.
/// The main render loop calls this and applies the result to the Controls widget.
pub fn control_bar_spec(app: &crate::App, content: MainViewContent) -> ControlBarSpec {
    match content {
        MainViewContent::Datatable => {
            let query_active = app
                .data_table_state
                .as_ref()
                .map(|s| !s.active_query.trim().is_empty())
                .unwrap_or(false);
            let dimmed = app.show_help
                || app.input_mode == crate::InputMode::Editing
                || app.input_mode == crate::InputMode::SortFilter
                || app.input_mode == crate::InputMode::PivotMelt
                || app.input_mode == crate::InputMode::Info
                || app.sort_filter_modal.active;
            ControlBarSpec::Datatable {
                dimmed,
                query_active,
            }
        }
        MainViewContent::Analysis => {
            let mut pairs = vec![
                ("Esc", "Back"),
                ("↑↓", "Navigate"),
                ("←→", "Scroll Columns"),
                ("Tab", "Sidebar"),
                ("Enter", "Select"),
            ];
            if app.sampling_threshold.is_some() {
                if let Some(results) = app.analysis_modal.current_results() {
                    if results.sample_size.is_some() {
                        pairs.push(("r", "Resample"));
                    }
                }
            }
            ControlBarSpec::Custom(pairs)
        }
        MainViewContent::Chart => ControlBarSpec::Custom(vec![("Esc", "Back"), ("e", "Export")]),
        MainViewContent::Home => ControlBarSpec::Custom(home_control_keys(
            app.home.path_input_active,
            app.home.browsing.is_some(),
            !app.home.filter.is_empty(),
            app.data_table_state.is_some(),
            app.home.sort,
        )),
    }
}

/// Control bar keys for the home screen.
///
/// Split out as a pure function so the one invariant that matters can be tested: the
/// bar must never advertise plain `q` as quit. Every plain character on this screen
/// goes into the filter — `q` types a `q`, or you could not search for "quarterly" —
/// and a control bar promising otherwise leaves the user with no visible way out.
pub fn home_control_keys(
    path_input_active: bool,
    browsing: bool,
    has_filter: bool,
    has_data: bool,
    sort: crate::home::SortMode,
) -> Vec<(&'static str, &'static str)> {
    let g = crate::glyphs::get();
    let mut keys = vec![(g.updown, "Move"), (g.enter, "Open")];

    if path_input_active {
        keys.push(("Esc", "Cancel"));
    } else {
        keys.push(("type", "Filter"));
        keys.push((g.updown_lr, "Fold"));
        keys.push((g.tab, sort.label()));
        keys.push(("~", "Path"));
        if browsing {
            keys.push((g.backspace, "Up"));
        }
        // Esc peels off one layer of context at a time, so label it with what it will
        // actually do next rather than a generic "Back".
        keys.push((
            "Esc",
            if has_filter {
                "Clear"
            } else if browsing {
                "Up"
            } else if has_data {
                "Back to data"
            } else {
                "Quit"
            },
        ));
    }

    // Esc already reads "Quit" when there is nothing left to back out of; saying it
    // twice is noise.
    if !keys.iter().any(|(_, label)| *label == "Quit") {
        keys.push(("^C", "Quit"));
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::home_control_keys;

    /// Every combination of home-screen state the control bar can be drawn in.
    fn all_states() -> Vec<(bool, bool, bool, bool)> {
        let mut out = Vec::new();
        for path_input in [false, true] {
            for browsing in [false, true] {
                for filter in [false, true] {
                    for data in [false, true] {
                        out.push((path_input, browsing, filter, data));
                    }
                }
            }
        }
        out
    }

    #[test]
    fn home_bar_never_advertises_bare_q_as_quit() {
        // The bug this guards: the bar said "q Quit" while `q` typed into the filter,
        // so there was no discoverable way to leave the home screen.
        for (p, b, f, d) in all_states() {
            for (key, _) in home_control_keys(p, b, f, d, crate::home::SortMode::Natural) {
                assert_ne!(
                    key, "q",
                    "bare `q` advertised in state (path={p}, browsing={b}, filter={f}, data={d})"
                );
            }
        }
    }

    #[test]
    fn home_bar_always_offers_a_way_out() {
        for (p, b, f, d) in all_states() {
            let keys = home_control_keys(p, b, f, d, crate::home::SortMode::Natural);
            assert!(
                keys.iter().any(|(_, label)| *label == "Quit"),
                "no quit offered in state (path={p}, browsing={b}, filter={f}, data={d})"
            );
        }
    }

    #[test]
    fn home_bar_labels_esc_with_what_it_will_do() {
        // Esc escalates, so the label has to track the state rather than say "Back".
        let esc = |p, b, f, d| {
            home_control_keys(p, b, f, d, crate::home::SortMode::Natural)
                .into_iter()
                .find(|(key, _)| *key == "Esc")
                .map(|(_, label)| label)
        };
        assert_eq!(esc(false, false, true, false), Some("Clear"));
        assert_eq!(esc(false, true, false, false), Some("Up"));
        assert_eq!(esc(false, false, false, true), Some("Back to data"));
        assert_eq!(esc(false, false, false, false), Some("Quit"));
        assert_eq!(esc(true, false, false, false), Some("Cancel"));
    }

    #[test]
    fn home_bar_offers_up_only_while_browsing() {
        let has_up = |b| {
            home_control_keys(false, b, false, false, crate::home::SortMode::Natural)
                .iter()
                .any(|(_, label)| *label == "Up")
        };
        assert!(has_up(true));
        assert!(!has_up(false));
    }
}
