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
    /// Full-screen progress for a dataset that is still loading. Whatever table state
    /// exists belongs to the dataset being replaced, so none of it is shown.
    Loading,
}

impl MainViewContent {
    /// Which view is showing. The one place that decides, so the main area and the
    /// control bar at the foot of it cannot disagree about what the user is looking at.
    ///
    /// Home first: it is where you are, not an overlay. Then a load in flight, which
    /// owns the screen until it has a dataset to hand over — every other view would be
    /// drawing the dataset it is replacing.
    pub fn current(app: &crate::App) -> Self {
        if app.input_mode == crate::InputMode::Home {
            MainViewContent::Home
        } else if app.awaiting_dataset {
            MainViewContent::Loading
        } else {
            MainViewContent::from_app_state(
                app.analysis_modal.active,
                app.input_mode == crate::InputMode::Chart,
            )
        }
    }

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
    Datatable {
        dimmed: bool,
        query_active: bool,
        /// True when `q` pops to the home screen instead of quitting.
        q_pops: bool,
    },
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
                || app.input_mode == crate::InputMode::Export
                || app.input_mode == crate::InputMode::Copy
                || app.sort_filter_modal.active;
            ControlBarSpec::Datatable {
                dimmed,
                query_active,
                q_pops: app.opened_from_home,
            }
        }
        MainViewContent::Analysis => ControlBarSpec::Custom(analysis_control_keys(app)),
        MainViewContent::Chart => ControlBarSpec::Custom(chart_control_keys(app)),
        // Only the keys that survive the busy gate in `App::key`. Offering anything
        // else would be advertising something that does nothing.
        MainViewContent::Loading => ControlBarSpec::Custom(vec![
            ("^O", "Home"),
            ("?", "Help"),
            // The same meaning q carries at the table: pop or quit.
            ("q", if app.opened_from_home { "Home" } else { "Quit" }),
        ]),
        MainViewContent::Home => ControlBarSpec::Custom(home_control_keys(
            app.home.path_input_active,
            if app.home.below_browse_start() {
                Browse::BelowStart
            } else if app.home.browsing.is_some() {
                Browse::AtStart
            } else {
                Browse::Listing
            },
            !app.home.filter.is_empty(),
            app.data_table_state.is_some(),
            app.selected_directory_to_enter().is_some(),
            app.what_enter_does(),
        )),
    }
}

/// Control bar keys for the analysis screen, per view, tool and Data Quality
/// page. This is the screen's one hint surface: the widgets draw no key rows
/// of their own, and a detail view's bar describes the detail, not the view
/// it came from.
fn analysis_control_keys(app: &crate::App) -> Vec<(&'static str, &'static str)> {
    use crate::analysis_modal::{AnalysisTool, AnalysisView};
    let g = crate::glyphs::get();
    let modal = &app.analysis_modal;
    match modal.view {
        AnalysisView::DistributionDetail => {
            return vec![
                ("Esc", "Back"),
                (g.updown, "Distribution"),
                ("s", "Scale"),
                ("?", "Help"),
            ];
        }
        AnalysisView::CorrelationDetail => return vec![("Esc", "Back"), ("?", "Help")],
        AnalysisView::Main => {}
    }
    if modal.selected_tool == Some(AnalysisTool::DataQuality) {
        return data_quality_control_keys(app);
    }
    // The bar is cut from the right: while the tool list owns the keys, the
    // action that advances (Enter) must outlive column scrolling.
    let mut pairs = if modal.focus == crate::analysis_modal::AnalysisFocus::Sidebar {
        vec![
            ("Esc", "Back"),
            ("Enter", "Select"),
            (g.updown, "Navigate"),
            ("Tab", "Focus"),
            (g.updown_lr, "Scroll Columns"),
        ]
    } else {
        vec![
            ("Esc", "Back"),
            (g.updown, "Navigate"),
            (g.updown_lr, "Scroll Columns"),
            ("Tab", "Focus"),
            ("Enter", "Select"),
        ]
    };
    if app.sampling_threshold.is_some()
        && let Some(results) = app.analysis_modal.current_results()
        && results.sample_size.is_some()
    {
        pairs.push(("r", "Resample"));
    }
    pairs.push(("?", "Help"));
    pairs
}

/// The Data Quality pages' keys. Whatever owns the keys right now — a run in
/// flight, a popup, the scope editor, the plan editor — the bar says so.
fn data_quality_control_keys(app: &crate::App) -> Vec<(&'static str, &'static str)> {
    use crate::data_quality::QualityPage;
    let g = crate::glyphs::get();
    let modal = &app.analysis_modal;
    if modal.computing.is_some() {
        return vec![("Esc", "Cancel")];
    }
    if modal.data_quality_show_access {
        return vec![("Enter", "Close"), ("Esc", "Close")];
    }
    if modal.data_quality_confirm_run {
        return vec![("Enter", "Run"), ("Esc", "Cancel")];
    }
    if modal.data_quality_observation_detail {
        return vec![("Enter", "Evidence"), ("Esc", "Back")];
    }
    if modal.data_quality_page == QualityPage::Scope {
        return vec![("Enter", "Apply"), ("PgUp/PgDn", "Files"), ("Esc", "Back")];
    }
    if modal.data_quality_page == QualityPage::TimeRoles {
        return vec![
            (g.updown, "Role"),
            (g.updown_lr, "Column"),
            ("Enter", "Done"),
            ("Esc", "Back"),
        ];
    }
    if modal.data_quality_editing {
        return vec![
            (g.updown, "Field"),
            (g.updown_lr, "Value"),
            ("Enter", "Apply"),
            ("Esc", "Cancel"),
        ];
    }
    match modal.data_quality_page {
        QualityPage::Plan => vec![
            ("Enter", "Run"),
            ("e", "Edit"),
            ("p", "Access"),
            ("Tab", "Focus"),
            ("Esc", "Back"),
            ("?", "Help"),
        ],
        QualityPage::Segments => vec![
            ("[ ]", "Column"),
            ("m", "Metric"),
            ("b", "Baseline"),
            ("1-4", "Page"),
            ("e", "Plan"),
            ("Esc", "Back"),
        ],
        QualityPage::Trends => vec![
            ("[ ]", "Column"),
            ("m", "Metric"),
            ("1-4", "Page"),
            ("e", "Plan"),
            ("Esc", "Back"),
        ],
        _ => vec![
            ("1", "Overview"),
            ("2", "Columns"),
            ("3", "Segments"),
            ("4", "Trends"),
            ("Enter", "Inspect"),
            ("e", "Plan"),
            ("Tab", "Focus"),
            ("?", "Help"),
            // Every other page carries it; the overview is not the one place
            // without a way out.
            ("Esc", "Back"),
        ],
    }
}

/// Control bar keys for the chart view: what works right now, most-needed
/// first, since the bar is cut from the right.
///
/// While the column Picker is open it owns the keys, so the bar says so; the
/// rest of the time the bar leads with the direct chart-type switch and names
/// what the focused row itself takes.
fn chart_control_keys(app: &crate::App) -> Vec<(&'static str, &'static str)> {
    let g = crate::glyphs::get();
    if app.chart_export_modal.active {
        return vec![
            ("Enter", "Export"),
            ("Tab", "Next"),
            ("Esc", "Cancel"),
            ("?", "Help"),
        ];
    }
    let modal = &app.chart_modal;
    if modal.picker.is_some() {
        let mut keys = vec![("type", "Narrow"), (g.updown, "Move")];
        if modal.is_multi_row(modal.focus) {
            keys.push(("Space", "Toggle"));
            keys.push(("Enter", "Done"));
        } else {
            keys.push(("Enter", "Choose"));
        }
        keys.push(("Esc", "Back"));
        return keys;
    }
    // "Chart", not "Type": this bar also spells the key name "type" (the
    // picker's narrow chip), and the label names what 1-5 switch — the same
    // word as the `c Chart` chip that opened this screen.
    let mut keys = vec![("1-5", "Chart"), ("Tab", "Options")];
    if modal.is_picker_row(modal.focus) {
        keys.push(("Space", "Edit"));
    } else if modal.is_toggle_row(modal.focus) {
        keys.push(("Space", "Toggle"));
    } else if modal.is_number_row(modal.focus) {
        keys.push((g.updown_lr, "Adjust"));
    } else if modal.focus == crate::chart_modal::ChartFocus::Style {
        keys.push((g.updown_lr, "Style"));
    }
    keys.push(("e", "Export"));
    keys.push(("?", "Help"));
    keys.push(("Esc", "Back"));
    keys
}

/// Where the home screen is, as far as Esc is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Browse {
    /// The root listing.
    Listing,
    /// In the directory the browse began at; Esc returns to the listing.
    AtStart,
    /// Below where the browse began; Esc goes up a level.
    BelowStart,
}

/// Control bar keys for the home screen.
///
/// Split out as a pure function so the one invariant that matters can be tested: the
/// bar must never advertise plain `q` as quit. Every plain character on this screen
/// goes into the filter — `q` types a `q`, or you could not search for "quarterly" —
/// and a control bar promising otherwise leaves the user with no visible way out.
pub fn home_control_keys(
    path_input_active: bool,
    browsing: Browse,
    has_filter: bool,
    has_data: bool,
    on_a_directory: bool,
    enter: crate::WhatEnter,
) -> Vec<(&'static str, &'static str)> {
    // Named keys are spelled out — "Enter", "Tab", "Bksp" — matching the analysis and
    // chart bars, and avoiding U+23CE and U+21E5, which plenty of terminal fonts do
    // not carry. Only the arrows stay as glyphs: those are basic Arrows, present
    // everywhere, and they have no compact spelling.
    //
    // Ordered by what a narrow terminal can least afford to lose: the bar is cut from
    // the right, so the way out comes before the conveniences. At 70 columns this is
    // the difference between seeing "^C Quit" and seeing nothing about leaving.
    let g = crate::glyphs::get();
    // Enter is labelled with what it will do on *this* row, not with the word "Open". On
    // a directory whose files are not one table, Enter goes inside — and a bar saying
    // "Open" there taught the wrong thing on the first try, which is the try that forms
    // the impression. `Open all` is the promise the `(all files)` row and every directory
    // that reads as one table keep; `Inside` is what the other directories do, and the
    // same thing `→` does, so those two rows are given one chip between them below.
    let enter_says = match enter {
        crate::WhatEnter::OpensDirectory => "Open all",
        crate::WhatEnter::GoesInside => "Inside",
        crate::WhatEnter::LooksFirst => "Look",
        crate::WhatEnter::FoldsSection => "Fold",
        crate::WhatEnter::ShowsMore => "Show all",
        crate::WhatEnter::OpensFile => "Open",
        // The row only explains itself — an HTTP place has no listing to
        // browse — so the chip must not promise an Open it cannot do.
        crate::WhatEnter::Explains => "About",
        crate::WhatEnter::Nothing => "",
    };
    let mut keys = vec![("Enter", enter_says), (g.updown, "Move")];
    if enter_says.is_empty() {
        keys.remove(0);
    }

    if path_input_active {
        keys.push(("Esc", "Cancel"));
        keys.push(("Tab", "Complete"));
    } else {
        // Esc peels off one layer of context at a time, so label it with what it will
        // actually do next rather than a generic "Back". At the top level it does
        // nothing, and is not offered.
        if has_filter {
            keys.push(("Esc", "Clear"));
        } else if browsing == Browse::BelowStart {
            keys.push(("Esc", "Up"));
        } else if browsing == Browse::AtStart {
            keys.push(("Esc", "Back"));
        } else if has_data {
            keys.push(("Esc", "Back to data"));
        } else {
            // The way out takes Esc's place, so a narrow bar still shows it.
            keys.push(("^C", "Quit"));
        }
        keys.push(("type", "Filter"));
        // `~` opens the path prompt only on an empty filter; with one typed it
        // is an ordinary filter character, and the chip must not say otherwise.
        if !has_filter {
            keys.push(("~", "Path"));
        }
        if browsing != Browse::Listing {
            keys.push(("Bksp", "Up"));
        }
        // → does not fold on a directory row, it goes inside — and nothing else on screen
        // says that door exists. One chip, not two beside it: the bar is cut from the
        // right and this hint is already near that end, so `← Fold` alongside would
        // cost eleven more columns and be the first thing lost. ← still folds, and says
        // so on every other row.
        //
        // Not when Enter goes inside as well. Two chips for one outcome is the bar
        // implying a choice that is not there, and the column it costs is better spent
        // on a key that does something else.
        if on_a_directory && enter != crate::WhatEnter::GoesInside {
            keys.push((g.arrow_right, "Inside"));
        } else if !on_a_directory && browsing == Browse::Listing {
            // Only the root listing has sections to fold. The listing browsed into is
            // the whole screen, never folds, and a chip saying otherwise is a promise
            // the keys do not keep.
            keys.push((g.updown_lr, "Fold"));
        }
        keys.push((g.ctrl_updown, "Section"));
        // The key is an action; which order is currently in effect is state, and it
        // belongs with the other state at the far end of the bar rather than dressed
        // up as something to press.
        keys.push(("Tab", "Sort"));
        // `?` is the one printable that does not type into the filter — but only
        // while the filter is empty, so it is only promised then.
        if !has_filter {
            keys.push(("?", "Help"));
        }
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
    use super::{Browse, home_control_keys};

    /// Every Data Quality page's bar offers Esc: the overview was the one
    /// screen without a way out.
    #[test]
    fn every_data_quality_page_offers_a_way_out() {
        use crate::analysis_modal::AnalysisTool;
        use crate::data_quality::QualityPage;

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());
        app.analysis_modal.active = true;
        app.analysis_modal.selected_tool = Some(AnalysisTool::DataQuality);
        for page in [
            QualityPage::Plan,
            QualityPage::Scope,
            QualityPage::TimeRoles,
            QualityPage::Overview,
            QualityPage::Columns,
            QualityPage::Detail,
            QualityPage::Segments,
            QualityPage::Trends,
        ] {
            app.analysis_modal.data_quality_page = page;
            let keys = super::analysis_control_keys(&app);
            assert!(
                keys.iter().any(|(key, _)| *key == "Esc"),
                "{page:?} offers no way out"
            );
        }
    }

    /// Every combination of home-screen state the control bar can be drawn in.
    fn all_states() -> Vec<(bool, Browse, bool, bool, bool)> {
        let mut out = Vec::new();
        for path_input in [false, true] {
            for browsing in [Browse::Listing, Browse::AtStart, Browse::BelowStart] {
                for filter in [false, true] {
                    for data in [false, true] {
                        for directory in [false, true] {
                            out.push((path_input, browsing, filter, data, directory));
                        }
                    }
                }
            }
        }
        out
    }

    /// What Enter does on a row that is not a directory, for the tests that are about
    /// something else.
    const OPENS: crate::WhatEnter = crate::WhatEnter::OpensFile;

    #[test]
    fn home_bar_never_advertises_bare_q_as_quit() {
        // The bug this guards: the bar said "q Quit" while `q` typed into the filter,
        // so there was no discoverable way to leave the home screen.
        for (p, b, f, d, n) in all_states() {
            for (key, _) in home_control_keys(p, b, f, d, n, OPENS) {
                assert_ne!(
                    key, "q",
                    "bare `q` advertised in state (path={p}, browsing={b:?}, filter={f}, data={d}, directory={n})"
                );
            }
        }
    }

    #[test]
    fn home_bar_always_offers_a_way_out() {
        for (p, b, f, d, n) in all_states() {
            let keys = home_control_keys(p, b, f, d, n, OPENS);
            assert!(
                keys.iter().any(|(_, label)| *label == "Quit"),
                "no quit offered in state (path={p}, browsing={b:?}, filter={f}, data={d}, directory={n})"
            );
        }
    }

    #[test]
    fn home_bar_leads_with_the_way_out() {
        // A narrow terminal cuts the bar from the right. Whatever survives has to
        // include how to leave.
        for (p, b, f, d, n) in all_states() {
            let keys = home_control_keys(p, b, f, d, n, OPENS);
            // Esc while there is a layer to back out of; Ctrl+C at the top, where
            // Esc does nothing and is not offered.
            let way_out = keys
                .iter()
                .position(|(key, _)| *key == "Esc" || *key == "^C")
                .expect("a way out is always offered");
            assert!(
                way_out < 3,
                "the way out is {way_out} deep in state (path={p}, browsing={b:?}, filter={f}, data={d}, directory={n}); \
                 a narrow bar would cut it"
            );
        }
    }

    #[test]
    fn home_bar_labels_esc_with_what_it_will_do() {
        // Esc escalates, so the label has to track the state rather than say "Back".
        let esc = |p, b, f, d| {
            home_control_keys(p, b, f, d, false, OPENS)
                .into_iter()
                .find(|(key, _)| *key == "Esc")
                .map(|(_, label)| label)
        };
        assert_eq!(esc(false, Browse::Listing, true, false), Some("Clear"));
        assert_eq!(esc(false, Browse::BelowStart, false, false), Some("Up"));
        assert_eq!(esc(false, Browse::AtStart, false, false), Some("Back"));
        assert_eq!(
            esc(false, Browse::Listing, false, true),
            Some("Back to data")
        );
        // Nothing to back out of: Esc does nothing and is not offered.
        assert_eq!(esc(false, Browse::Listing, false, false), None);
        assert_eq!(esc(true, Browse::Listing, false, false), Some("Cancel"));
    }

    /// Enter is labelled with what it will do on this row, and the two doors are two
    /// chips only where they are two different things.
    ///
    /// A bar reading `Enter Open` on a directory Enter steps into teaches the wrong thing
    /// on the first try, and the first try is the one that forms the impression.
    #[test]
    fn home_bar_labels_enter_with_what_it_will_do() {
        let g = crate::glyphs::get();
        let bar = |directory, enter| {
            home_control_keys(false, Browse::AtStart, false, false, directory, enter)
        };
        let label = |keys: &[(&'static str, &'static str)], k: &str| {
            keys.iter().find(|(key, _)| *key == k).map(|(_, l)| *l)
        };

        // A directory that reads as one table: Enter opens all of it, → goes inside. Two
        // doors, both advertised, because they are two different outcomes.
        let one_table = bar(true, crate::WhatEnter::OpensDirectory);
        assert_eq!(label(&one_table, "Enter"), Some("Open all"));
        assert_eq!(label(&one_table, g.arrow_right), Some("Inside"));

        // A directory that is somewhere to look: Enter and → do the same thing, so the
        // bar says it once rather than implying a choice that is not there.
        let look_inside = bar(true, crate::WhatEnter::GoesInside);
        assert_eq!(label(&look_inside, "Enter"), Some("Inside"));
        assert_eq!(
            label(&look_inside, g.arrow_right),
            None,
            "one outcome, one chip: {look_inside:?}"
        );

        // A file: unchanged.
        assert_eq!(
            label(&bar(false, crate::WhatEnter::OpensFile), "Enter"),
            Some("Open")
        );
        // A row nothing has looked into says so rather than promising either.
        assert_eq!(
            label(&bar(true, crate::WhatEnter::LooksFirst), "Enter"),
            Some("Look")
        );
    }

    /// On a directory that opens as one dataset, → does not fold — it goes inside. The
    /// bar is the only thing on screen that says so.
    #[test]
    fn home_bar_offers_inside_only_on_a_dataset_directory() {
        let g = crate::glyphs::get();
        let labels = |directory| {
            home_control_keys(false, Browse::Listing, false, false, directory, OPENS)
                .into_iter()
                .collect::<Vec<_>>()
        };

        let on_directory = labels(true);
        assert!(
            on_directory.contains(&(g.arrow_right, "Inside")),
            "the door is advertised: {on_directory:?}"
        );
        assert!(
            !on_directory.iter().any(|(key, _)| *key == g.updown_lr),
            "the pair would say → folds, which it does not here: {on_directory:?}"
        );
        assert_eq!(
            on_directory.len(),
            labels(false).len(),
            "one chip in place of one, so the bar is no wider on this row than any \
             other — it is cut from the right and this hint is near that end"
        );

        let elsewhere = labels(false);
        assert!(
            !elsewhere.iter().any(|(_, label)| *label == "Inside"),
            "nothing to go inside of: {elsewhere:?}"
        );
        assert!(
            elsewhere.contains(&(g.updown_lr, "Fold")),
            "both arrows fold: {elsewhere:?}"
        );
    }

    /// While browsing, the one section on screen never folds, so the bar does not say
    /// it does.
    #[test]
    fn home_bar_offers_fold_only_on_the_root_listing() {
        let g = crate::glyphs::get();
        let has_fold = |b| {
            home_control_keys(false, b, false, false, false, OPENS)
                .iter()
                .any(|(key, _)| *key == g.updown_lr)
        };
        assert!(has_fold(Browse::Listing));
        assert!(!has_fold(Browse::AtStart));
        assert!(!has_fold(Browse::BelowStart));
    }

    #[test]
    fn home_bar_offers_up_only_while_browsing() {
        let has_up = |b| {
            home_control_keys(false, b, false, false, false, OPENS)
                .iter()
                .any(|(_, label)| *label == "Up")
        };
        assert!(has_up(Browse::AtStart));
        assert!(has_up(Browse::BelowStart));
        assert!(!has_up(Browse::Listing));
    }
}
