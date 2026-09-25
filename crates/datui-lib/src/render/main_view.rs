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
                (crate::glyphs::get().updown, "Navigate"),
                (crate::glyphs::get().updown_lr, "Scroll Columns"),
                ("Tab", "Sidebar"),
                ("Enter", "Select"),
            ];
            if app.sampling_threshold.is_some()
                && let Some(results) = app.analysis_modal.current_results()
                && results.sample_size.is_some()
            {
                pairs.push(("r", "Resample"));
            }
            ControlBarSpec::Custom(pairs)
        }
        MainViewContent::Chart => ControlBarSpec::Custom(vec![("Esc", "Back"), ("e", "Export")]),
        // Only the keys that survive the busy gate in `App::key`. Offering anything
        // else would be advertising something that does nothing.
        MainViewContent::Loading => {
            ControlBarSpec::Custom(vec![("^O", "Home"), ("?", "Help"), ("q", "Quit")])
        }
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
            app.selected_folder_to_enter().is_some(),
            app.what_enter_does(),
        )),
    }
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
    on_a_folder: bool,
    enter: crate::WhatEnter,
) -> Vec<(&'static str, &'static str)> {
    // Named keys are spelled out — "Enter", "Tab", "Bksp" — matching the analysis and
    // chart bars, and avoiding U+23CE and U+21E5, which plenty of terminal fonts do
    // not carry. Only the arrows stay as glyphs: those are basic Arrows, present
    // everywhere, and they have no compact spelling.
    //
    // Ordered by what a narrow terminal can least afford to lose: the bar is cut from
    // the right, so the way out comes before the conveniences. At 70 columns this is
    // the difference between seeing "Esc Quit" and seeing nothing about leaving.
    let g = crate::glyphs::get();
    // Enter is labelled with what it will do on *this* row, not with the word "Open".
    // On a folder whose files are not one table, Enter goes inside — and a bar saying
    // "Open" there taught the wrong thing on the first try, which is the try that forms
    // the impression. `Open all` is the promise the `(all files)` row and every folder
    // that reads as one table keep; `Inside` is what the other folders do, and the same
    // thing `→` does, so those two rows are given one chip between them below.
    let enter_says = match enter {
        crate::WhatEnter::OpensFolder => "Open all",
        crate::WhatEnter::GoesInside => "Inside",
        crate::WhatEnter::LooksFirst => "Look",
        crate::WhatEnter::OpensFile | crate::WhatEnter::Other => "Open",
    };
    let mut keys = vec![("Enter", enter_says), (g.updown, "Move")];

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
        keys.push(("~", "Path"));
        if browsing != Browse::Listing {
            keys.push(("Bksp", "Up"));
        }
        // → does not fold on a folder row, it goes inside — and nothing else on screen
        // says that door exists. One chip, not two beside it: the bar is cut from the
        // right and this hint is already near that end, so `← Fold` alongside would
        // cost eleven more columns and be the first thing lost. ← still folds, and says
        // so on every other row.
        //
        // Not when Enter goes inside as well. Two chips for one outcome is the bar
        // implying a choice that is not there, and the column it costs is better spent
        // on a key that does something else.
        if on_a_folder && enter != crate::WhatEnter::GoesInside {
            keys.push((g.arrow_right, "Inside"));
        } else if !on_a_folder && browsing == Browse::Listing {
            // Only the root listing has sections to fold. The listing browsed into is
            // the whole screen, never folds, and a chip saying otherwise is a promise
            // the keys do not keep.
            keys.push((g.updown_lr, "Fold"));
        }
        keys.push(("^↑↓", "Section"));
        // The key is an action; which order is currently in effect is state, and it
        // belongs with the other state at the far end of the bar rather than dressed
        // up as something to press.
        keys.push(("Tab", "Sort"));
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

    /// Every combination of home-screen state the control bar can be drawn in.
    fn all_states() -> Vec<(bool, Browse, bool, bool, bool)> {
        let mut out = Vec::new();
        for path_input in [false, true] {
            for browsing in [Browse::Listing, Browse::AtStart, Browse::BelowStart] {
                for filter in [false, true] {
                    for data in [false, true] {
                        for folder in [false, true] {
                            out.push((path_input, browsing, filter, data, folder));
                        }
                    }
                }
            }
        }
        out
    }

    /// What Enter does on a row that is not a folder, for the tests that are about
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
                    "bare `q` advertised in state (path={p}, browsing={b:?}, filter={f}, data={d}, folder={n})"
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
                "no quit offered in state (path={p}, browsing={b:?}, filter={f}, data={d}, folder={n})"
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
                "the way out is {way_out} deep in state (path={p}, browsing={b:?}, filter={f}, data={d}, folder={n}); \
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
    /// A bar reading `Enter Open` on a folder Enter steps into teaches the wrong thing
    /// on the first try, and the first try is the one that forms the impression.
    #[test]
    fn home_bar_labels_enter_with_what_it_will_do() {
        let g = crate::glyphs::get();
        let bar =
            |folder, enter| home_control_keys(false, Browse::AtStart, false, false, folder, enter);
        let label = |keys: &[(&'static str, &'static str)], k: &str| {
            keys.iter().find(|(key, _)| *key == k).map(|(_, l)| *l)
        };

        // A folder that reads as one table: Enter opens all of it, → goes inside. Two
        // doors, both advertised, because they are two different outcomes.
        let one_table = bar(true, crate::WhatEnter::OpensFolder);
        assert_eq!(label(&one_table, "Enter"), Some("Open all"));
        assert_eq!(label(&one_table, g.arrow_right), Some("Inside"));

        // A folder that is somewhere to look: Enter and → do the same thing, so the bar
        // says it once rather than implying a choice that is not there.
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

    /// On a folder that opens as one dataset, → does not fold — it goes inside. The bar
    /// is the only thing on screen that says so.
    #[test]
    fn home_bar_offers_inside_only_on_a_dataset_folder() {
        let g = crate::glyphs::get();
        let labels = |folder| {
            home_control_keys(false, Browse::Listing, false, false, folder, OPENS)
                .into_iter()
                .collect::<Vec<_>>()
        };

        let on_folder = labels(true);
        assert!(
            on_folder.contains(&(g.arrow_right, "Inside")),
            "the door is advertised: {on_folder:?}"
        );
        assert!(
            !on_folder.iter().any(|(key, _)| *key == g.updown_lr),
            "the pair would say → folds, which it does not here: {on_folder:?}"
        );
        assert_eq!(
            on_folder.len(),
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
