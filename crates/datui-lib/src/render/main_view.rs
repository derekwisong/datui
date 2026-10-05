use crate::render::footer::{Hint, registry_hint, registry_hint_in};

/// Determines which full-screen content is active in the main view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MainViewContent {
    /// Data table with optional sidebar and input strip.
    Datatable,
    /// Full-screen analysis modal.
    Analysis,
    /// Full-screen chart view.
    Chart,
    /// Full-screen value counts of one column.
    ValueCounts,
    /// Full-screen bytes of one file.
    Hex,
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
        } else if app.awaiting_dataset() {
            MainViewContent::Loading
        } else if app.input_mode == crate::InputMode::Hex && app.hex.is_some() {
            MainViewContent::Hex
        } else if app.value_counts_shown() {
            MainViewContent::ValueCounts
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

/// The keys of the mode in effect, for the footer's right end. Empty at rest: the
/// footer then offers only help. A surface with its own footer (a dialog, a
/// sidebar, the inspector) names its own keys, so the footer adds none.
pub fn mode_hints(app: &crate::App, content: MainViewContent) -> Vec<Hint> {
    use datui_cli::keys::Context;
    if app.confirmation_modal.active || app.error_modal.active || app.help_visible() {
        return Vec::new();
    }
    match content {
        MainViewContent::Datatable => {
            if app.input_mode == crate::InputMode::Editing {
                return match app.input_type {
                    Some(crate::InputType::Find) => vec![
                        // The switches are on the prompt's own line, beside their state.
                        registry_hint_in(Context::Find, Some("Find"), "Enter"),
                        registry_hint(Context::Find, "Ctrl+G"),
                        registry_hint_in(Context::Find, Some("Find"), "Esc"),
                    ],
                    _ => {
                        let row = crate::editing_keys::row_number(
                            app.query_prompt_text().unwrap_or_default(),
                        )
                        .is_some();
                        let mut keys = vec![if row {
                            Hint::new("Enter", "Go")
                        } else {
                            registry_hint(Context::Query, "Enter")
                        }];
                        if !row {
                            keys.push(registry_hint(Context::Query, "Tab"));
                        }
                        if crate::QueryMode::available().len() > 1 {
                            keys.push(registry_hint(Context::Query, "Ctrl+T"));
                        }
                        keys.push(registry_hint(Context::Query, "Esc"));
                        keys
                    }
                };
            }
            // A wait the user can stop: Esc stops it, over the form that started it.
            if app.pivot_computing() || app.finding() || app.view_applying() {
                return vec![Hint::new("Esc", "Stop")];
            }
            // The builder is a takeover with no footer of its own.
            if app.input_mode == crate::InputMode::PivotMelt && app.pivot_melt_modal.active {
                return crate::widgets::pivot_melt::hints(&app.pivot_melt_modal);
            }
            if app.input_mode != crate::InputMode::Normal
                || app.sort_filter_modal.active
                || app.view_modal.active
            {
                return Vec::new();
            }
            let mut keys = Vec::new();
            if let Some(key) = app.follow_mark().and_then(|f| f.key) {
                keys.push(Hint::new("t", key));
                keys.push(Hint::new("Esc", "Stop"));
            } else if app.find_hint_shown() {
                keys.push(registry_hint_in(
                    Context::Find,
                    Some("At the table"),
                    "n / N",
                ));
                keys.push(registry_hint_in(Context::Find, Some("At the table"), "Esc"));
            } else if app.column_hints_shown() {
                keys.push(registry_hint(Context::Table, "+ / -"));
                keys.push(registry_hint(Context::Table, "[ / ]"));
                keys.push(registry_hint(Context::Table, "F"));
            } else if app
                .data_table_state
                .as_ref()
                .is_some_and(|s| s.is_drilled_down())
            {
                keys.push(Hint::new("Esc", "Back"));
            } else if app
                .data_table_state
                .as_ref()
                .is_some_and(|s| s.can_drill_down())
            {
                // A `by` view: Enter drills where it would otherwise inspect.
                keys.push(registry_hint(Context::Table, "Enter"));
            }
            let state = app.data_table_state.as_ref();
            // Read through a format spec: `b` reads it with another.
            if state.and_then(|s| s.format_read()).is_some() {
                keys.push(registry_hint(Context::Table, "b"));
            }
            // A file of several tables: `T` opens another.
            if app.offers_other_tables() {
                keys.push(registry_hint(Context::Table, "T"));
            }
            if app.app_config.display.notes_accent && state.is_some_and(|s| s.notes_unseen()) {
                let mut notes = Hint::new("i", "Notes");
                notes.accented = true;
                keys.push(notes);
            }
            keys
        }
        MainViewContent::Analysis => followed(app, screen_hints(analysis_control_keys(app))),
        MainViewContent::Chart => followed(app, chart_hints(app)),
        MainViewContent::ValueCounts => followed(app, screen_hints(value_counts_control_keys(app))),
        MainViewContent::Hex => screen_hints(hex_control_keys(app)),
        MainViewContent::Loading => vec![Hint::new("^O", "Home")],
        MainViewContent::Home => {
            // The Documentation view names its keys in its own footer.
            if app.documentation.is_open() {
                return Vec::new();
            }
            if app.home.path_input_active {
                return vec![
                    Hint::new("Enter", "Open"),
                    Hint::new("Tab", "Complete"),
                    Hint::new("Esc", "Cancel"),
                ];
            }
            // Fixed slots, each its full width whether or not the row offers it, so
            // moving the selection never moves the footer.
            let enter = Some(enter_label(app.what_enter_does())).filter(|l| !l.is_empty());
            let docs = registry_hint(Context::Home, "Ctrl+E");
            // The bundled catalog's heading has nothing for ^D; its slot offers
            // Delete, the same width: `Del Hide ` for `^D Forget`.
            let catalog = if app.home_hides_catalog() {
                Hint::new("Del", format!("{:<w$}", "Hide", w = CATALOG_SLOT - 1))
            } else {
                slot("^D", app.home_catalog_action(), CATALOG_SLOT)
            };
            vec![
                slot("Enter", enter, ENTER_SLOT),
                catalog,
                slot(
                    "^E",
                    app.home_documented_row()
                        .is_some()
                        .then_some(docs.label.as_ref()),
                    docs.label.len(),
                ),
            ]
        }
    }
}

/// A screen's keys with what `t` does there, first, while a followed file has rows
/// it has not read.
fn followed(app: &crate::App, mut keys: Vec<Hint>) -> Vec<Hint> {
    if let Some(key) = app.follow_mark().and_then(|f| f.key) {
        keys.insert(0, Hint::new("t", key));
    }
    keys
}

/// A screen's own keys in the footer: the two or three it leads with, without help,
/// which the footer always offers.
fn screen_hints(keys: Vec<(&'static str, &'static str)>) -> Vec<Hint> {
    let keys: Vec<_> = keys
        .into_iter()
        .filter(|(key, _)| !matches!(*key, "?" | "F1"))
        .collect();
    let mut shown: Vec<_> = keys.iter().take(3).copied().collect();
    // The way out stays, in the last place, wherever the screen listed it.
    if let Some(esc) = keys.iter().skip(3).find(|(key, _)| *key == "Esc")
        && !shown.iter().any(|(key, _)| *key == "Esc")
        && let Some(last) = shown.last_mut()
    {
        *last = *esc;
    }
    shown
        .into_iter()
        .map(|(key, label)| Hint::new(key, label))
        .collect()
}

/// The key that opens help, as the footer offers it: `?`, or F1 where `?` types (a
/// prompt, a filter typed on the home screen).
pub fn help_key(app: &crate::App, content: MainViewContent) -> Option<&'static str> {
    // Help waits, with every other key, while a pivot is computed at its form; a
    // question or an error answers first, and offers its own keys.
    if app.help_visible()
        || app.pivot_computing()
        || app.confirmation_modal.active
        || app.error_modal.active
    {
        return None;
    }
    let types = match content {
        MainViewContent::Datatable => {
            app.input_mode == crate::InputMode::Editing
                || (app.input_mode == crate::InputMode::PivotMelt
                    && crate::widgets::pivot_melt::question_types(&app.pivot_melt_modal))
        }
        // The Documentation view over home takes no text, whatever the filter holds.
        MainViewContent::Home => {
            !app.documentation.is_open()
                && (!app.home.filter.is_empty() || app.home.path_input_active)
        }
        MainViewContent::Hex => app.hex.as_ref().is_some_and(|v| v.prompt.is_some()),
        _ => false,
    };
    Some(if types { "F1" } else { "?" })
}

/// Columns the home screen's Enter label is given, whatever it says.
const ENTER_SLOT: usize = 8;
/// Columns Ctrl+D's label is given: `Add` or `Forget`.
const CATALOG_SLOT: usize = 6;

/// A hint in a slot of fixed width: `key label`, the label padded to `width`, or as
/// many blanks where the row does not offer the key.
fn slot(key: &'static str, label: Option<&str>, width: usize) -> Hint {
    match label {
        Some(label) => Hint::new(key, format!("{label:<width$}")),
        None => Hint::new(" ".repeat(key.len()), " ".repeat(width)),
    }
}

/// What Enter does on the home screen's row, as the footer names it.
///
/// Enter is labelled with what it will do on *this* row, not with the word "Open". On
/// a directory whose files are not one table, Enter goes inside — and a hint saying
/// "Open" there taught the wrong thing on the first try, which is the try that forms
/// the impression.
pub fn enter_label(enter: crate::WhatEnter) -> &'static str {
    match enter {
        crate::WhatEnter::OpensDirectory => "Open all",
        crate::WhatEnter::GoesInside => "Inside",
        crate::WhatEnter::LooksFirst => "Look",
        crate::WhatEnter::FoldsSection => "Fold",
        crate::WhatEnter::ShowsMore => "Show all",
        crate::WhatEnter::ShowsHidden => "Show",
        crate::WhatEnter::OpensFile => "Open",
        crate::WhatEnter::OpensHex => "Hex",
        // The row only explains itself — an HTTP place has no listing to browse —
        // so the hint must not promise an Open it cannot do.
        crate::WhatEnter::Explains => "About",
        crate::WhatEnter::Nothing => "",
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
                (g.updown, "Distribution"),
                ("s", "Scale"),
                ("?", "Help"),
                ("Esc", "Back"),
            ];
        }
        AnalysisView::CorrelationDetail => {
            return vec![("m", "Method"), ("?", "Help"), ("Esc", "Back")];
        }
        AnalysisView::Main => {}
    }
    // The Sample form owns the keys over whichever tool; its footer names the rest.
    if let Some(form) = &modal.sample_form {
        let listing = modal.focus == crate::analysis_modal::AnalysisFocus::Sidebar;
        return match (form.inline, listing) {
            // A tool's first run, from the list: Enter takes the form as it stands.
            (true, true) => vec![
                ("Enter", "Run"),
                ("Tab", "Sample"),
                (crate::glyphs::get().updown, "Tools"),
                ("?", "Help"),
                ("Esc", "Back"),
            ],
            // The form has the cursor: the bar is its only hint surface, so it names
            // what the focused row takes.
            (inline, false) | (inline @ false, _) => {
                let g = crate::glyphs::get();
                let mut keys = vec![
                    ("Enter", if inline { "Run" } else { "Apply" }),
                    (g.updown, "Row"),
                ];
                if form.field.is_text() {
                    keys.push(("type", "Edit"));
                } else {
                    keys.push((g.updown_lr, "Change"));
                }
                if form.field == crate::sample_modal::SampleField::Files
                    && form.context.files.len() > crate::widgets::sample_form::FILES_SHOWN
                {
                    keys.push(("PgUp/PgDn", "Files"));
                }
                keys.push(("Esc", if inline { "Back" } else { "Cancel" }));
                keys
            }
        };
    }
    if modal.selected_tool == Some(AnalysisTool::DataQuality) {
        return data_quality_control_keys(app);
    }
    // A run in flight owns Esc, and nothing else acts until it is done.
    if modal.computing.is_some() {
        return vec![("Esc", "Cancel")];
    }
    // Only keys that act right now, in the one chip order: primary first, the way
    // out last. The footer shows the first three with Esc kept, so what the
    // focused pane is for leads, then Tab, which is how the other pane is
    // reached; the shared sample after.
    let in_pane = modal.focus == crate::analysis_modal::AnalysisFocus::Main;
    let esc = (
        "Esc",
        if in_pane && modal.selected_tool.is_some() {
            "Tools"
        } else {
            "Close"
        },
    );
    let mut pairs = Vec::new();
    let mut rest = Vec::new();
    if !in_pane {
        pairs.push(("Enter", "Open"));
        rest.push((g.updown, "Tools"));
    } else if let Some(tool) = modal.selected_tool {
        // Enter opens a detail only where the tool has one: a column's
        // distribution, or a pair off the diagonal.
        let detail = match tool {
            AnalysisTool::DistributionAnalysis => true,
            AnalysisTool::CorrelationMatrix => modal
                .selected_correlation
                .is_some_and(|(row, col)| row != col),
            _ => false,
        };
        if detail {
            pairs.push(("Enter", "Detail"));
        }
        if matches!(tool, AnalysisTool::CorrelationMatrix) {
            rest.push(("m", "Method"));
        }
        rest.push((g.updown, "Rows"));
        // Describe and Distribution scroll only when the statistics do not all fit.
        let columns = match tool {
            AnalysisTool::CorrelationMatrix => true,
            _ => modal.column_scroll().is_some_and(|columns| columns.max > 0),
        };
        if columns {
            rest.push((g.updown_lr, "Columns"));
        }
    }
    if modal.selected_tool.is_some() {
        pairs.push(("Tab", if in_pane { "Tools" } else { "Result" }));
        pairs.push(("s", "Sample"));
        pairs.push(("v", "View Rows"));
    }
    pairs.extend(rest);
    // On a sample: another one, or every row.
    if modal.view == crate::analysis_modal::AnalysisView::Main
        && app
            .analysis_modal
            .current_results()
            .is_some_and(|results| results.sample_size.is_some())
    {
        pairs.push(("r", "Resample"));
        pairs.push(("a", "All Rows"));
    }
    pairs.push(("?", "Help"));
    pairs.push(esc);
    pairs
}

/// The Data Quality pages' keys. Whatever owns the keys right now — a run in
/// flight, a popup, a list of choices, Setup — the bar says so.
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
    if modal.data_quality_evidence_read.is_some() {
        return vec![("Enter", "Read"), ("Esc", "Cancel")];
    }
    if modal.data_quality_observation_detail {
        if modal.quality_selected_is_clean() {
            return vec![(
                "Enter",
                if modal.data_quality_checks_expanded {
                    "Fewer Checks"
                } else {
                    "All Checks"
                },
            )]
            .into_iter()
            .chain((modal.data_quality_detail_scroll.max > 0).then_some((g.updown, "Scroll")))
            .chain([("Esc", "Back")])
            .collect();
        }
        // Enter shows the rows the run kept, asks to read rows it did not keep, and
        // otherwise only closes the popup; the chip says which.
        let rows = modal.selected_finding().and_then(|(_, finding)| {
            let results = modal.data_quality_results.as_ref()?;
            let rows = finding.evidence(results).ok()?;
            Some(
                !matches!(rows, crate::quality_report::EvidenceRows::Files(_))
                    && app.quality_rows_kept().is_some(),
            )
        });
        let enter = match rows {
            Some(true) => "Show Rows",
            Some(false) => "Read Rows",
            None => "Close",
        };
        let mut keys = vec![("Enter", enter)];
        if modal.data_quality_detail_scroll.max > 0 {
            keys.push((g.updown, "Scroll"));
        }
        keys.push(("Esc", "Back"));
        return keys;
    }
    if modal.data_quality_picker.is_some() {
        return vec![
            ("Enter", "Choose"),
            (g.updown, "Move"),
            ("type", "Narrow"),
            ("Esc", "Cancel"),
        ];
    }
    if modal.data_quality_page == QualityPage::TimeRoles {
        return vec![
            (g.updown, "Role"),
            (g.updown_lr, "Column"),
            ("Enter", "Done"),
            ("Esc", "Cancel"),
        ];
    }
    if let Some(form) = modal.data_quality_export.as_ref() {
        let mut keys = vec![("Enter", "Export"), ("Tab", "Next")];
        if form.on_format {
            keys.push((g.updown_lr, "Format"));
        }
        keys.push(("Esc", "Cancel"));
        return keys;
    }
    if modal.data_quality_page == QualityPage::ExpectedWindows {
        let mut keys = vec![("Enter", "Done")];
        if modal
            .data_quality_expected_form
            .as_ref()
            .is_some_and(|form| !form.typing())
        {
            keys.push((g.updown_lr, "Windows"));
        }
        keys.extend([(g.updown, "Field"), ("Esc", "Cancel")]);
        return keys;
    }
    // The intent form over the list owns the keys: the rows, and what the focused
    // one takes.
    if let Some(form) = modal.data_quality_intent_form.as_ref() {
        use crate::intent_modal::IntentField;
        let mut keys = vec![("Enter", "Apply"), ("Tab", "Next")];
        match form.field {
            IntentField::Key | IntentField::Required => keys.push(("Space", "Toggle")),
            IntentField::ReadAs if form.time.is_none() => keys.push((g.updown_lr, "Reading")),
            _ => {}
        }
        keys.push(("Esc", "Cancel"));
        return keys;
    }
    if modal.data_quality_page == QualityPage::Intent {
        return vec![
            ("Space", "Declare"),
            ("Enter", "Done"),
            (g.updown, "Column"),
            ("Esc", "Cancel"),
        ];
    }
    if modal.data_quality_page == QualityPage::IntervalPairs {
        let mut keys = Vec::new();
        if !modal.data_quality_plan.candidate_pairs().is_empty() {
            keys.extend([("Space", "Toggle"), (g.updown, "Pair")]);
        }
        keys.extend([("Enter", "Done"), ("Esc", "Cancel")]);
        return keys;
    }
    // The tool list has the cursor, the narrow terminal's picker included: its keys
    // are the list's, not the page's. Sample stays second, as on every tool's bar.
    if modal.focus == crate::analysis_modal::AnalysisFocus::Sidebar {
        return vec![
            ("Enter", "Open"),
            ("Tab", "Result"),
            ("s", "Sample"),
            (g.updown, "Tools"),
            ("?", "Help"),
            ("Esc", "Close"),
        ];
    }
    if modal.data_quality_page == QualityPage::Setup {
        return setup_control_keys(app);
    }
    // One shape on every page: what this page is for, then the keys every page
    // shares in one order (Setup first: the plan is what a report is read
    // against), then the rest of this page's, Tab, and the way out last. The
    // footer keeps the first three with Esc; the tabs on screen name the pages.
    let page = modal.data_quality_page;
    let results = modal.data_quality_results.as_ref();
    // Column and metric pick what the segments show; with nothing split they would
    // change nothing, so they are not offered.
    let measured = modal.quality_result_plan();
    let segmented =
        results.is_some() && measured.grain != crate::data_quality::QualityGrain::Dataset;
    let trend = results.is_some_and(|results| crate::data_quality::shows_trend(measured, results));
    let mut own: Vec<(&'static str, &'static str)> = Vec::new();
    // An empty page says which plan setting fills it, and Enter opens that.
    if let Some(setup) = app.quality_page_setup() {
        own.push(("Enter", setup.label()));
    }
    match page {
        QualityPage::Overview if results.is_some() => own.extend([
            ("Enter", "Details"),
            ("c", "Column"),
            ("t", "Type"),
            ("o", modal.data_quality_findings.order.next().chip()),
        ]),
        QualityPage::Columns if results.is_some() => own.push(("Enter", "Inspect")),
        QualityPage::Detail => own.push(("Enter", "Columns")),
        QualityPage::Segments if segmented => own.extend([
            ("Enter", "Details"),
            (
                "o",
                if modal.data_quality_segments_by_change {
                    "In Order"
                } else {
                    "By Change"
                },
            ),
            ("b", "Baseline"),
        ]),
        QualityPage::SegmentDetail => own.push(("Enter", "Segments")),
        QualityPage::Trends if trend => {
            own.extend([("Enter", "Details"), ("m", "Measure")]);
            own.extend(trend_keys(modal));
        }
        QualityPage::TrendDetail => {
            own.extend([(g.updown, "Bar"), ("Enter", "Trends"), ("m", "Measure")]);
            own.extend(trend_keys(modal));
        }
        QualityPage::Gaps => own.push(("Enter", "Trends")),
        // No trend to draw, but expected windows to list.
        QualityPage::Trends => own.extend(trend_keys(modal)),
        QualityPage::Intervals if results.is_some_and(|results| !results.temporal.is_empty()) => {
            own.push(("Enter", "Details"));
        }
        // Enter opens the rows behind the count under the cursor, when it has any.
        QualityPage::IntervalDetail if modal.interval_evidence(None).is_some() => {
            own.push((
                "Enter",
                if app.quality_rows_kept().is_some() {
                    "Show Rows"
                } else {
                    "Read Rows"
                },
            ));
        }
        _ => {}
    }
    let mut own = own.into_iter();
    // A narrowed list is the first thing Esc undoes; a drill-in goes back to its
    // page, and a page to the tools.
    let narrowed = page == QualityPage::Overview && modal.data_quality_findings.narrowed();
    let top = matches!(
        page,
        QualityPage::Overview
            | QualityPage::Columns
            | QualityPage::Segments
            | QualityPage::Trends
            | QualityPage::Intervals
    );
    let esc = match (narrowed, top) {
        (true, _) => "All Findings",
        (false, true) => "Tools",
        (false, false) => "Back",
    };
    let mut keys: Vec<(&'static str, &'static str)> = own.next().into_iter().collect();
    keys.extend([
        ("e", "Setup"),
        ("s", "Sample"),
        (g.updown_lr, "Page"),
        ("v", "View Rows"),
    ]);
    if results.is_some() {
        keys.push(("x", "Export"));
    }
    keys.extend(own);
    keys.extend([("Tab", "Focus"), ("?", "Help"), ("Esc", esc)]);
    keys
}

/// The keys Trends adds when they act: a coarser window, staged in Setup, where
/// segments came out thin or unsampled; the gaps, where windows are expected.
fn trend_keys(modal: &crate::analysis_modal::AnalysisModal) -> Vec<(&'static str, &'static str)> {
    let mut keys = Vec::new();
    let Some(results) = modal.data_quality_results.as_ref() else {
        return keys;
    };
    let plan = modal.quality_result_plan();
    if plan.coarser_grain().is_some() {
        let view = crate::quality_trends::trend_view(results, modal.data_quality_metric, 1);
        let (unsampled, thin) = view.coverage();
        if view.sampled && unsampled + thin > 0 {
            keys.push(("w", "Coarser"));
        }
    }
    if crate::quality_trends::expected_gaps(plan, results).is_some() {
        keys.push(("g", "Gaps"));
    }
    keys
}

/// Setup's keys: Run first, then what the row under the cursor takes, and the
/// way out last. Enter is Run here and nowhere else; the lists and forms Setup opens say
/// Choose, Done or Apply. While a cancelled read finishes, Run is not offered, and
/// Setup's own line says why.
fn setup_control_keys(app: &crate::App) -> Vec<(&'static str, &'static str)> {
    use crate::analysis_modal::SetupRow;
    let g = crate::glyphs::get();
    let modal = &app.analysis_modal;
    let esc = (
        "Esc",
        if modal.setup_edited() {
            "Discard"
        } else {
            "Back"
        },
    );
    let mut keys = Vec::new();
    if app.cancelled_analysis_running().is_none() {
        keys.push(("Enter", "Run"));
    }
    let row = modal.setup_row();
    let choices = !modal.data_quality_plan.interval_pairs().is_empty();
    match row {
        SetupRow::Sample => keys.push(("Space", "Sample")),
        SetupRow::TextAsTime => keys.push(("Space", "Choose")),
        SetupRow::TimeRoles if !app.quality_time_candidates().is_empty() => {
            keys.push(("Space", "Time Roles"));
        }
        SetupRow::TimeRoles => {}
        SetupRow::Intervals if !modal.data_quality_plan.candidate_pairs().is_empty() => {
            keys.push(("Space", "Intervals"));
        }
        SetupRow::Intervals => {}
        SetupRow::Expected
            if matches!(
                modal.data_quality_plan.grain,
                crate::data_quality::QualityGrain::TimeWindows { .. }
            ) =>
        {
            keys.push(("Space", "Expected"));
        }
        SetupRow::Expected => {}
        SetupRow::Intent => keys.push(("Space", "Intent")),
        SetupRow::Latency if !choices => {}
        SetupRow::WindowBy if !modal.data_quality_plan.windows_intervals() => {}
        SetupRow::Grain
        | SetupRow::Compare
        | SetupRow::Values
        | SetupRow::Latency
        | SetupRow::WindowBy => {
            keys.extend([(g.updown_lr, "Change"), ("Space", "Choose")]);
        }
    }
    keys.extend([(g.updown, "Row"), ("s", "Sample"), ("p", "Access")]);
    if let Some(kept) = app.quality_kept_rows() {
        keys.push((
            "d",
            if kept.copy_bytes > 0 {
                "Release"
            } else {
                "Release Rows"
            },
        ));
    }
    keys.push(("?", "Help"));
    keys.push(esc);
    keys
}

/// Control bar keys for the chart view: what works right now, most-needed
/// first, since the bar is cut from the right.
///
/// While the column Picker is open it owns the keys, so the bar says so; the
/// rest of the time the bar leads with the direct chart-type switch and names
/// what the focused row itself takes.
/// Control bar keys for Value Counts: only those that act on what is on screen.
/// Control bar keys for the hex view: its prompt's while one is open, Esc while a
/// find reads, and otherwise the view's own, most used first.
fn hex_control_keys(app: &crate::App) -> Vec<(&'static str, &'static str)> {
    use crate::hex_view::PromptKind;
    let Some(view) = app.hex.as_ref() else {
        return vec![("?", "Help")];
    };
    if view.picker.is_some() {
        return vec![("^Q", "Quit"), ("Esc", "Cancel")];
    }
    match view.prompt {
        Some(PromptKind::Find) => {
            return vec![
                ("Enter", "Find"),
                ("^U", "UTF-16"),
                ("F1", "Help"),
                ("Esc", "Cancel"),
            ];
        }
        Some(PromptKind::GoTo) => {
            return vec![("Enter", "Go"), ("F1", "Help"), ("Esc", "Cancel")];
        }
        Some(PromptKind::RecordSize) => {
            return vec![("Enter", "Set"), ("F1", "Help"), ("Esc", "Cancel")];
        }
        None => {}
    }
    if app.finding() {
        return vec![("Esc", "Cancel"), ("^O", "Home"), ("?", "Help")];
    }
    let mut keys = vec![("f", "Find")];
    if view.found.as_ref().is_some_and(|f| f.hit.is_some()) {
        keys.push(("n", "Next"));
        keys.push(("N", "Prev"));
    }
    if view
        .found
        .as_ref()
        .and_then(|f| f.stride)
        .is_some_and(|s| (1..=crate::hex_view::MAX_RECORD_SIZE as u64).contains(&s))
        && view.record_size
            != view
                .found
                .as_ref()
                .and_then(|f| f.stride)
                .map(|s| s as usize)
    {
        keys.push(("R", "Use stride"));
    }
    keys.push((":", "Offset"));
    keys.push(("r", "Row size"));
    keys.push((
        "v",
        if view.mark.is_some() {
            "Unmark"
        } else {
            "Mark"
        },
    ));
    keys.push(("i", "Inspector"));
    keys.push(("#", if view.decimal { "Hex" } else { "Decimal" }));
    if app.has_format_specs() {
        keys.push(("B", "Format"));
    }
    keys.push(("?", "Help"));
    if matches!(
        view.origin,
        crate::hex_view::Origin::Table | crate::hex_view::Origin::Info
    ) {
        keys.push(("Esc", "Back"));
    }
    keys.push(("q", app.hex_q_label()));
    keys
}

fn value_counts_control_keys(app: &crate::App) -> Vec<(&'static str, &'static str)> {
    // The export dialog carries its own footer.
    if app.input_mode == crate::InputMode::Export {
        return vec![("^Q", "Quit"), ("Esc", "Cancel")];
    }
    let g = crate::glyphs::get();
    let modal = &app.value_counts;
    let counts = modal.current();
    let mut keys = Vec::new();
    if let Some(counts) = counts {
        if !modal.shows_histogram()
            && !matches!(
                modal.selected_kind(),
                Some(crate::value_counts::LineKind::Other(_)) | None
            )
        {
            keys.push(("Enter", "Rows"));
        }
        // A sample's way to the exact counts comes first: it says the counts are
        // not all there is.
        if counts.is_sample() && !modal.counting() {
            keys.push(("a", "All rows"));
        }
        if counts.histogram.is_some() {
            keys.push((
                "c",
                if modal.shows_histogram() {
                    "Counts"
                } else {
                    "Histogram"
                },
            ));
        }
        if !modal.shows_histogram() {
            keys.push(("s", "Sort"));
        }
        keys.push((g.updown_lr, "Column"));
        keys.push(("y", "Copy"));
        keys.push(("e", "Export"));
    } else {
        keys.push((g.updown_lr, "Column"));
    }
    keys.push(("?", "Help"));
    // With counts on screen, Esc stops a count of every row and keeps them.
    keys.push((
        "Esc",
        if counts.is_some() && modal.counting() {
            "Stop"
        } else {
            "Back"
        },
    ));
    keys
}

/// The chart screen's keys in the footer: what the focused row takes, then the
/// crosshair and export; a picker's or the plot's keys while they have them.
fn chart_hints(app: &crate::App) -> Vec<Hint> {
    use datui_cli::keys::Context;
    let g = crate::glyphs::get();
    if app.chart_export_modal.active {
        return vec![
            Hint::new("Enter", "Export"),
            Hint::new("Tab", "Next"),
            Hint::new("Esc", "Cancel"),
        ];
    }
    let modal = &app.chart_modal;
    if modal.picker.is_some() {
        return if modal.picker_multi() {
            vec![
                Hint::new("Space", "Toggle"),
                Hint::new("Enter", "Done"),
                Hint::new("Esc", "Back"),
            ]
        } else {
            vec![
                Hint::new(g.updown, "Move"),
                Hint::new("Enter", "Choose"),
                Hint::new("Esc", "Back"),
            ]
        };
    }
    // The plot has the keys: the arrows move the crosshair, Tab hands them back.
    if modal.plot_focus {
        return vec![
            Hint::new(g.updown_lr, "Cursor"),
            Hint::new("Tab", "Panel"),
            Hint::new("Esc", "Back"),
        ];
    }
    use crate::chart_modal::ChartFocus;
    let focus = modal.focus;
    // A Rows change waits for Enter; Esc puts it back.
    if focus == ChartFocus::LimitRows && modal.rows_draft.is_some() {
        return vec![
            Hint::new("Enter", "Read"),
            Hint::new(g.updown_lr, "Switch"),
            Hint::new("Esc", "Undo"),
        ];
    }
    let row = if modal.picker_for(focus).is_some() {
        Hint::new("Space", "Pick")
    } else if modal.is_toggle_row(focus) {
        Hint::new("Space", "Toggle")
    } else {
        Hint::new(
            g.updown_lr,
            match focus {
                ChartFocus::Type => "Type",
                ChartFocus::TimeUnit => "Bucket",
                ChartFocus::Aggregate => "Aggregate",
                ChartFocus::Quantile => "Percentile",
                ChartFocus::Bins | ChartFocus::Bandwidth => "Adjust",
                ChartFocus::LimitRows => "Switch",
                ChartFocus::Order => "Order",
                ChartFocus::Range => "Range",
                ChartFocus::Cumulative => "Cumulative",
                _ => "Change",
            },
        )
    };
    let mut keys = vec![row];
    if modal.has_crosshair() {
        keys.push(registry_hint(Context::Chart, "x"));
    }
    // Offered where `e` acts: a chart whose rows say what to draw.
    if app.data_table_state.is_some() && modal.can_export() {
        keys.push(registry_hint(Context::Chart, "e"));
    }
    if keys.len() < 3 {
        keys.push(registry_hint(Context::Chart, "Esc"));
    }
    keys
}

#[cfg(test)]
mod tests {

    /// `?` does nothing under a question or an error, so the footer does not
    /// offer it there.
    #[test]
    fn no_help_key_under_a_question_or_an_error() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());
        app.input_mode = crate::InputMode::Normal;
        let content = super::MainViewContent::Datatable;
        assert_eq!(super::help_key(&app, content), Some("?"));
        app.confirmation_modal
            .show("Overwrite out.csv?".to_string());
        assert_eq!(super::help_key(&app, content), None);
        app.confirmation_modal.hide();
        app.error_modal.show("Cannot read it.".to_string());
        assert_eq!(super::help_key(&app, content), None);
        app.error_modal.hide();
        assert_eq!(super::help_key(&app, content), Some("?"));
    }

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
            QualityPage::Setup,
            QualityPage::TimeRoles,
            QualityPage::IntervalPairs,
            QualityPage::ExpectedWindows,
            QualityPage::TrendDetail,
            QualityPage::Gaps,
            QualityPage::Overview,
            QualityPage::Columns,
            QualityPage::Detail,
            QualityPage::Segments,
            QualityPage::Trends,
            QualityPage::Intervals,
            QualityPage::IntervalDetail,
        ] {
            app.analysis_modal.data_quality_page = page;
            let keys = super::analysis_control_keys(&app);
            assert!(
                keys.iter().any(|(key, _)| *key == "Esc"),
                "{page:?} offers no way out"
            );
        }
    }

    /// The analysis bar offers only keys that act: Enter where a tool has a
    /// detail, ←→ where there is something to scroll to, Tab once a tool is
    /// chosen.
    #[test]
    fn the_analysis_bar_offers_only_keys_that_act() {
        use crate::analysis_modal::{AnalysisFocus, AnalysisTool};

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());
        app.analysis_modal.active = true;
        let g = crate::glyphs::get();
        let has = |app: &crate::App, key: &str| {
            super::analysis_control_keys(app)
                .iter()
                .any(|(k, _)| *k == key)
        };
        let label = |app: &crate::App, key: &str| {
            super::analysis_control_keys(app)
                .into_iter()
                .find(|(k, _)| *k == key)
                .map(|(_, label)| label)
        };

        app.analysis_modal.focus = AnalysisFocus::Sidebar;
        assert_eq!(label(&app, "Enter"), Some("Open"));
        assert_eq!(label(&app, "Esc"), Some("Close"));
        assert!(!has(&app, "Tab"), "no tool, nothing beside the list");

        app.analysis_modal.selected_tool = Some(AnalysisTool::Describe);
        app.analysis_modal.focus = AnalysisFocus::Main;
        assert!(has(&app, "Tab"));
        assert!(!has(&app, "Enter"), "Describe has no detail");
        assert!(!has(&app, g.updown_lr), "every statistic fits");
        app.analysis_modal.describe_columns.max = 2;
        assert_eq!(label(&app, g.updown_lr), Some("Columns"));

        app.analysis_modal.selected_tool = Some(AnalysisTool::DistributionAnalysis);
        assert_eq!(label(&app, "Enter"), Some("Detail"));
        assert_eq!(
            label(&app, "Esc"),
            Some("Tools"),
            "Esc goes back to the tools"
        );
        // The footer shows a screen's first three keys: Tab is one of them, from
        // either pane.
        let shown = |app: &crate::App| {
            super::screen_hints(super::analysis_control_keys(app))
                .iter()
                .map(|hint| hint.key.to_string())
                .collect::<Vec<_>>()
        };
        // One chip order: primary first, Esc last.
        assert_eq!(shown(&app), ["Enter", "Tab", "Esc"]);
        app.analysis_modal.focus = AnalysisFocus::Sidebar;
        assert_eq!(shown(&app), ["Enter", "Tab", "Esc"]);
        let keys = super::analysis_control_keys(&app);
        assert_eq!(keys.last().map(|(k, _)| *k), Some("Esc"));
        assert_eq!(label(&app, "Tab"), Some("Result"));
        app.analysis_modal.focus = AnalysisFocus::Main;

        app.analysis_modal.selected_tool = Some(AnalysisTool::CorrelationMatrix);
        app.analysis_modal.selected_correlation = Some((1, 1));
        assert!(!has(&app, "Enter"), "a column with itself has no detail");
        app.analysis_modal.selected_correlation = Some((1, 2));
        assert_eq!(label(&app, "Enter"), Some("Detail"));
        assert_eq!(label(&app, "m"), Some("Method"));
    }
}
