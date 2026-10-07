use crate::render::footer::{Hint, registry_hint, registry_hint_as, registry_hint_in};
use datui_cli::keys::Context;

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
    /// footer at the foot of it cannot disagree about what the user is looking at.
    ///
    /// Home first: it is where you are, not an overlay. Then a load in flight, which
    /// owns the screen until it has a dataset to hand over — every other view would be
    /// drawing the dataset it is replacing.
    pub fn current(app: &crate::App) -> Self {
        if app.input_mode == crate::InputMode::Home {
            MainViewContent::Home
        } else if app.awaiting_dataset() {
            MainViewContent::Loading
        } else if app.input_mode == crate::InputMode::Hex && app.hex_view.view.is_some() {
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
    if app.confirmation_modal.active || app.error_modal.active || app.help_visible() {
        return Vec::new();
    }
    match content {
        MainViewContent::Datatable => {
            if app.input_mode == crate::InputMode::Editing {
                return match app.prompt.input_type {
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
                            registry_hint_as(Context::Query, None, "Enter", "Go")
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
            if app.input_mode == crate::InputMode::Sample
                && let Some(form) = &app.sample.form
            {
                return sample_form_hints(form);
            }
            // A wait the user can stop: Esc stops it, over the form that started it.
            if app.pivot_computing()
                || app.finding()
                || app.view_applying()
                || (app.sample_drawing() && app.in_normal_table_view())
            {
                return vec![stop()];
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
                keys.push(registry_hint_as(Context::Table, None, "t", key));
                keys.push(stop());
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
                keys.push(registry_hint(Context::Table, "Esc"));
            } else if app
                .data_table_state
                .as_ref()
                .is_some_and(|s| s.can_drill_down())
            {
                // A `by` view: Enter drills where it would otherwise inspect.
                keys.push(registry_hint(Context::Table, "Enter"));
            }
            let state = app.data_table_state.as_ref();
            // A view's chart waits: `c` draws it.
            if app.chart.modal.restored {
                keys.push(registry_hint(Context::Table, "c"));
            }
            // Read through a format spec: `b` reads it with another.
            if state.and_then(|s| s.format_read()).is_some() {
                keys.push(registry_hint(Context::Table, "b"));
            }
            // A file of several tables: `T` opens another.
            if app.offers_other_tables() {
                keys.push(registry_hint(Context::Table, "T"));
            }
            if app.app_config.display.notes_accent && state.is_some_and(|s| s.notes_unseen()) {
                keys.push(registry_hint_as(Context::Table, None, "i", "Notes").accented());
            }
            keys
        }
        MainViewContent::Analysis => followed(app, screen_hints(analysis_control_keys(app))),
        MainViewContent::Chart => followed(app, chart_hints(app)),
        MainViewContent::ValueCounts => followed(app, screen_hints(value_counts_control_keys(app))),
        MainViewContent::Hex => screen_hints(hex_control_keys(app)),
        MainViewContent::Loading => vec![registry_hint(Context::Global, "Ctrl+O")],
        MainViewContent::Home => {
            // The Documentation view names its keys in its own footer.
            if app.info.documentation.is_open() {
                return Vec::new();
            }
            if app.home.path_input_active {
                return vec![
                    registry_hint(Context::Home, "Enter"),
                    registry_hint_as(Context::Home, None, "Tab", "Complete"),
                    registry_hint_as(Context::Home, None, "Esc", "Cancel"),
                ];
            }
            // Fixed slots, each its full width whether or not the row offers it, so
            // moving the selection never moves the footer.
            let enter = Some(enter_label(app.what_enter_does())).filter(|l| !l.is_empty());
            // The bundled catalog's heading has nothing for ^D; its slot offers
            // Delete, the same width: `Del Hide ` for `^D Forget`.
            let catalog = if app.home_hides_catalog() {
                slot("Delete", Some("Hide"), CATALOG_SLOT - 1)
            } else {
                slot("Ctrl+D", app.home_catalog_action(), CATALOG_SLOT)
            };
            let docs = registry_hint(Context::Home, "Ctrl+E");
            let width = docs.label.len();
            let docs = docs.padded(width);
            vec![
                slot("Enter", enter, ENTER_SLOT),
                catalog,
                if app.home_documented_row().is_some() {
                    docs
                } else {
                    docs.blank(width)
                },
            ]
        }
    }
}

/// Esc stopping a wait at the table.
fn stop() -> Hint {
    registry_hint_as(Context::Table, Some("Go"), "Esc", "Stop")
}

/// The table's Sample form: what Enter does as the form stands, then the keys the
/// focused row takes.
fn sample_form_hints(form: &crate::sample_modal::SampleForm) -> Vec<Hint> {
    let enter = if form.no_sample() {
        "Clear"
    } else if form.anyway {
        "Draw anyway"
    } else {
        "Draw"
    };
    let key = |keys| registry_hint(Context::Sample, keys);
    let mut keys = vec![
        registry_hint_as(Context::Sample, None, "Enter", enter),
        registry_hint_as(Context::Sample, None, "↑ / ↓", "Row"),
    ];
    keys.push(if form.field.is_text() {
        key("(type)")
    } else {
        key("← / →")
    });
    keys.push(key("Esc"));
    keys
}

/// A screen's keys with what `t` does there, first, while a followed file has rows
/// it has not read.
fn followed(app: &crate::App, mut keys: Vec<Hint>) -> Vec<Hint> {
    if let Some(key) = app.follow_mark().and_then(|f| f.key) {
        keys.insert(0, registry_hint_as(app.keys_context(), None, "t", key));
    }
    keys
}

/// A screen's own keys in the footer: the two or three it leads with, without help,
/// which the footer always offers.
fn screen_hints(keys: Vec<Hint>) -> Vec<Hint> {
    let mut shown: Vec<Hint> = keys.iter().take(3).cloned().collect();
    // The way out stays, in the last place, wherever the screen listed it.
    if let Some(esc) = keys.iter().skip(3).find(|hint| hint.key == "Esc")
        && !shown.iter().any(|hint| hint.key == "Esc")
        && let Some(last) = shown.last_mut()
    {
        *last = esc.clone();
    }
    shown
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
            !app.info.documentation.is_open()
                && (!app.home.filter.is_empty() || app.home.path_input_active)
        }
        MainViewContent::Hex => app
            .hex_view
            .view
            .as_ref()
            .is_some_and(|v| v.prompt.is_some()),
        _ => false,
    };
    Some(if types { "F1" } else { "?" })
}

/// Columns the home screen's Enter label is given, whatever it says.
const ENTER_SLOT: usize = 8;
/// Columns Ctrl+D's label is given: `Add` or `Forget`.
const CATALOG_SLOT: usize = 6;

/// A home hint in a slot of fixed width: the key and `label`, padded to `width`, or
/// as many blanks where the row does not offer the key.
fn slot(keys: &str, label: Option<&'static str>, width: usize) -> Hint {
    match label {
        Some(label) => registry_hint_as(Context::Home, None, keys, label).padded(width),
        None => registry_hint(Context::Home, keys).blank(width),
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
        crate::WhatEnter::GoesUp => "Up",
        crate::WhatEnter::ShowsHidden => "Show",
        crate::WhatEnter::OpensFile => "Open",
        crate::WhatEnter::OpensHex => "Hex",
        // The row only explains itself — an HTTP place has no listing to browse —
        // so the hint must not promise an Open it cannot do.
        crate::WhatEnter::Explains => "About",
        crate::WhatEnter::Nothing => "",
    }
}

/// Footer keys for the analysis screen, per view, tool and Data Quality
/// page. This is the screen's one hint surface: the widgets draw no key rows
/// of their own, and a detail view's bar describes the detail, not the view
/// it came from.
fn analysis_control_keys(app: &crate::App) -> Vec<Hint> {
    use crate::analysis_modal::{AnalysisTool, AnalysisView};
    let modal = &app.analysis_modal;
    let context = app.keys_context();
    let key = |keys| registry_hint(context, keys);
    let say = |keys, label| registry_hint_as(context, None, keys, label);
    match modal.view {
        AnalysisView::DistributionDetail => return vec![key("↑ / ↓"), key("s"), key("Esc")],
        AnalysisView::CorrelationDetail => return vec![key("m"), key("Esc")],
        AnalysisView::Main => {}
    }
    // The Sample form owns the keys over whichever tool; its footer names the rest.
    if let Some(form) = &modal.sample_form {
        let listing = modal.focus == crate::analysis_modal::AnalysisFocus::Sidebar;
        let sample = |keys| registry_hint(Context::Sample, keys);
        let sample_as = |keys, label| registry_hint_as(Context::Sample, None, keys, label);
        return match (form.inline, listing) {
            // A tool's first run, from the list: Enter takes the form as it stands.
            (true, true) => vec![
                sample_as("Enter", "Run"),
                sample_as("Tab", "Sample"),
                sample_as("↑ / ↓", "Tools"),
                sample_as("Esc", "Back"),
            ],
            // The form has the cursor: the bar is its only hint surface, so it names
            // what the focused row takes.
            (inline, false) | (inline @ false, _) => {
                let mut keys = vec![
                    sample_as("Enter", if inline { "Run" } else { "Apply" }),
                    sample_as("↑ / ↓", "Row"),
                ];
                keys.push(if form.field.is_text() {
                    sample("(type)")
                } else {
                    sample("← / →")
                });
                if form.field == crate::sample_modal::SampleField::Files
                    && form.context.files.len() > crate::widgets::sample_form::FILES_SHOWN
                {
                    keys.push(sample("PgUp / PgDn"));
                }
                keys.push(sample_as("Esc", if inline { "Back" } else { "Cancel" }));
                keys
            }
        };
    }
    if modal.selected_tool == Some(AnalysisTool::DataQuality) {
        return data_quality_control_keys(app);
    }
    // A run in flight owns Esc, and nothing else acts until it is done.
    if modal.computing.is_some() {
        return vec![say("Esc", "Cancel")];
    }
    // Only keys that act right now, in the one chip order: primary first, the way
    // out last. The footer shows the first three with Esc kept, so what the
    // focused pane is for leads, then Tab, which is how the other pane is
    // reached; the shared sample after.
    let in_pane = modal.focus == crate::analysis_modal::AnalysisFocus::Main;
    let esc = say(
        "Esc",
        if in_pane && modal.selected_tool.is_some() {
            "Tools"
        } else {
            "Close"
        },
    );
    let mut keys = Vec::new();
    let mut rest = Vec::new();
    if !in_pane {
        keys.push(say("Enter", "Open"));
        rest.push(say("↑ / ↓", "Tools"));
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
            keys.push(say("Enter", "Detail"));
        }
        if matches!(tool, AnalysisTool::CorrelationMatrix) {
            rest.push(key("m"));
        }
        rest.push(say("↑ / ↓", "Rows"));
        // Describe and Distribution scroll only when the statistics do not all fit.
        let columns = match tool {
            AnalysisTool::CorrelationMatrix => true,
            _ => modal.column_scroll().is_some_and(|columns| columns.max > 0),
        };
        if columns {
            rest.push(say("← / →", "Columns"));
        }
    }
    if modal.selected_tool.is_some() {
        keys.push(say("Tab", if in_pane { "Tools" } else { "Result" }));
        keys.push(key("s"));
        keys.push(key("v"));
    }
    keys.extend(rest);
    // On a sample: another one, or every row.
    if modal.view == crate::analysis_modal::AnalysisView::Main
        && app
            .analysis_modal
            .current_results()
            .is_some_and(|results| results.sample_size.is_some())
    {
        keys.push(key("r"));
        keys.push(key("a"));
    }
    keys.push(esc);
    keys
}

/// The Data Quality pages' keys. Whatever owns the keys right now — a run in
/// flight, a popup, a list of choices, Setup — the bar says so.
fn data_quality_control_keys(app: &crate::App) -> Vec<Hint> {
    use crate::data_quality::QualityPage;
    let modal = &app.analysis_modal;
    let dq = |group, keys| registry_hint_in(Context::DataQuality, Some(group), keys);
    let dq_as =
        |group, keys, label| registry_hint_as(Context::DataQuality, Some(group), keys, label);
    if modal.computing.is_some() {
        return vec![dq_as("Report", "Esc", "Cancel")];
    }
    if modal.quality.show_access {
        return vec![dq("Popups", "Enter"), dq_as("Popups", "Esc", "Close")];
    }
    if modal.quality.evidence_read.is_some() {
        return vec![
            dq_as("Popups", "Enter", "Read"),
            dq_as("Popups", "Esc", "Cancel"),
        ];
    }
    if modal.quality.observation_detail {
        let enter = if modal.quality_selected_is_clean() {
            if modal.quality.checks_expanded {
                "Fewer checks"
            } else {
                "All checks"
            }
        } else {
            // Enter shows the rows the run kept, asks to read rows it did not keep,
            // and otherwise only closes the popup; the chip says which.
            let rows = modal.selected_finding().and_then(|(_, finding)| {
                let results = modal.quality.results.as_ref()?;
                let rows = finding.evidence(results).ok()?;
                Some(
                    !matches!(rows, crate::quality_report::EvidenceRows::Files(_))
                        && app.quality_rows_kept().is_some(),
                )
            });
            match rows {
                Some(true) => "Show rows",
                Some(false) => "Read rows",
                None => "Close",
            }
        };
        let mut keys = vec![dq_as("Popups", "Enter", enter)];
        if modal.quality.detail_scroll.max > 0 {
            keys.push(dq("Popups", "↑ / ↓"));
        }
        keys.push(dq("Popups", "Esc"));
        return keys;
    }
    if modal.quality.picker.is_some() {
        return vec![
            dq_as("Popups", "Enter", "Choose"),
            dq_as("Popups", "↑ / ↓", "Move"),
            dq("Popups", "(type)"),
            dq_as("Popups", "Esc", "Cancel"),
        ];
    }
    let lists = |up_down, left_right: Option<&'static str>| {
        let mut keys = vec![dq_as("Setup lists", "↑ / ↓", up_down)];
        keys.extend(left_right.map(|label| dq_as("Setup lists", "← / →", label)));
        keys
    };
    let done = || {
        vec![
            dq("Setup lists", "Enter"),
            dq_as("Setup lists", "Esc", "Cancel"),
        ]
    };
    if modal.quality.page == QualityPage::TimeRoles {
        return [lists("Role", Some("Column")), done()].concat();
    }
    if let Some(form) = modal.quality.export.as_ref() {
        let mut keys = vec![dq_as("Forms", "Enter", "Export"), dq("Forms", "Tab")];
        if form.on_format {
            keys.push(dq_as("Forms", "← / →", "Format"));
        }
        keys.push(dq("Forms", "Esc"));
        return keys;
    }
    if modal.quality.page == QualityPage::ExpectedWindows {
        let mut keys = vec![dq("Setup lists", "Enter")];
        if modal
            .quality
            .expected_form
            .as_ref()
            .is_some_and(|form| !form.typing())
        {
            keys.push(dq_as("Setup lists", "← / →", "Windows"));
        }
        keys.extend([
            dq_as("Setup lists", "↑ / ↓", "Field"),
            dq_as("Setup lists", "Esc", "Cancel"),
        ]);
        return keys;
    }
    // The intent form over the list owns the keys: the rows, and what the focused
    // one takes.
    if let Some(form) = modal.quality.intent_form.as_ref() {
        use crate::intent_modal::IntentField;
        let mut keys = vec![dq("Forms", "Enter"), dq("Forms", "Tab")];
        match form.field {
            IntentField::Key | IntentField::Required => keys.push(dq("Forms", "Space")),
            IntentField::ReadAs if form.time.is_none() => {
                keys.push(dq_as("Forms", "← / →", "Reading"));
            }
            _ => {}
        }
        keys.push(dq("Forms", "Esc"));
        return keys;
    }
    if modal.quality.page == QualityPage::Intent {
        return [
            vec![
                dq_as("Setup lists", "Space", "Declare"),
                dq("Setup lists", "Enter"),
            ],
            lists("Column", None),
            vec![dq_as("Setup lists", "Esc", "Cancel")],
        ]
        .concat();
    }
    if modal.quality.page == QualityPage::IntervalPairs {
        let mut keys = Vec::new();
        if !modal.quality.plan.candidate_pairs().is_empty() {
            keys.push(dq("Setup lists", "Space"));
            keys.extend(lists("Pair", None));
        }
        keys.extend(done());
        return keys;
    }
    // The tool list has the cursor, the narrow terminal's picker included: its keys
    // are the list's, not the page's. Sample stays second, as on every tool's bar.
    if modal.focus == crate::analysis_modal::AnalysisFocus::Sidebar {
        return vec![
            dq_as("Report", "Enter", "Open"),
            dq_as("Report", "Tab", "Result"),
            dq("Report", "s"),
            dq_as("Report", "↑ / ↓", "Tools"),
            dq_as("Report", "Esc", "Close"),
        ];
    }
    if modal.quality.page == QualityPage::Setup {
        return setup_control_keys(app);
    }
    // One shape on every page: what this page is for, then the keys every page
    // shares in one order (Setup first: the plan is what a report is read
    // against), then the rest of this page's, Tab, and the way out last. The
    // footer keeps the first three with Esc; the tabs on screen name the pages.
    let page = modal.quality.page;
    let results = modal.quality.results.as_ref();
    // Column and metric pick what the segments show; with nothing split they would
    // change nothing, so they are not offered.
    let measured = modal.quality_result_plan();
    let segmented =
        results.is_some() && measured.grain != crate::data_quality::QualityGrain::Dataset;
    let trend = results.is_some_and(|results| crate::data_quality::shows_trend(measured, results));
    let mut own: Vec<Hint> = Vec::new();
    // An empty page says which plan setting fills it, and Enter opens that.
    if let Some(setup) = app.quality_page_setup() {
        own.push(dq_as("Report", "Enter", setup.label()));
    }
    let rows = if app.quality_rows_kept().is_some() {
        "Show rows"
    } else {
        "Read rows"
    };
    match page {
        QualityPage::Overview if results.is_some() => own.extend([
            dq("Report", "Enter"),
            dq_as("Report", "c", "Column"),
            dq_as("Report", "t", "Type"),
            dq_as("Report", "o", modal.quality.findings.order.next().chip()),
        ]),
        QualityPage::Columns if results.is_some() => own.push(dq_as("Report", "Enter", "Inspect")),
        QualityPage::Detail => own.push(dq_as("Report", "Enter", "Columns")),
        QualityPage::Segments if segmented => own.extend([
            dq("Segments", "Enter"),
            dq_as(
                "Segments",
                "o",
                if modal.quality.segments_by_change {
                    "In order"
                } else {
                    "By change"
                },
            ),
            dq("Segments", "b"),
        ]),
        QualityPage::SegmentDetail => own.push(dq_as("Report", "Enter", "Segments")),
        QualityPage::Trends if trend => {
            own.extend([dq("Trends", "Enter"), dq("Trends", "m")]);
            own.extend(trend_keys(modal));
        }
        QualityPage::TrendDetail => {
            own.extend([
                dq_as("Report", "↑ / ↓", "Bar"),
                dq_as("Trends", "Enter", "Trends"),
                dq("Trends", "m"),
            ]);
            own.extend(trend_keys(modal));
        }
        QualityPage::Gaps => own.push(dq_as("Trends", "Enter", "Trends")),
        // No trend to draw, but expected windows to list.
        QualityPage::Trends => own.extend(trend_keys(modal)),
        QualityPage::Intervals if results.is_some_and(|results| !results.temporal.is_empty()) => {
            own.push(dq("Intervals", "Enter"));
        }
        // Enter opens the rows behind the count under the cursor, when it has any.
        QualityPage::IntervalDetail if modal.interval_evidence(None).is_some() => {
            own.push(dq_as("Intervals", "Enter", rows));
        }
        _ => {}
    }
    let mut own = own.into_iter();
    // A narrowed list is the first thing Esc undoes; a drill-in goes back to its
    // page, and a page to the tools.
    let narrowed = page == QualityPage::Overview && modal.quality.findings.narrowed();
    let top = matches!(
        page,
        QualityPage::Overview
            | QualityPage::Columns
            | QualityPage::Segments
            | QualityPage::Trends
            | QualityPage::Intervals
    );
    let esc = match (narrowed, top) {
        (true, _) => "All findings",
        (false, true) => "Tools",
        (false, false) => "Back",
    };
    let mut keys: Vec<Hint> = own.next().into_iter().collect();
    keys.extend([
        dq("Report", "e"),
        dq("Report", "s"),
        dq("Report", "← / →"),
        dq("Report", "v"),
    ]);
    if results.is_some() {
        keys.push(dq("Report", "x"));
    }
    keys.extend(own);
    keys.extend([dq("Report", "Tab"), dq_as("Report", "Esc", esc)]);
    keys
}

/// The keys Trends adds when they act: a coarser window, staged in Setup, where
/// segments came out thin or unsampled; the gaps, where windows are expected.
fn trend_keys(modal: &crate::analysis_modal::AnalysisModal) -> Vec<Hint> {
    let mut keys = Vec::new();
    let Some(results) = modal.quality.results.as_ref() else {
        return keys;
    };
    let plan = modal.quality_result_plan();
    if plan.coarser_grain().is_some() {
        let (sampled, unsampled, thin) = crate::quality_trends::segment_coverage(results);
        if sampled && unsampled + thin > 0 {
            keys.push(registry_hint_in(Context::DataQuality, Some("Trends"), "w"));
        }
    }
    if crate::quality_trends::expected_gaps(plan, results).is_some() {
        keys.push(registry_hint_in(Context::DataQuality, Some("Trends"), "g"));
    }
    keys
}

/// Setup's keys: Run first, then what the row under the cursor takes, and the
/// way out last. Enter is Run here and nowhere else; the lists and forms Setup opens say
/// Choose, Done or Apply. While a cancelled read finishes, Run is not offered, and
/// Setup's own line says why.
fn setup_control_keys(app: &crate::App) -> Vec<Hint> {
    use crate::analysis_modal::SetupRow;
    let modal = &app.analysis_modal;
    let key = |keys| registry_hint_in(Context::DataQuality, Some("Setup"), keys);
    let say = |keys, label| registry_hint_as(Context::DataQuality, Some("Setup"), keys, label);
    let mut keys = Vec::new();
    if app.cancelled_analysis_running().is_none() {
        keys.push(key("Enter"));
    }
    let row = modal.setup_row();
    let choices = !modal.quality.plan.interval_pairs().is_empty();
    match row {
        SetupRow::Sample => keys.push(say("Space", "Sample")),
        SetupRow::TextAsTime => keys.push(say("Space", "Choose")),
        SetupRow::TimeRoles if !app.quality_time_candidates().is_empty() => {
            keys.push(say("Space", "Time roles"));
        }
        SetupRow::TimeRoles => {}
        SetupRow::Intervals if !modal.quality.plan.candidate_pairs().is_empty() => {
            keys.push(say("Space", "Intervals"));
        }
        SetupRow::Intervals => {}
        SetupRow::Expected
            if matches!(
                modal.quality.plan.grain,
                crate::data_quality::QualityGrain::TimeWindows { .. }
            ) =>
        {
            keys.push(say("Space", "Expected"));
        }
        SetupRow::Expected => {}
        SetupRow::Intent => keys.push(say("Space", "Intent")),
        SetupRow::Latency if !choices => {}
        SetupRow::WindowBy if !modal.quality.plan.windows_intervals() => {}
        SetupRow::Grain
        | SetupRow::Compare
        | SetupRow::Values
        | SetupRow::Latency
        | SetupRow::WindowBy => {
            keys.extend([say("← / →", "Change"), say("Space", "Choose")]);
        }
    }
    keys.extend([say("↑ / ↓", "Row"), key("s"), key("p")]);
    if let Some(kept) = app.quality_kept_rows() {
        keys.push(if kept.copy_bytes > 0 {
            key("d")
        } else {
            say("d", "Release rows")
        });
    }
    keys.push(if modal.setup_edited() {
        key("Esc")
    } else {
        say("Esc", "Back")
    });
    keys
}

/// Footer keys for the hex view: its prompt's while one is open, Esc while a
/// find reads, and otherwise the view's own, most used first.
fn hex_control_keys(app: &crate::App) -> Vec<Hint> {
    use crate::hex_view::PromptKind;
    let key = |keys| registry_hint(Context::Hex, keys);
    let say = |keys, label| registry_hint_as(Context::Hex, None, keys, label);
    let prompt = |keys| registry_hint_in(Context::Hex, Some("Prompt"), keys);
    let prompt_as = |keys, label| registry_hint_as(Context::Hex, Some("Prompt"), keys, label);
    let Some(view) = app.hex_view.view.as_ref() else {
        return Vec::new();
    };
    if view.picker.is_some() {
        return vec![
            registry_hint(Context::Global, "Ctrl+Q"),
            registry_hint(Context::FormatPicker, "Esc"),
        ];
    }
    match view.prompt {
        Some(PromptKind::Find) => {
            return vec![prompt("Enter"), prompt("Ctrl+U"), prompt("Esc")];
        }
        Some(PromptKind::GoTo) => return vec![prompt_as("Enter", "Go"), prompt("Esc")],
        Some(PromptKind::RecordSize) => return vec![prompt_as("Enter", "Set"), prompt("Esc")],
        None => {}
    }
    if app.finding() {
        return vec![
            say("Esc", "Cancel"),
            registry_hint(Context::Global, "Ctrl+O"),
        ];
    }
    let mut keys = vec![key("f")];
    if view.found.as_ref().is_some_and(|f| f.hit.is_some()) {
        keys.push(say("n", "Next"));
        keys.push(say("N", "Prev"));
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
        keys.push(say("R", "Use stride"));
    }
    keys.push(say(":", "Offset"));
    keys.push(key("r"));
    keys.push(if view.mark.is_some() {
        say("v", "Unmark")
    } else {
        key("v")
    });
    keys.push(key("i"));
    keys.push(say("#", if view.decimal { "Hex" } else { "Decimal" }));
    if app.has_format_specs() {
        keys.push(say("B", "Format"));
    }
    if matches!(
        view.origin,
        crate::hex_view::Origin::Table | crate::hex_view::Origin::Info
    ) {
        keys.push(registry_hint_in(Context::Hex, Some("Go"), "Esc"));
    }
    keys.push(say("q", app.hex_q_label()));
    keys
}

/// Footer keys for Value Counts: only those that act on what is on screen.
fn value_counts_control_keys(app: &crate::App) -> Vec<Hint> {
    let key = |keys| registry_hint(Context::ValueCounts, keys);
    let say = |keys, label| registry_hint_as(Context::ValueCounts, None, keys, label);
    // The export dialog carries its own footer.
    if app.input_mode == crate::InputMode::Export {
        return vec![
            registry_hint(Context::Global, "Ctrl+Q"),
            registry_hint_in(Context::Export, Some("Form"), "Esc"),
        ];
    }
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
            keys.push(say("Enter", "Rows"));
        }
        // A sample's way to the exact counts comes first: it says the counts are
        // not all there is.
        if counts.is_sample() && !modal.counting() {
            keys.push(key("a"));
        }
        if counts.histogram.is_some() {
            keys.push(if modal.shows_histogram() {
                say("c", "Counts")
            } else {
                key("c")
            });
        }
        if !modal.shows_histogram() {
            keys.push(key("s"));
        }
        keys.extend([key("← / →"), key("y"), key("e")]);
    } else {
        keys.push(key("← / →"));
    }
    // With counts on screen, Esc stops a count of every row and keeps them.
    keys.push(if counts.is_some() && modal.counting() {
        say("Esc", "Stop")
    } else {
        key("Esc")
    });
    keys
}

/// The chart screen's keys in the footer: what the focused row takes, then the
/// crosshair and export; a picker's or the plot's keys while they have them.
fn chart_hints(app: &crate::App) -> Vec<Hint> {
    let in_group = |group, keys| registry_hint_in(Context::Chart, Some(group), keys);
    let say = |group, keys, label| registry_hint_as(Context::Chart, Some(group), keys, label);
    if app.chart.export_modal.active {
        return ["Enter", "Tab", "Esc"]
            .into_iter()
            .map(|keys| in_group("Export dialog", keys))
            .collect();
    }
    let modal = &app.chart.modal;
    if modal.picker.is_some() {
        return if modal.picker_multi() {
            vec![
                say("Picker", "Space", "Toggle"),
                say("Picker", "Enter", "Done"),
                in_group("Picker", "Esc"),
            ]
        } else {
            vec![
                in_group("Picker", "↑ / ↓"),
                in_group("Picker", "Enter"),
                in_group("Picker", "Esc"),
            ]
        };
    }
    // The plot has the keys: the arrows move the crosshair, Tab hands them back.
    if modal.plot_focus {
        return ["← / →", "Tab", "Esc"]
            .into_iter()
            .map(|keys| in_group("Crosshair", keys))
            .collect();
    }
    use crate::chart_modal::ChartFocus;
    let focus = modal.focus;
    // A Rows change waits for Enter; Esc puts it back.
    if focus == ChartFocus::LimitRows && modal.rows_pending() {
        return vec![
            say("Shelves", "Enter", "Read"),
            say("Shelves", "← / →", "Switch"),
            say("Shelves", "Esc", "Undo"),
        ];
    }
    let row = if modal.picker_for(focus).is_some() {
        say("Shelves", "Space", "Pick")
    } else if modal.is_toggle_row(focus) {
        say("Shelves", "Space", "Toggle")
    } else {
        say(
            "Shelves",
            "← / →",
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
        app.confirmation_modal.show(
            "Overwrite out.csv?".to_string(),
            crate::feedback::Confirm::ClearRecents,
        );
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
            app.analysis_modal.quality.page = page;
            let keys = super::analysis_control_keys(&app);
            assert!(
                keys.iter().any(|hint| hint.key == "Esc"),
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
                .any(|hint| hint.key == key)
        };
        let label = |app: &crate::App, key: &str| {
            super::analysis_control_keys(app)
                .into_iter()
                .find(|hint| hint.key == key)
                .map(|hint| hint.label.to_string())
        };

        app.analysis_modal.focus = AnalysisFocus::Sidebar;
        assert_eq!(label(&app, "Enter").as_deref(), Some("Open"));
        assert_eq!(label(&app, "Esc").as_deref(), Some("Close"));
        assert!(!has(&app, "Tab"), "no tool, nothing beside the list");

        app.analysis_modal.selected_tool = Some(AnalysisTool::Describe);
        app.analysis_modal.focus = AnalysisFocus::Main;
        assert!(has(&app, "Tab"));
        assert!(!has(&app, "Enter"), "Describe has no detail");
        assert!(!has(&app, g.updown_lr), "every statistic fits");
        app.analysis_modal.describe_columns.max = 2;
        assert_eq!(label(&app, g.updown_lr).as_deref(), Some("Columns"));

        app.analysis_modal.selected_tool = Some(AnalysisTool::DistributionAnalysis);
        assert_eq!(label(&app, "Enter").as_deref(), Some("Detail"));
        assert_eq!(
            label(&app, "Esc").as_deref(),
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
        assert_eq!(keys.last().map(|hint| hint.key.as_ref()), Some("Esc"));
        assert_eq!(label(&app, "Tab").as_deref(), Some("Result"));
        app.analysis_modal.focus = AnalysisFocus::Main;

        app.analysis_modal.selected_tool = Some(AnalysisTool::CorrelationMatrix);
        app.analysis_modal.selected_correlation = Some((1, 1));
        assert!(!has(&app, "Enter"), "a column with itself has no detail");
        app.analysis_modal.selected_correlation = Some((1, 2));
        assert_eq!(label(&app, "Enter").as_deref(), Some("Detail"));
        assert_eq!(label(&app, "m").as_deref(), Some("Method"));
    }
}
