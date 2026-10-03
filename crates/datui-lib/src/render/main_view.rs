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
        /// True when Enter drills into the row's group rather than inspecting it.
        enter_drills: bool,
    },
    /// Custom keybinding list for this view (e.g. analysis or chart).
    Custom(Vec<(&'static str, &'static str)>),
}

/// Returns the control bar keybindings and options for the current main view content.
/// The main render loop calls this and applies the result to the Controls widget.
pub fn control_bar_spec(app: &crate::App, content: MainViewContent) -> ControlBarSpec {
    // A confirmation takes every key until it is answered, over any screen: the
    // keys underneath do nothing meanwhile.
    if app.confirmation_modal.active {
        return ControlBarSpec::Custom(crate::render::overlays::confirmation_keys());
    }
    match content {
        MainViewContent::Datatable => {
            // A surface that owns the keyboard gets a bar that describes it:
            // the table's chips advertise keys that type here, not act.
            if app.input_mode == crate::InputMode::Editing {
                return ControlBarSpec::Custom(match app.input_type {
                    Some(crate::InputType::GoToLine) => {
                        vec![("Enter", "Go"), ("F1", "Help"), ("Esc", "Cancel")]
                    }
                    Some(crate::InputType::Find) => vec![
                        ("Enter", "Find"),
                        ("^R", "Regex"),
                        ("^L", "Column"),
                        ("F1", "Help"),
                        ("Esc", "Cancel"),
                    ],
                    // In the SQL input Tab completes; the tab bar is Shift+Tab or
                    // ^T away. Alt+Enter is named beside the tabs, where it has room.
                    _ if app.query_mode == crate::QueryMode::Sql
                        && app.query_focus == crate::QueryFocus::Input =>
                    {
                        vec![
                            ("Enter", "Run"),
                            ("Tab", "Complete"),
                            ("^T", "Mode"),
                            ("F1", "Help"),
                            ("Esc", "Cancel"),
                        ]
                    }
                    _ => vec![
                        ("Enter", "Run"),
                        ("^T", "Mode"),
                        ("Tab", "Focus"),
                        ("F1", "Help"),
                        ("Esc", "Cancel"),
                    ],
                });
            }
            // Export and Copy carry their own footers; the bar keeps only the
            // globals that still act, rather than a dimmed row of untruths.
            if app.input_mode == crate::InputMode::Export
                || app.input_mode == crate::InputMode::Copy
            {
                return ControlBarSpec::Custom(vec![("^Q", "Quit"), ("Esc", "Cancel")]);
            }
            // The column picker's footer names its keys.
            if app.input_mode == crate::InputMode::GoToColumn
                || app.input_mode == crate::InputMode::PickFormat
            {
                return ControlBarSpec::Custom(vec![("^Q", "Quit"), ("Esc", "Cancel")]);
            }
            // The inspector's footer names its keys; it has nothing to cancel.
            if app.input_mode == crate::InputMode::Inspect {
                // Inside a drill, Esc steps up a level, as the footer says.
                let esc = if app.inspector_modal.drill.is_some() {
                    "Back"
                } else {
                    "Close"
                };
                return ControlBarSpec::Custom(vec![("^Q", "Quit"), ("Esc", esc)]);
            }
            // Esc stops a pivot or a view being read, like any other cancellable
            // wait. At the form only the hard escapes act meanwhile.
            if app.pivot_computing() {
                return ControlBarSpec::Custom(vec![("Esc", "Cancel"), ("^O", "Home")]);
            }
            // A find reading the view stops with Esc too.
            if app.finding() {
                return ControlBarSpec::Custom(vec![
                    ("Esc", "Cancel"),
                    ("^O", "Home"),
                    ("?", "Help"),
                    ("q", if app.opened_from_home { "Home" } else { "Quit" }),
                ]);
            }
            if app.view_applying() {
                return ControlBarSpec::Custom(vec![
                    ("Esc", "Cancel"),
                    ("^O", "Home"),
                    ("?", "Help"),
                    ("q", if app.opened_from_home { "Home" } else { "Quit" }),
                ]);
            }
            let query_active = app
                .data_table_state
                .as_ref()
                .map(|s| !s.get_active_query().trim().is_empty())
                .unwrap_or(false);
            let dimmed = app.show_help
                || app.input_mode == crate::InputMode::SortFilter
                || app.input_mode == crate::InputMode::PivotMelt
                || app.input_mode == crate::InputMode::Info
                || app.sort_filter_modal.active;
            ControlBarSpec::Datatable {
                dimmed,
                query_active,
                q_pops: app.opened_from_home,
                // From the table alone, so the chip holds still under a dimming sidebar.
                enter_drills: app
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.can_drill_down()),
            }
        }
        MainViewContent::Analysis => ControlBarSpec::Custom(analysis_control_keys(app)),
        MainViewContent::Chart => ControlBarSpec::Custom(chart_control_keys(app)),
        MainViewContent::ValueCounts => ControlBarSpec::Custom(value_counts_control_keys(app)),
        MainViewContent::Hex => ControlBarSpec::Custom(hex_control_keys(app)),
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
    // Only keys that act right now. The bar is cut from the right, so the way out
    // leads, then what the focused pane is for, then the shared sample: the bar
    // keeps its leading chips, and a sample it never names is a feature nobody
    // finds.
    let mut pairs = vec![("Esc", "Back")];
    let mut rest = Vec::new();
    if modal.focus == crate::analysis_modal::AnalysisFocus::Sidebar {
        pairs.push(("Enter", "Select"));
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
        rest.push(("Tab", "Focus"));
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
    if modal.data_quality_confirm_run {
        return vec![("Enter", "Run"), ("Esc", "Cancel")];
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
            ("Esc", "Back"),
            ("s", "Sample"),
            ("Enter", "Select"),
            (g.updown, "Tools"),
            ("Tab", "Focus"),
            ("?", "Help"),
        ];
    }
    if modal.data_quality_page == QualityPage::Setup {
        return setup_control_keys(app);
    }
    // One shape on every page: the way out, then what this page is for, then the
    // keys every page shares in one order, then the rest of this page's. The bar is
    // cut by position, so the page's own action and the sample, which every tool's
    // bar names, are what survive 80 columns; the tabs on screen name the pages.
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
        QualityPage::IntervalDetail if modal.interval_evidence().is_some() => {
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
    // A narrowed list is the first thing Esc undoes.
    let narrowed = page == QualityPage::Overview && modal.data_quality_findings.narrowed();
    let mut keys = vec![("Esc", if narrowed { "All Findings" } else { "Back" })];
    keys.extend(own.next());
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
    keys.extend([("Tab", "Focus"), ("?", "Help")]);
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

/// Setup's keys: the way out and Run first, then what the row under the cursor
/// takes. Enter is Run here and nowhere else; the lists and forms Setup opens say
/// Choose, Done or Apply. While a cancelled read finishes, Run is not offered, and
/// Setup's own line says why.
fn setup_control_keys(app: &crate::App) -> Vec<(&'static str, &'static str)> {
    use crate::analysis_modal::SetupRow;
    let g = crate::glyphs::get();
    let modal = &app.analysis_modal;
    let mut keys = vec![(
        "Esc",
        if modal.setup_edited() {
            "Discard"
        } else {
            "Back"
        },
    )];
    if app.cancelled_analysis_running().is_none() {
        keys.push(("Enter", "Run"));
    }
    let row = modal.setup_row();
    let choices = !modal.data_quality_plan.interval_pairs().is_empty();
    match row {
        SetupRow::Sample => keys.push(("Space", "Sample Form")),
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
    if view.origin == crate::hex_view::Origin::Table {
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
        if !matches!(
            modal.selected_kind(),
            Some(crate::value_counts::LineKind::Other(_)) | None
        ) {
            keys.push(("Enter", "Rows"));
        }
        // A sample's way to the exact counts comes first: it says the counts are
        // not all there is.
        if counts.is_sample() && !modal.counting() {
            keys.push(("a", "All rows"));
        }
        keys.push(("s", "Sort"));
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
    // picker's narrow chip), and the label names what 1-6 switch — the same
    // word as the `c Chart` chip that opened this screen.
    //
    // At 80 columns the row count leaves room for two chips beside Help and Esc:
    // the chart switch and what the focused row takes. Export is `e`, as at the
    // table; Tab is one of several ways down a form whose rail shows the rows.
    use crate::chart_modal::ChartFocus;
    let mut keys = vec![("1-6", "Chart")];
    // The plot has the keys: the arrows move the crosshair, Tab hands them back.
    if modal.plot_focus {
        keys.extend([
            (g.updown_lr, "Cursor"),
            ("e", "Export"),
            ("g", "Grid"),
            ("Tab", "Options"),
            ("?", "Help"),
            ("Esc", "Back"),
        ]);
        return keys;
    }
    if modal.is_picker_row(modal.focus) {
        keys.push(("Space", "Edit"));
    } else if modal.is_toggle_row(modal.focus) {
        keys.push(("Space", "Toggle"));
    } else if modal.is_number_row(modal.focus) {
        keys.push((g.updown_lr, "Adjust"));
    } else {
        match modal.focus {
            ChartFocus::Style => keys.push((g.updown_lr, "Style")),
            ChartFocus::Range => keys.push((g.updown_lr, "Range")),
            ChartFocus::Order => keys.push((g.updown_lr, "Order")),
            _ => {}
        }
    }
    keys.push(("e", "Export"));
    if modal.has_grid() {
        keys.push(("g", "Grid"));
    }
    if modal.has_crosshair() {
        keys.push(("x", "Cursor"));
    }
    keys.push(("Tab", "Options"));
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
        crate::WhatEnter::ShowsHidden => "Show",
        crate::WhatEnter::OpensFile => "Open",
        crate::WhatEnter::OpensHex => "Hex",
        // The row only explains itself — an HTTP place has no listing to
        // browse — so the chip must not promise an Open it cannot do.
        crate::WhatEnter::Explains => "About",
        crate::WhatEnter::Nothing => "",
    };
    // What a first session needs leads, the way out with it, so 80 columns show all of
    // it: Enter, that typing filters, `~` for a path, Esc, help and quit. The moves and
    // conveniences follow, and the bar is cut from the right (#547 M2).
    let mut keys = vec![("Enter", enter_says)];
    if enter_says.is_empty() {
        keys.clear();
    }

    if path_input_active {
        // What Enter does is the typed path's, not the row's under the prompt.
        keys = vec![("Enter", "Open")];
        keys.push(("Esc", "Cancel"));
        keys.push(("Tab", "Complete"));
        keys.push((g.updown, "Pick"));
    } else {
        keys.push(("type", "Filter"));
        // `~` opens the path prompt only on an empty filter; with one typed it
        // is an ordinary filter character, and the chip must not say otherwise.
        if !has_filter {
            keys.push(("~", "Path"));
        }
        // Esc peels off one layer of context at a time, so label it with what it will
        // actually do next rather than a generic "Back". At the top level it does
        // nothing, and is not offered. Back to the open data, it names where the next
        // keys will land: a reflexive Esc too many puts them on the table (#547 D14).
        if has_filter {
            keys.push(("Esc", "Clear"));
        } else if browsing == Browse::BelowStart {
            keys.push(("Esc", "Up"));
        } else if browsing == Browse::AtStart {
            keys.push(("Esc", "Back"));
        } else if has_data {
            keys.push(("Esc", "Table"));
        }
        // `?` is the one printable that does not type into the filter — but only
        // while the filter is empty, so it is only promised then.
        if !has_filter {
            keys.push(("?", "Help"));
        }
        keys.push(("^C", "Quit"));
        keys.push((g.updown, "Move"));
        if browsing != Browse::Listing {
            keys.push(("Bksp", "Up"));
        }
        // → does not fold on a directory row, it goes inside — and nothing else on screen
        // says that door exists. One chip, not two beside it: ← still folds, and says so
        // on every other row.
        //
        // Not when Enter goes inside as well. Two chips for one outcome is the bar
        // implying a choice that is not there.
        if on_a_directory && enter != crate::WhatEnter::GoesInside {
            keys.push((g.arrow_right, "Inside"));
        } else if !on_a_directory && browsing == Browse::Listing {
            // Only the root listing has sections to fold. The listing browsed into is
            // the whole screen and never folds.
            keys.push((g.updown_lr, "Fold"));
        }
        keys.push((g.ctrl_updown, "Section"));
        // The key is an action; which order is currently in effect is state, and it
        // belongs at the far end of the bar rather than dressed up as something to press.
        keys.push(("Tab", "Sort"));
    }

    // Ctrl+C quits from the path prompt too.
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
        assert_eq!(label(&app, "Enter"), Some("Select"));
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

        app.analysis_modal.selected_tool = Some(AnalysisTool::CorrelationMatrix);
        app.analysis_modal.selected_correlation = Some((1, 1));
        assert!(!has(&app, "Enter"), "a column with itself has no detail");
        app.analysis_modal.selected_correlation = Some((1, 2));
        assert_eq!(label(&app, "Enter"), Some("Detail"));
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
                way_out < 5,
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
        assert_eq!(esc(false, Browse::Listing, false, true), Some("Table"));
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

    /// At 80 columns every home state shows that typing filters, `~` where it opens the
    /// path prompt, help where `?` asks for it, and a way out; the caption yields first
    /// (#547 M2).
    #[test]
    fn home_bar_at_80_columns_keeps_what_a_first_session_needs() {
        use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
        let enters = [
            crate::WhatEnter::OpensFile,
            crate::WhatEnter::OpensDirectory,
            crate::WhatEnter::ShowsMore,
            crate::WhatEnter::GoesInside,
        ];
        for (p, b, f, d, n) in all_states() {
            for enter in enters {
                let keys = home_control_keys(p, b, f, d, n, enter);
                let controls = crate::widgets::controls::Controls::from_context(
                    0,
                    &crate::render::context::RenderContext::for_test(),
                )
                .with_custom_controls(keys)
                .with_caption(Some("by recent".to_string()))
                .with_caption_yielding(true);
                let area = Rect::new(0, 0, 80, 1);
                let mut buf = Buffer::empty(area);
                controls.render(area, &mut buf);
                let bar: String = (0..80).map(|x| buf[(x, 0)].symbol().to_string()).collect();
                let state =
                    format!("path={p}, browsing={b:?}, filter={f}, data={d}, enter={enter:?}");
                if p {
                    assert!(bar.contains("Cancel"), "{state}: {bar:?}");
                    continue;
                }
                assert!(bar.contains("type  Filter"), "{state}: {bar:?}");
                assert!(bar.contains("Quit"), "{state}: {bar:?}");
                if !f {
                    assert!(bar.contains("~  Path"), "{state}: {bar:?}");
                    assert!(bar.contains("?  Help"), "{state}: {bar:?}");
                }
                if f || b != Browse::Listing || d {
                    assert!(bar.contains("Esc"), "{state}: {bar:?}");
                }
                assert!(
                    !bar.contains("by recent"),
                    "the caption went first: {bar:?}"
                );
            }
        }
        // With room for every chip, the caption is there too.
        let keys = home_control_keys(false, Browse::Listing, false, false, false, OPENS);
        let controls = crate::widgets::controls::Controls::from_context(
            0,
            &crate::render::context::RenderContext::for_test(),
        )
        .with_custom_controls(keys)
        .with_caption(Some("by recent".to_string()))
        .with_caption_yielding(true);
        let area = Rect::new(0, 0, 200, 1);
        let mut buf = Buffer::empty(area);
        controls.render(area, &mut buf);
        let bar: String = (0..200).map(|x| buf[(x, 0)].symbol().to_string()).collect();
        assert!(bar.contains("Sort") && bar.contains("by recent"), "{bar:?}");
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
