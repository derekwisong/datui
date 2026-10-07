use super::*;
use crate::analysis_modal::{AnalysisFocus, DetailScroll, SetupRow};
use crate::config::Theme;
use crate::data_quality::fixtures::measure;
use crate::data_quality::{
    DataQualityPlan, DataQualityResults, QualityCompute, QualityMetric, QualityPage,
};
use crate::quality_export::ExportForm;
use crate::quality_intent::{ColumnIntent, DeclaredIntent};
use crate::render::context::RenderContext;
use crate::table::DataTableState;
use crate::widgets::data_quality::{SetupView, render};
use polars::prelude::*;
use std::sync::Arc;

fn frame() -> LazyFrame {
    let rows = 2_000i64;
    df!(
            "id" => (0..rows).map(|row| row % 1_500).collect::<Vec<_>>(),
            "status" => (0..rows).map(|row| ["open", "closed", "void"][row as usize % 3]).collect::<Vec<_>>(),
            "amount" => (0..rows).map(|row| (row % 130) as f64 - 10.0).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy()
}

struct Screen {
    state: DataTableState,
    plan: DataQualityPlan,
    results: DataQualityResults,
    theme: Theme,
    ctx: RenderContext,
    findings: crate::quality_report::FindingsView,
}

/// What a draw puts over the page: the intent form or the export dialog.
#[derive(Default)]
struct Over<'a> {
    intent: Option<&'a IntentForm>,
    export: Option<&'a ExportForm>,
    access: bool,
}

impl Screen {
    fn new(compute: QualityCompute) -> Self {
        let lf = frame();
        let schema = Arc::new((*lf.clone().collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            lf.clone(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        let plan = DataQualityPlan {
            compute,
            dataset_rows: 500,
            intent: DeclaredIntent {
                key: vec!["id".to_string()],
                columns: vec![
                    ColumnIntent {
                        required: true,
                        allowed: vec!["open".to_string(), "closed".to_string()],
                        ..ColumnIntent::new("status")
                    },
                    ColumnIntent {
                        min: Some("0".to_string()),
                        max: Some("100".to_string()),
                        ..ColumnIntent::new("amount")
                    },
                ],
            },
            ..DataQualityPlan::default()
        };
        let results = measure(&lf, Some(2_000), &plan);
        Self {
            state,
            plan,
            results,
            theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
            ctx: RenderContext::for_test(),
            findings: crate::quality_report::FindingsView::default(),
        }
    }

    fn draw(&self, page: QualityPage, field: usize, over: Over<'_>, size: (u16, u16)) -> String {
        let config = DataQualityWidgetConfig {
            checks_expanded: false,
            state: &self.state,
            plan: &self.plan,
            measured: &self.plan,
            results: Some(&self.results),
            from_cache: false,
            metric: QualityMetric::NullRate,
            column_index: 0,
            segment_index: 0,
            interval_index: 0,
            trend_line: 0,
            expected_form: None,
            segments_by_change: false,
            page,
            setup: SetupView::default(),
            plan_field: field,
            show_access: over.access,
            observation_detail: false,
            focus: AnalysisFocus::Main,
            theme: &self.theme,
            ctx: &self.ctx,
            findings: &self.findings,
            rows_kept: true,
            evidence_read: None,
            intent_form: over.intent,
            export_form: over.export,
        };
        let (width, height) = size;
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        let mut table = TableState::default();
        let mut sidebar = TableState::default();
        render(
            config,
            &mut table,
            &mut sidebar,
            &mut DetailScroll::default(),
            area,
            &mut buf,
        );
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Every character outside ASCII is a glyph slot, which `LANG=C` swaps for its
/// ASCII twin, or the frame.
fn assert_glyph_slots(text: &str) {
    let g = glyphs::get();
    let slots = [
        g.rail,
        g.rule_h,
        g.rule_h_focused,
        g.middot,
        g.ellipsis,
        g.selector,
        g.checkbox_on,
        g.checkbox_off,
        g.warning,
        g.check,
        g.arrow_left,
        g.arrow_right,
    ]
    .concat();
    for c in text.chars().filter(|c| !c.is_ascii()) {
        assert!(
            slots.contains(c) || "╭╮╰╯│─".contains(c),
            "{c:?} is not a glyph slot:\n{text}"
        );
    }
}

const SIZES: [(u16, u16); 2] = [(80, 24), (60, 20)];

/// The list names each column, its type and what it must hold, with the key
/// under it; the form takes the rows the type takes. Both fit 80x24 and 60x20.
#[test]
fn the_intent_list_and_form_fit_80x24_and_60x20() {
    let screen = Screen::new(QualityCompute::Sample);
    let mut form = IntentForm::new(
        "amount",
        DataType::Float64,
        None,
        &screen.plan.intent,
        &screen.theme,
    );
    form.field = IntentField::Minimum;
    for size in SIZES {
        let text = screen.draw(QualityPage::Intent, 2, Over::default(), size);
        for expected in ["Column intent", "Must hold", "key", "0 to 100", "Key: id"] {
            assert!(text.contains(expected), "{expected} at {size:?}:\n{text}");
        }
        assert_glyph_slots(&text);

        let over = Over {
            intent: Some(&form),
            ..Over::default()
        };
        let text = screen.draw(QualityPage::Intent, 2, over, size);
        for expected in [
            "Intent: amount",
            "Key:",
            "Required:",
            "Minimum:",
            "Maximum:",
            "100",
            "A number · empty for no bound",
        ] {
            assert!(text.contains(expected), "{expected} at {size:?}:\n{text}");
        }
        assert!(!text.contains("Allowed:"), "a float takes no set:\n{text}");
        assert_glyph_slots(&text);
    }
    // A refused Enter says why on the form's own line.
    form.error = Some("Minimum is above maximum".to_string());
    let over = Over {
        intent: Some(&form),
        ..Over::default()
    };
    let text = screen.draw(QualityPage::Intent, 2, over, (80, 24));
    assert!(text.contains("Minimum is above maximum"), "{text}");
}

/// Setup names the declared intent on its row, and the Read section and the
/// access plan say what it costs: nothing past the sample, and a key that speaks
/// only for the sampled rows; on a full scan, the key's own pass.
#[test]
fn setup_discloses_what_intent_costs() {
    let screen = Screen::new(QualityCompute::Sample);
    let field = SetupRow::Intent.index();
    for size in SIZES {
        let text = screen.draw(QualityPage::Setup, field, Over::default(), size);
        assert!(text.contains("Column intent"), "{text}");
        assert!(text.contains("key id"), "{text}");
        assert_glyph_slots(&text);
    }
    let text = screen.draw(QualityPage::Setup, field, Over::default(), (120, 50));
    assert!(
        text.contains("Column intent: on the rows read · no extra read"),
        "{text}"
    );
    assert!(
        text.contains("Key: repeats among the 500 sampled rows only"),
        "{text}"
    );
    let over = Over {
        access: true,
        ..Over::default()
    };
    let text = screen.draw(QualityPage::Setup, field, over, (100, 30));
    assert!(text.contains("Column intent"), "{text}");
    assert!(text.contains("sampled rows only"), "{text}");

    let full = Screen::new(QualityCompute::Full);
    let text = full.draw(QualityPage::Setup, field, Over::default(), (120, 50));
    assert!(text.contains("key adds 1 pass"), "{text}");
}

/// The report lists the declared rules' violations as problems, and the export
/// dialog fits over it at both sizes.
#[test]
fn the_report_and_export_dialog_fit_80x24_and_60x20() {
    let screen = Screen::new(QualityCompute::Full);
    let mut export = ExportForm::new("orders", &screen.theme);
    for size in SIZES {
        let text = screen.draw(QualityPage::Overview, 0, Over::default(), size);
        for title in ["Repeated key", "Not allowed", "Out of range"] {
            assert!(text.contains(title), "{title} at {size:?}:\n{text}");
        }
        assert_glyph_slots(&text);
        let over = Over {
            export: Some(&export),
            ..Over::default()
        };
        let text = screen.draw(QualityPage::Overview, 0, over, size);
        for expected in [
            "Export Report",
            "Path:",
            "orders-quality.json",
            "Format:",
            "JSON",
        ] {
            assert!(text.contains(expected), "{expected} at {size:?}:\n{text}");
        }
        assert_glyph_slots(&text);
    }
    export.error = Some("Type a path to write to".to_string());
    let over = Over {
        export: Some(&export),
        ..Over::default()
    };
    let text = screen.draw(QualityPage::Overview, 0, over, (80, 24));
    assert!(text.contains("Type a path to write to"), "{text}");
}
