//! A view's chart: saved with the view, put back when the view is applied, and
//! written into an exported chart as its recipe.

use crate::App;
use crate::view::{MatchCriteria, SavedChart, SavedView};
use std::path::Path;

impl App {
    /// The chart drawn of the dataset on screen, as a view keeps it; `None` before a
    /// chart of it has been chosen.
    pub(crate) fn saved_chart(&self) -> Option<SavedChart> {
        let modal = &self.chart_modal;
        if modal.dataset != Some(self.dataset_generation)
            || modal.x().is_none() && modal.y().is_empty()
        {
            return None;
        }
        let export = self.chart_export_modal.saved();
        Some(modal.saved(self.analysis_modal.sample.seed, Some(export)))
    }

    /// Put `chart`, a view's, in place: `c` at the table draws it, and the export
    /// dialog opens as it was last used for it.
    pub(crate) fn restore_view_chart(&mut self, chart: Option<&SavedChart>) {
        let Some(chart) = chart else {
            return;
        };
        self.chart_modal.restore(chart, self.dataset_generation);
        // The chart's own sample is drawn from the tools' seed; a view's sample is
        // read whole and needs none.
        if let Some(seed) = chart.seed
            && self.analysis_modal.own_sample.is_none()
        {
            self.analysis_modal.sample.seed = seed;
        }
        self.chart_export_modal.restore = chart.export.clone();
    }

    /// How the chart on screen was made, as a view's JSON that can be saved as one:
    /// the datui version, the source, the query, filters and sort, the sample or
    /// every row, and the chart. Written into an export of it, at `path`.
    pub(crate) fn chart_recipe(&self, path: &Path) -> Option<String> {
        let state = self.data_table_state.as_ref()?;
        let mut settings = crate::view_settings_of(state);
        settings.chart = self.saved_chart();
        let source = if self.reads_stdin() {
            std::path::PathBuf::from(crate::stdin::PATH)
        } else {
            self.path.clone()?
        };
        let name = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().to_string())
            .unwrap_or_else(|| "chart".to_string());
        let rows = match (state.sampled(), &settings.chart) {
            (Some(sampled), _) => {
                let sample = sampled.sample();
                format!("{} · seed {}", sampled.label(), sample.seed)
            }
            (
                None,
                Some(SavedChart {
                    rows: Some(rows),
                    seed: Some(seed),
                    ..
                }),
            ) => format!(
                "sample of up to {} rows · seed {seed}",
                crate::numfmt::group_chrome(*rows)
            ),
            _ => "every row".to_string(),
        };
        let view = SavedView {
            id: format!("recipe-{name}"),
            name,
            description: Some(format!("The recipe of a chart exported by datui: {rows}")),
            created: std::time::SystemTime::now(),
            last_used: None,
            usage_count: 0,
            last_matched_file: None,
            match_criteria: MatchCriteria {
                exact_path: Some(source),
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: self.view_table().map(str::to_string),
            },
            settings,
        };
        let mut json = serde_json::to_value(&view).ok()?;
        let object = json.as_object_mut()?;
        object.insert("datui".into(), env!("CARGO_PKG_VERSION").into());
        object.insert("rows".into(), rows.into());
        serde_json::to_string_pretty(&json).ok()
    }
}
