//! A view's chart: saved with the view, put back when the view is applied, and
//! written into an exported chart as its recipe.

use crate::App;
use crate::view::SavedChart;

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

    /// How the chart on screen was made, as a view's JSON that reads back as one:
    /// the datui version, the source (a URL without its credentials or query), the
    /// table of a file of tables, what rows the chart read, and the view's settings
    /// (sample, query, filters, sort, columns, reshape, chart and its export).
    pub(crate) fn chart_recipe(&self) -> Option<String> {
        let state = self.data_table_state.as_ref()?;
        let mut settings = crate::view_settings_of(state);
        settings.chart = self.saved_chart();
        let source = if self.reads_stdin() {
            crate::stdin::PATH.to_string()
        } else {
            public_location(&self.path.as_ref()?.to_string_lossy())
        };
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
        let mut recipe = serde_json::Map::new();
        recipe.insert("datui".into(), env!("CARGO_PKG_VERSION").into());
        recipe.insert("source".into(), source.into());
        if let Some(table) = self.view_table() {
            recipe.insert("table".into(), table.into());
        }
        recipe.insert("rows".into(), rows.into());
        recipe.insert("settings".into(), serde_json::to_value(&settings).ok()?);
        serde_json::to_string_pretty(&recipe).ok()
    }
}

/// `location` as a recipe may say it: a URL without its user, password, query or
/// fragment, which can carry credentials or a presigned signature; a path as it is.
pub(crate) fn public_location(location: &str) -> String {
    let Some(at) = location.find("://") else {
        return location.to_string();
    };
    let (scheme, rest) = location.split_at(at + 3);
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{scheme}{host}{path}")
}

#[cfg(test)]
mod tests {
    use super::public_location;

    #[test]
    fn a_url_in_a_recipe_says_no_credentials_or_query() {
        for (given, said) in [
            (
                "https://user:secret@host.example/data/x.parquet?X-Amz-Signature=abc#top",
                "https://host.example/data/x.parquet",
            ),
            ("s3://key@bucket/prefix/", "s3://bucket/prefix/"),
            ("https://host.example", "https://host.example"),
            ("/data/x?.csv", "/data/x?.csv"),
        ] {
            assert_eq!(public_location(given), said);
        }
    }
}
