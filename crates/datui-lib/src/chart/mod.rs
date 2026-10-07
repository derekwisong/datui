//! The chart view: its spec and panel, preparing the data off the UI thread, and
//! exporting the chart as SVG, PNG or PDF.

pub mod chart_data;
pub mod chart_export;
pub mod chart_export_modal;
pub(crate) mod chart_jobs;
pub(crate) mod chart_keys;
pub mod chart_modal;
pub(crate) mod chart_pdf;
pub mod chart_plot;
pub(crate) mod chart_recipe;
