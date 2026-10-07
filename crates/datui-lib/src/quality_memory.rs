//! What Data Quality keeps for the session: its reports, the rows its sampled runs
//! read, and local copies of remote sources.

use std::path::PathBuf;
use std::sync::Arc;

use crate::{data_quality, sampling};

/// A Data Quality report, by everything its plan says: the acquisition it measured
/// and the report's own choices.
pub(crate) struct QualityCacheEntry {
    pub(crate) dataset_generation: u64,
    pub(crate) view_generation: u64,
    pub(crate) plan: data_quality::DataQualityPlan,
    pub(crate) results: data_quality::DataQualityResults,
    pub(crate) bytes: usize,
}

/// Rows a sampled Data Quality run read, and what decided which rows they were: its
/// acquisition identity. A run or a drill that names the same rows cuts these instead
/// of reading.
///
/// The dataset and view generations stand for the source: a session snapshot of
/// the dataset as opened and the view as it was. A file changed on disk since is not
/// noticed; opening it again starts a new dataset generation, and reads it again.
#[derive(Debug, Clone)]
pub struct KeptQualitySample {
    pub(crate) dataset_generation: u64,
    pub(crate) view_generation: u64,
    pub(crate) sample: sampling::Sample,
    pub(crate) rows: std::sync::Arc<data_quality::QualitySample>,
    /// The source as the run that read these rows found it, stated when it began: a
    /// report measured on them later is labeled with this, not with the file as it
    /// stands then.
    pub(crate) source: crate::quality_export::SourceIdentity,
}

impl KeptQualitySample {
    pub(crate) fn same_rows(&self, other: &Self) -> bool {
        self.dataset_generation == other.dataset_generation
            && self.view_generation == other.view_generation
            && self.sample == other.sample
    }
}

/// Memory Data Quality keeps for the session: the rows its sampled runs read and the
/// reports they made. Past it, reports that retained rows can remake go first, then
/// the oldest rows, then the oldest other reports; the newest of each always stays.
pub const QUALITY_MEMORY_BUDGET: usize = 256 * 1024 * 1024;

/// Where a full scan's passes read a remote source from, decided at Run.
pub(crate) enum QualityCopyJob {
    /// The source, in each pass.
    Source,
    /// A copy fetched earlier.
    Kept(Arc<crate::cloud::local_copy::LocalCopy>),
    /// A copy of `objects` fetched under `root` first.
    Fetch {
        objects: Vec<crate::cloud::local_copy::RemoteObject>,
        root: PathBuf,
    },
}

/// A local copy of a dataset's remote objects, kept for later full scans of the
/// dataset it was fetched for.
#[derive(Debug, Clone)]
pub struct RetainedCopy {
    pub(crate) dataset_generation: u64,
    pub(crate) copy: Arc<crate::cloud::local_copy::LocalCopy>,
}

/// Acquisitions released to the budget that Setup still names, so a Run that reads
/// them again says why.
pub(crate) const QUALITY_RELEASED_REMEMBERED: usize = 16;
