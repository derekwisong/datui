//! A catalog dataset's documentation: what its columns mean, in their publisher's words.
//!
//! Long, coded datasets (weather stations, trip records) are unreadable without one.
//! A catalog entry carries it as `documentation` (the link) and `columns`; the Info
//! panel, the inspector and the home screen's details read it from here.

use crate::home::catalog::Dataset;
use std::collections::BTreeMap;

/// One column's note.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Column {
    pub description: String,
    pub unit: String,
    /// Code to meaning; `""` is what a blank or null value means.
    pub values: BTreeMap<String, String>,
}

impl Column {
    /// The description and the unit on one line.
    pub fn about(&self) -> String {
        match (self.description.is_empty(), self.unit.is_empty()) {
            (false, false) => format!("{} ({})", self.description, self.unit),
            (false, true) => self.description.clone(),
            (true, false) => self.unit.clone(),
            (true, true) => String::new(),
        }
    }

    /// What `value` stands for, by its exact code: GHCN's source flags `a` and `A`
    /// are different sources. `None` (a null) and a blank value read the `""` entry.
    pub fn meaning(&self, value: Option<&str>) -> Option<(&str, &str)> {
        let code = value.map(str::trim).unwrap_or("");
        self.values
            .get_key_value(code)
            .map(|(key, meaning)| (key.as_str(), meaning.as_str()))
    }

    /// The legend line for `value`: `S = failed spatial consistency check`, or
    /// `blank = did not fail any quality assurance check`.
    pub fn legend_line(&self, value: Option<&str>) -> Option<String> {
        let (code, meaning) = self.meaning(value)?;
        let code = if code.is_empty() { "blank" } else { code };
        Some(format!("{code} = {meaning}"))
    }
}

/// A dataset's codebook.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Codebook {
    /// The publisher's documentation the notes come from.
    pub source: String,
    pub columns: BTreeMap<String, Column>,
}

impl Codebook {
    /// The column notes a catalog entry carries, when it carries any.
    pub fn of(dataset: &Dataset) -> Option<Self> {
        if dataset.columns.is_empty() {
            return None;
        }
        Some(Self {
            source: dataset.documentation.clone(),
            columns: dataset
                .columns
                .iter()
                .map(|(name, note)| {
                    (
                        name.clone(),
                        Column {
                            description: note.description.clone(),
                            unit: note.unit.clone(),
                            values: note.values.iter().cloned().collect(),
                        },
                    )
                })
                .collect(),
        })
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.get(name)
    }

    /// Whether any of `names` has a note.
    pub fn covers<'a>(&self, mut names: impl Iterator<Item = &'a str>) -> bool {
        names.any(|name| self.columns.contains_key(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flag() -> Column {
        Column {
            description: "Quality flag".to_string(),
            unit: String::new(),
            values: [
                ("", "did not fail any quality assurance check"),
                ("S", "failed spatial consistency check"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        }
    }

    #[test]
    fn a_code_reads_its_meaning_and_blank_reads_the_empty_entry() {
        let column = flag();
        assert_eq!(
            column.legend_line(Some("S")).as_deref(),
            Some("S = failed spatial consistency check")
        );
        assert_eq!(column.legend_line(Some("s")), None);
        assert_eq!(
            column.legend_line(None).as_deref(),
            Some("blank = did not fail any quality assurance check")
        );
        assert_eq!(
            column.legend_line(Some(" ")).as_deref(),
            Some("blank = did not fail any quality assurance check")
        );
        assert_eq!(column.legend_line(Some("Q")), None);
        assert_eq!(column.about(), "Quality flag");
    }
}
