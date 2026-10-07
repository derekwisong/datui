//! Copying from the inspector, and handing a value to another program.

use crate::app::jobs::{Answer, Job};
use crate::{
    App, app::modals::copy_modal, clipboard, inspector::external_open, inspector::inspector_bytes,
    inspector::inspector_drill, sentence,
};

impl App {
    /// `y` in the inspector: the focused value as its view shows it (exact stored value,
    /// indented JSON, decoded text, or base64 bytes), through the copy dialog's
    /// clipboard path. Over a capped destination's limit it is refused unformatted; a
    /// large one is written off this thread.
    pub(super) fn copy_inspected_field(&mut self) {
        use copy_modal::thousands;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let (Some(row), Some(field)) = (state.inspect_row(), self.inspector_modal.focused()) else {
            return;
        };
        let field = field.clone();
        let column = if field.buffered() {
            row.values.column(&field.name).ok().cloned()
        } else {
            self.inspector_modal
                .read_values(row.frame, row.row)
                .and_then(|values| values.column(&field.name).ok().cloned())
        };
        let Some(column) = column else {
            self.flash_note(format!("{} is not read yet; Enter reads it", field.name));
            return;
        };
        let message = format!(
            "Copied {} of row {}",
            field.name,
            thousands(row.display_row)
        );
        use crate::widgets::inspector::CopyAs;
        match self.inspector_pane().map(|pane| pane.copy) {
            Some(CopyAs::Text(text)) => self.copy_string(text.to_string(), message),
            Some(CopyAs::Escaped) => {
                let text = column
                    .get(0)
                    .map(|v| crate::exact::escaped(&crate::exact::value_text(&v)))
                    .unwrap_or_default();
                self.copy_string(text, message);
            }
            _ => self.copy_value(column, message),
        }
    }

    /// `Y` in the inspector: the whole row as one exact JSON object. Unread fields are
    /// left out, and the flash counts them.
    pub(super) fn copy_inspected_row(&mut self) {
        use copy_modal::thousands;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let fields = self.inspector_modal.fields.clone();
        let read = self.inspector_modal.read.clone();
        let display = row.display_row;
        let size = row.values.estimated_size()
            + self
                .inspector_modal
                .read_values(row.frame, row.row)
                .map_or(0, |v| v.estimated_size());
        let build = move || {
            let (json, kept, unread) =
                crate::widgets::inspector::row_json(&fields, &row, read.as_ref());
            let message = if unread > 0 {
                format!(
                    "Copied row {}: {} fields, {} not read",
                    thousands(display),
                    thousands(kept),
                    thousands(unread)
                )
            } else {
                format!("Copied row {} as JSON", thousands(display))
            };
            (json, message)
        };
        if size <= Self::FIELD_COPY_INLINE_BYTES {
            let (json, message) = build();
            self.copy_string(json, message);
            return;
        }
        let limit = match self.copy_destination() {
            Ok(destination) => destination.accepts().base64_limit,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
            let (json, message) = build();
            if let Some(limit) = limit
                && json.len() > limit / 4 * 3
            {
                return Err(clipboard::over_osc52_limit(None, limit));
            }
            Ok(Answer::Copied {
                payload: clipboard::Payload::text(json),
                message,
            })
        });
    }

    /// `o` in the inspector: the value written to its own file, in its shown view, for
    /// another program; see [`external_open`].
    pub(super) fn open_inspected_value(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let Some(field) = self.inspector_modal.focused().cloned() else {
            return;
        };
        let column = if field.buffered() {
            row.values.column(&field.name).ok().cloned()
        } else {
            self.inspector_modal
                .read_values(row.frame, row.row)
                .and_then(|values| values.column(&field.name).ok().cloned())
        };
        let Some(column) = column else {
            self.flash_note(format!("{} is not read yet; Enter reads it", field.name));
            return;
        };
        if self.external.open_dir.is_none() {
            match tempfile::Builder::new().prefix("datui-values-").tempdir() {
                Ok(dir) => self.external.open_dir = Some(dir),
                Err(e) => {
                    self.flash_note(format!("Could not open the value: {e}"));
                    return;
                }
            }
        }
        let dir = self
            .external
            .open_dir
            .as_ref()
            .map(|d| d.path().to_path_buf())
            .unwrap_or_default();
        let shown_as = self.inspector_pane().map(|pane| pane.copy);
        let name = field.name.clone();
        let display = row.display_row;
        self.spawn_job(Job::OpenValue, Some("Writing the value..."), move |_| {
            use crate::widgets::inspector::CopyAs;
            let value = column.get(0).map_err(|e| e.to_string())?;
            let (bytes, extension, document): (Vec<u8>, &str, bool) = match (&shown_as, &value) {
                (Some(CopyAs::Text(text)), _) => {
                    let ext = if inspector_drill::looks_like_json(text) {
                        "json"
                    } else {
                        "txt"
                    };
                    (text.as_bytes().to_vec(), ext, false)
                }
                (_, polars::prelude::AnyValue::Binary(b)) => {
                    let kind = inspector_bytes::sniff(b);
                    (
                        b.to_vec(),
                        kind.map_or("bin", |k| k.extension()),
                        kind.is_some_and(|k| k.is_document()),
                    )
                }
                (_, polars::prelude::AnyValue::BinaryOwned(b)) => {
                    let kind = inspector_bytes::sniff(b);
                    (
                        b.clone(),
                        kind.map_or("bin", |k| k.extension()),
                        kind.is_some_and(|k| k.is_document()),
                    )
                }
                (_, v) if crate::exact::is_nested_value(v) => {
                    let text = crate::exact::copy_text(&column).map_err(|e| e.to_string())?;
                    (text.into_bytes(), "json", false)
                }
                (_, v) => {
                    let text = crate::exact::value_text(v);
                    let trimmed = text.trim_start();
                    let ext = if inspector_drill::looks_like_json(&text) {
                        "json"
                    } else if trimmed.starts_with('<') {
                        "xml"
                    } else {
                        "txt"
                    };
                    (text.into_bytes(), ext, false)
                }
            };
            let file = external_open::file_name(&name, display, extension);
            let path =
                external_open::write_value(&dir, &file, &bytes).map_err(|e| e.to_string())?;
            Ok(Answer::ValueWritten(external_open::ExternalOpen {
                path,
                document,
            }))
        });
    }

    /// Open `node` as a level below: a list or struct at once, text as its JSON, parsed
    /// here when short and on a worker when long. `path` is remembered if it does not
    /// parse.
    pub(super) fn inspector_open(
        &mut self,
        frame: u64,
        row: usize,
        label: String,
        path: String,
        node: inspector_drill::Node,
    ) {
        use inspector_drill::{JSON_INLINE_BYTES, Node, Shape};
        if node.shape() != Shape::Leaf {
            self.inspector_modal.drill_in(frame, row, label, node);
            return;
        }
        let Some(len) = node
            .with_text(|s| inspector_drill::opens_as_json(s).then_some(s.len()))
            .flatten()
        else {
            return;
        };
        // Short text is parsed on this key.
        if len <= JSON_INLINE_BYTES {
            match node.with_text(inspector_drill::parse_json) {
                Some(Ok(value)) => {
                    let node = Node::Json {
                        root: std::sync::Arc::new(value),
                        path: Vec::new(),
                    };
                    self.inspector_modal.drill_in(frame, row, label, node);
                }
                Some(Err(e)) => {
                    self.inspector_modal.not_json = Some((frame, row, path));
                    self.flash_note(sentence(&e));
                }
                None => {}
            }
            return;
        }
        let token = self.inspector_modal.wait_for_json(frame, row, label, path);
        // The worker reads the text in place (a one-row slice or shared document), so
        // nothing up to the 4 MiB cap is copied.
        self.spawn_job(
            Job::InspectJson { token },
            Some(Self::READING_JSON),
            move |_| match node.with_text(inspector_drill::parse_json) {
                Some(parsed) => parsed.map(|value| Answer::JsonParsed(std::sync::Arc::new(value))),
                None => Err("not JSON: not text".to_string()),
            },
        );
    }

    /// `y` inside a drill: the focused item's exact value, objects and arrays as
    /// indented JSON.
    pub(super) fn copy_drilled_item(&mut self) {
        use inspector_drill::Node;
        let Some(drill) = self.inspector_modal.drill.as_ref() else {
            return;
        };
        let Some((label, node)) = drill.level().focused() else {
            return;
        };
        let g = crate::glyphs::get();
        let mut path: Vec<&str> = drill.levels.iter().map(|l| l.label.as_str()).collect();
        path.push(&label);
        let display_row = self
            .data_table_state
            .as_ref()
            .and_then(|s| s.inspect_row())
            .map_or(drill.row + 1, |r| r.display_row);
        let message = format!(
            "Copied {} of row {}",
            path.join(&format!(" {} ", g.trail)),
            copy_modal::thousands(display_row)
        );
        match node {
            Node::Native(series) => self.copy_value(polars::prelude::Column::from(series), message),
            Node::Json { .. } => {
                let limit = match self.copy_destination() {
                    Ok(destination) => destination.accepts().base64_limit,
                    Err(e) => {
                        self.error_modal.show(e);
                        return;
                    }
                };
                // Formatted here up to a size that is quick; past it, on a worker.
                let cap = limit.map_or(Self::JSON_COPY_MAX_BYTES, |limit| limit / 4 * 3);
                let quick = cap.min(Self::FIELD_COPY_INLINE_BYTES);
                let null = serde_json::Value::Null;
                let value = node.json().unwrap_or(&null);
                if let Some(text) = inspector_drill::json_copy_text(value, quick) {
                    self.finish_copy(clipboard::Payload::text(text), message);
                    return;
                }
                if let Some(limit) = limit.filter(|_| cap <= Self::FIELD_COPY_INLINE_BYTES) {
                    self.error_modal
                        .show(clipboard::over_osc52_limit(None, limit));
                    return;
                }
                // The worker resolves the path itself: the document is shared, not copied.
                self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
                    let value = node.json().unwrap_or(&serde_json::Value::Null);
                    let text =
                        inspector_drill::json_copy_text(value, cap).ok_or_else(|| match limit {
                            Some(limit) => clipboard::over_osc52_limit(None, limit),
                            None => "Copy failed: the value is too large to copy".to_string(),
                        })?;
                    Ok(Answer::Copied {
                        payload: clipboard::Payload::text(text),
                        message,
                    })
                });
            }
        }
    }
}
