//! What the inspector reads off the UI thread: fields not in the buffer, and the
//! JSON, indented text and decompressed bytes its views ask for.

use crate::app::jobs::{Answer, Job};
use crate::{
    App, Overlay, inspector::inspector_bytes, inspector::inspector_drill,
    inspector::inspector_modal,
};

impl App {
    /// Read the focused row's hidden and binary fields, waited on; rows moved to are
    /// then read too while focus stays on this field.
    pub(crate) fn read_focused_field(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let Some(field) = self.inspector_modal.focused().cloned() else {
            return;
        };
        let shown = crate::widgets::inspector::shown(
            &field,
            &row,
            self.inspector_modal.read.as_ref(),
            state,
        );
        if matches!(
            shown,
            crate::widgets::inspector::Shown::Unread | crate::widgets::inspector::Shown::Failed(_)
        ) {
            self.inspector_modal.follow = Some(field.name.clone());
            self.read_inspected_fields(&row, true);
        }
    }

    /// What the inspector needs after a pass: the row moved to (while following), long
    /// JSON indented, compressed bytes decompressed. None holds keys; moving on drops
    /// the unwanted.
    pub(crate) fn inspector_needs(&mut self) {
        if self.overlay != Overlay::Inspect {
            return;
        }
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(row) = state.inspect_row() else {
            return;
        };
        let modal = &self.inspector_modal;
        if modal.drill.is_some() {
            return;
        }
        let Some(field) = modal.focused().cloned() else {
            return;
        };
        let read_here = modal
            .read
            .as_ref()
            .is_some_and(|r| r.key() == (row.frame, row.row));
        if modal.follow.as_deref() == Some(field.name.as_str()) && !field.buffered() && !read_here {
            self.read_inspected_fields(&row, false);
            return;
        }
        // Long JSON awaiting its JSON view, or compressed bytes awaiting their Text view.
        let pane = modal.pane_for(row.frame, row.row, &field.name);
        let place = (row.frame, row.row, field.name.clone());
        let indent = pane.is_some_and(|pane| pane.indent)
            && !modal.pretty.as_ref().is_some_and(|p| *p.place() == place);
        let unpack = pane.is_some_and(|pane| pane.unpack)
            && !modal.unpack.as_ref().is_some_and(|u| *u.place() == place);
        if !indent && !unpack {
            return;
        }
        let column = if field.buffered() {
            row.values.column(&field.name).ok().cloned()
        } else {
            modal
                .read_values(row.frame, row.row)
                .and_then(|values| values.column(&field.name).ok().cloned())
        };
        let Some(column) = column else {
            return;
        };
        let modal = &mut self.inspector_modal;
        if unpack {
            modal.unpack_token += 1;
            let token = modal.unpack_token;
            modal.unpack = Some(inspector_modal::Unpack::Pending { token, place });
            self.spawn_job(Job::InspectUnpack { token }, None, move |_| {
                let value = column.get(0).map_err(|e| e.to_string())?;
                let bytes = match &value {
                    polars::prelude::AnyValue::Binary(b) => *b,
                    polars::prelude::AnyValue::BinaryOwned(b) => b.as_slice(),
                    _ => return Err("not bytes".to_string()),
                };
                inspector_bytes::decode_text(bytes, inspector_bytes::sniff(bytes))
                    .map(Answer::Unpacked)
                    .ok_or_else(|| "not text".to_string())
            });
            return;
        }
        modal.pretty_token += 1;
        let token = modal.pretty_token;
        modal.pretty = Some(inspector_modal::Pretty::Pending { token, place });
        self.spawn_job(Job::InspectPretty { token }, None, move |_| {
            let value = column.get(0).map_err(|e| e.to_string())?;
            let text = match &value {
                polars::prelude::AnyValue::String(s) => *s,
                polars::prelude::AnyValue::StringOwned(s) => s.as_str(),
                _ => return Err("not text".to_string()),
            };
            let json = inspector_drill::parse_json(text)?;
            let (pretty, _) = inspector_drill::json_text(&json, true, usize::MAX);
            Ok(Answer::Indented(std::sync::Arc::from(pretty)))
        });
    }

    /// Read off-thread the fields of `row` the buffer lacks (hidden and binary columns),
    /// with the shown columns beside them so a sort ordering ties differently cannot
    /// swap in another row's fields. `wait`: the user waits, as on Enter; a read
    /// following the rows holds no keys.
    pub(crate) fn read_inspected_fields(&mut self, row: &crate::table::InspectRow, wait: bool) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let fields = state.inspect_fields();
        let wanted: Vec<String> = fields
            .iter()
            .filter(|f| !f.buffered())
            .map(|f| f.name.clone())
            .collect();
        let check: Vec<String> = fields
            .iter()
            .filter(|f| f.buffered())
            .map(|f| f.name.clone())
            .collect();
        let columns: Vec<String> = wanted.iter().chain(check.iter()).cloned().collect();
        let lf = match state.inspect_read_lf(row.row, &columns) {
            Ok(lf) => lf,
            Err(e) => {
                self.inspector_modal.read = Some(inspector_modal::FieldRead::Failed {
                    frame: row.frame,
                    row: row.row,
                    message: crate::error_display::user_message_from_polars(&e),
                });
                return;
            }
        };
        let expected = row.values.select(check.iter().map(String::as_str)).ok();
        let streaming = state.polars_streaming();
        let (frame, index) = (row.frame, row.row);
        self.inspector_modal.read = Some(inspector_modal::FieldRead::Reading { frame, row: index });
        self.spawn_job(
            Job::InspectRow { frame, row: index },
            wait.then_some(Self::READING_FIELDS),
            move |_| {
                let read = crate::analysis::statistics::collect_lazy(lf, streaming)
                    .map_err(|e| crate::error_display::user_message_from_polars(&e))?;
                if read.height() != 1 {
                    return Err("the row is no longer in the view".to_string());
                }
                let same = expected.is_some_and(|expected| {
                    read.select(check.iter().map(String::as_str))
                        .is_ok_and(|again| again.equals_missing(&expected))
                });
                if !same {
                    return Err("the view's order of equal rows changed on reading again; \
                         sort by a column that tells the rows apart"
                        .to_string());
                }
                let values = read
                    .select(wanted.iter().map(String::as_str))
                    .map_err(|e| e.to_string())?;
                Ok(Answer::FieldsRead(values))
            },
        );
    }
}
