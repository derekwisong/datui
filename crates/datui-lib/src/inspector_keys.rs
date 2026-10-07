//! The row inspector: opening and closing it, its keys, the value view and its
//! find, drilling into nested values, copying fields, and the reads it asks for.

use crate::form::ListMove;
use crate::jobs::{Answer, Job};
use crate::{
    App, AppEvent, Overlay, clipboard, copy_modal, external_open, inspector_bytes, inspector_drill,
    inspector_modal, inspector_reader, sentence,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Whether Enter at the table opens the inspector, as Space does: there is a table,
    /// and it is not one whose rows drill into groups (a `by` view, a SQL GROUP BY).
    /// Inside a drill-down there is nothing further to drill into either.
    pub fn enter_inspects(&self) -> bool {
        self.at_table()
            && self
                .data_table_state
                .as_ref()
                .is_some_and(|state| !state.can_drill_down())
    }

    /// Space at the table, and Enter where there is nothing to drill into: the
    /// inspector over the selected row.
    pub(crate) fn open_inspector(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        if state.inspect_row().is_none() {
            self.flash_note("No row to inspect".to_string());
            return;
        }
        self.inspector_modal
            .open(state.inspect_fields(), state.current_column());
        self.open_overlay(Overlay::Inspect);
    }

    fn close_inspector(&mut self) {
        self.close_overlay();
    }

    /// The inspector's list as the row shown has it: Filled, Compare and the find
    /// text depend on the row's values, which a key may have moved.
    fn refresh_inspector_list(&mut self) {
        if let Some(state) = self.data_table_state.as_ref() {
            let visible = crate::widgets::inspector::visible_fields(&self.inspector_modal, state);
            self.inspector_modal.set_visible(visible);
        }
    }

    /// The pane for the focused value: as last drawn while that is still the
    /// focused value, else built for the key (without the table's preview, which
    /// only a frame knows).
    fn inspector_pane(&self) -> Option<crate::widgets::inspector::Pane> {
        let modal = &self.inspector_modal;
        if modal.drill.is_some() {
            return modal.pane.as_ref().map(|(_, pane)| pane.clone());
        }
        let state = self.data_table_state.as_ref()?;
        let row = state.inspect_row()?;
        let field = modal.focused()?;
        if let Some(pane) = modal.pane_for(row.frame, row.row, &field.name) {
            return Some(pane.clone());
        }
        let shown = crate::widgets::inspector::shown(field, &row, modal.read.as_ref(), state);
        Some(crate::widgets::inspector::pane(
            &field.dtype,
            &shown,
            &crate::widgets::inspector::PaneAsk {
                choice: modal.view,
                width: modal
                    .pane
                    .as_ref()
                    .map_or(80, |(key, _)| key.width as usize),
                table: None,
                indented: crate::widgets::inspector::Indented::None,
                not_json: modal.known_not_json(row.frame, row.row, &field.name),
                unpacked: modal.unpacked(&(row.frame, row.row, field.name.clone())),
                read_key: "Enter",
            },
        ))
    }

    /// The inspector's keys. Moving between rows moves the table's cursor, so the
    /// table is where the inspector left it on close.
    pub(crate) fn inspector_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        let modal = &mut self.inspector_modal;
        if modal.finding {
            match event.code {
                KeyCode::Esc => modal.clear_find(),
                KeyCode::Enter | KeyCode::Tab | KeyCode::Down => modal.finding = false,
                KeyCode::Up => {
                    modal.finding = false;
                    self.refresh_inspector_list();
                    self.inspector_modal.move_field(ListMove::Up);
                    return None;
                }
                KeyCode::Backspace => modal.find_backspace(),
                KeyCode::Char(c) => modal.find_key(c, event.modifiers),
                _ => {}
            }
            // The focus follows the narrowing now, not at the next frame: a key
            // replayed before it acts on the field the find left focused.
            self.refresh_inspector_list();
            return None;
        }
        if modal.value_find.as_ref().is_some_and(|f| f.editing) {
            self.value_find_key(event);
            return None;
        }
        if event
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
        {
            return None;
        }
        if modal.focus == inspector_modal::Focus::Value {
            return self.inspector_value_key(event);
        }
        if modal.drill.is_some() {
            return self.drill_key(event);
        }
        self.refresh_inspector_list();
        let modal = &mut self.inspector_modal;
        if let Some(step) = ListMove::from_key(event) {
            modal.move_field(step);
            return None;
        }
        match event.code {
            // Esc backs out one level at a time: the find, then Compare, then the
            // inspector.
            KeyCode::Esc if !modal.filter.is_empty() => modal.clear_find(),
            KeyCode::Esc if modal.compare => {
                modal.compare = false;
                modal.filled_only = false;
            }
            KeyCode::Esc | KeyCode::Char(' ') => self.close_inspector(),
            // Two panes: Tab and Shift+Tab both cross to the value.
            KeyCode::Tab | KeyCode::BackTab => {
                if modal.focused().is_some() {
                    modal.focus = inspector_modal::Focus::Value;
                }
            }
            KeyCode::Char('/') => modal.finding = true,
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('f') => {
                modal.filled_only = !modal.filled_only;
                modal.list_offset = 0;
            }
            KeyCode::Char('s') => {
                modal.order = modal.order.next();
                modal.list_offset = 0;
            }
            KeyCode::Char('c') => {
                modal.compare = !modal.compare;
                if !modal.compare {
                    modal.filled_only = false;
                }
            }
            KeyCode::Char('m') => self.toggle_inspector_pin(),
            KeyCode::Right | KeyCode::Char('l') => return self.scroll_key(crate::Scroll::Next),
            KeyCode::Left | KeyCode::Char('h') => return self.scroll_key(crate::Scroll::Prev),
            KeyCode::Char('y') => self.copy_inspected_field(),
            KeyCode::Char('Y') => self.copy_inspected_row(),
            KeyCode::Char('o') => self.open_inspected_value(),
            KeyCode::Char('r') => self.read_focused_field(),
            KeyCode::Enter => return self.inspector_enter(),
            _ => {}
        }
        None
    }

    /// The keys with the focus in the value: scroll it, search it, change its view.
    fn inspector_value_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let Some(pane) = self.inspector_pane() else {
            self.inspector_modal.focus = inspector_modal::Focus::List;
            return None;
        };
        let modal = &mut self.inspector_modal;
        let h = modal.page.max(1);
        let content = &pane.content;
        let page = h.saturating_sub(1);
        if let Some(step) = ListMove::from_key(event) {
            match step {
                ListMove::Home => modal.reader.home(),
                ListMove::End => modal.reader.end(content, h),
                _ => modal.reader.scroll(content, h, step.delta(page)),
            }
            return None;
        }
        match event.code {
            KeyCode::Esc
                if modal
                    .value_find
                    .as_ref()
                    .is_some_and(|f| !f.text.is_empty()) =>
            {
                modal.value_find = None;
            }
            KeyCode::Esc | KeyCode::Tab | KeyCode::BackTab => {
                modal.focus = inspector_modal::Focus::List;
            }
            KeyCode::Char(' ') => self.close_inspector(),
            KeyCode::Char('/') => {
                modal.value_find = Some(inspector_modal::ValueFind {
                    editing: true,
                    pane: pane.id,
                    ..Default::default()
                });
            }
            KeyCode::Char('n') => self.next_value_hit(1),
            KeyCode::Char('N') => self.next_value_hit(-1),
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('y') => {
                if modal.drill.is_some() {
                    self.copy_drilled_item();
                } else {
                    self.copy_inspected_field();
                }
            }
            KeyCode::Char('o') if modal.drill.is_none() => self.open_inspected_value(),
            KeyCode::Right | KeyCode::Char('l') if modal.drill.is_none() => {
                return self.scroll_key(crate::Scroll::Next);
            }
            KeyCode::Left | KeyCode::Char('h') if modal.drill.is_none() => {
                return self.scroll_key(crate::Scroll::Prev);
            }
            _ => {}
        }
        None
    }

    /// A key typed into the value's find line. Enter finds every place and goes
    /// to the first at or after the pane's top.
    fn value_find_key(&mut self, event: &KeyEvent) {
        let pane = self.inspector_pane();
        let modal = &mut self.inspector_modal;
        let Some(find) = modal.value_find.as_mut() else {
            return;
        };
        match event.code {
            KeyCode::Esc => modal.value_find = None,
            KeyCode::Enter => {
                find.editing = false;
                let Some(pane) = pane else {
                    return;
                };
                find.hits = inspector_reader::find_hits(&pane.content, &find.text);
                find.pane = pane.id;
                let h = modal.page.max(1);
                let from = modal.reader.window(&pane.content, h).from;
                let at = find.hits.partition_point(|&p| p < from);
                find.current = (!find.hits.is_empty()).then(|| at % find.hits.len());
                if let Some(at) = find.current {
                    let pos = find.hits[at];
                    modal.reader.jump(&pane.content, h, pos);
                }
            }
            KeyCode::Backspace => {
                find.text.pop();
            }
            KeyCode::Char(c) => inspector_modal::edit_find(&mut find.text, c, event.modifiers),
            _ => {}
        }
    }

    /// `n` and `N` in the value: the next or the last place found, round the ends.
    fn next_value_hit(&mut self, step: isize) {
        let Some(pane) = self.inspector_pane() else {
            return;
        };
        let modal = &mut self.inspector_modal;
        let Some(find) = modal.value_find.as_mut() else {
            return;
        };
        if find.pane != pane.id && !find.text.is_empty() {
            find.hits = inspector_reader::find_hits(&pane.content, &find.text);
            find.pane = pane.id;
            find.current = None;
        }
        let n = find.hits.len();
        if n == 0 {
            return;
        }
        let at = match find.current {
            Some(at) => (at as isize + step).rem_euclid(n as isize) as usize,
            None if step > 0 => 0,
            None => n - 1,
        };
        find.current = Some(at);
        let pos = find.hits[at];
        let h = modal.page.max(1);
        modal.reader.jump(&pane.content, h, pos);
    }

    /// The inspector's keys inside a level drilled into: the same moves as at the
    /// row, but `→` and Enter open the focused item and `←` and Esc step back up.
    fn drill_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let modal = &mut self.inspector_modal;
        if let Some(step) = ListMove::from_key(event) {
            modal.move_field(step);
            return None;
        }
        match event.code {
            KeyCode::Esc | KeyCode::Left | KeyCode::Char('h') => {
                modal.drill_out();
            }
            KeyCode::Char(' ') => self.close_inspector(),
            // A level with nothing in it has no value to cross to.
            KeyCode::Tab | KeyCode::BackTab
                if modal
                    .drill
                    .as_ref()
                    .is_some_and(|drill| drill.level().focused().is_some()) =>
            {
                modal.focus = inspector_modal::Focus::Value;
            }
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('y') => self.copy_drilled_item(),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                let drill = modal.drill.as_ref()?;
                let (frame, row) = (drill.frame, drill.row);
                let (label, node) = drill.level().focused()?;
                let path = drill.item_key(&label);
                if node.opens() && !modal.known_not_json(frame, row, &path) {
                    self.inspector_open(frame, row, label, path, node);
                }
            }
            _ => {}
        }
        None
    }

    /// `e` in the inspector: the focused value's next view, where it has more than
    /// one. A number never has, so it never changes how a text field is then shown.
    fn inspector_view(&mut self) {
        if let Some(view) = self.inspector_pane().and_then(|pane| pane.next_view()) {
            self.inspector_modal.choose_view(view);
        }
    }

    /// `w`: word wrap or hard wrap, for every value until it is pressed again.
    fn inspector_wrap(&mut self) {
        let modal = &mut self.inspector_modal;
        modal.wrap = match modal.wrap {
            inspector_reader::Wrap::Word => inspector_reader::Wrap::Hard,
            inspector_reader::Wrap::Hard => inspector_reader::Wrap::Word,
        };
    }

    /// `m`: pin this row for Compare, or let the pin go when it is this row.
    fn toggle_inspector_pin(&mut self) {
        let Some(row) = self.data_table_state.as_ref().and_then(|s| s.inspect_row()) else {
            return;
        };
        let modal = &mut self.inspector_modal;
        let here = modal
            .pinned
            .as_ref()
            .is_some_and(|p| (p.frame, p.row) == (row.frame, row.row));
        if here {
            modal.pinned = None;
            self.flash_note("Unpinned".to_string());
        } else {
            let n = row.display_row;
            modal.pinned = Some(row);
            modal.compare = true;
            self.flash_note(format!(
                "Pinned row {}; Compare shows it",
                copy_modal::thousands(n)
            ));
        }
    }

    /// Enter in the inspector: on a group's row, its rows, as at the table; else
    /// open a nested value, or read the row's fields the buffer does not hold.
    fn inspector_enter(&mut self) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        if state.can_drill_down() {
            self.close_inspector();
            self.drill_selected_row();
            return None;
        }
        let row = state.inspect_row()?;
        let field = self.inspector_modal.focused().cloned()?;
        let shown = crate::widgets::inspector::shown(
            &field,
            &row,
            self.inspector_modal.read.as_ref(),
            state,
        );
        use crate::widgets::inspector::Shown;
        match shown {
            // A failed read is asked again: the pane said why, and Enter is the retry.
            Shown::Unread | Shown::Failed(_) => self.read_focused_field(),
            Shown::Value(ref v)
                if crate::widgets::inspector::value_opens(v)
                    && !self
                        .inspector_modal
                        .known_not_json(row.frame, row.row, &field.name) =>
            {
                let column = if field.buffered() {
                    row.values.column(&field.name).ok()
                } else {
                    self.inspector_modal
                        .read_values(row.frame, row.row)
                        .and_then(|values| values.column(&field.name).ok())
                };
                if let Some(column) = column {
                    let node =
                        inspector_drill::Node::Native(column.as_materialized_series().clone());
                    let path = inspector_drill::path_key([field.name.as_str()]);
                    self.inspector_open(row.frame, row.row, field.name.clone(), path, node);
                }
            }
            _ => {}
        }
        None
    }

    /// Read the focused row's hidden and binary fields, waited on; from then on the
    /// rows moved to are read too while the focus stays on this field.
    fn read_focused_field(&mut self) {
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

    /// What the inspector needs after a pass: the row moved to read while a read
    /// follows the rows, long JSON indented for its JSON view, and compressed
    /// bytes decompressed for their Text view. None holds the keys: moving on
    /// drops what is no longer wanted.
    pub(crate) fn inspector_needs(&mut self) {
        if self.overlay != Overlay::Inspect || !self.inspector_modal.active {
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
        // Long JSON text asked for the JSON view and not yet indented, or
        // compressed bytes asked for the Text view and not yet decompressed.
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

    /// `y` in the inspector: the focused value as its view shows it — the stored
    /// value exact, indented JSON in the JSON view, the text bytes hold in their
    /// Text view, bytes otherwise as base64 — through the same clipboard path as
    /// the copy dialog. One over a capped destination's limit is refused
    /// unformatted; a large one is written off this thread.
    fn copy_inspected_field(&mut self) {
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

    /// `Y` in the inspector: the whole row as one JSON object, exact, without
    /// leaving. Fields not read are left out, and the flash counts them.
    fn copy_inspected_row(&mut self) {
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

    /// `o` in the inspector: the value written to a file of its own, in the view
    /// it is shown in, for another program to open; see [`external_open`].
    fn open_inspected_value(&mut self) {
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

    /// Open `node` as a level under the one shown: a list or struct at once, text as
    /// the JSON it holds, parsed here when short and on a worker when long. `path`
    /// is the text's place, remembered when it does not parse.
    fn inspector_open(
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
        // The worker reads the text where it is: the node is a one-row slice or a
        // shared document, so nothing up to the 4 MiB cap is copied to hand it over.
        self.spawn_job(
            Job::InspectJson { token },
            Some(Self::READING_JSON),
            move |_| match node.with_text(inspector_drill::parse_json) {
                Some(parsed) => parsed.map(|value| Answer::JsonParsed(std::sync::Arc::new(value))),
                None => Err("not JSON: not text".to_string()),
            },
        );
    }

    /// `y` inside a drill: the focused item's whole value, exact, as `y` copies a
    /// field; a JSON object or array as indented JSON.
    fn copy_drilled_item(&mut self) {
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

    /// Read, off this thread, the fields of `row` the buffer does not hold — the
    /// hidden columns and the binary ones — with the shown columns beside them, so a
    /// sort that orders ties differently on a second read cannot pass another row's
    /// fields off as this one's. `wait`: the user waits on it, as on Enter; a read
    /// that follows the rows does not hold the keys.
    fn read_inspected_fields(&mut self, row: &crate::table::InspectRow, wait: bool) {
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
                let read = crate::statistics::collect_lazy(lf, streaming)
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
