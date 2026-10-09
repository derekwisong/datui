//! The home screen's keys.

use crate::app::feedback::Confirm;
use crate::{App, AppEvent, cloud::source, home, home::discover};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::Path;

impl App {
    /// Key handling for the home screen.
    pub(crate) fn home_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // The line beside the prompt answers the last key only; a key with something to
        // say sets it again below.
        self.home.status = None;
        // Answers handled in the same pass as this key are folded first, so the key acts
        // on the rows as they now are (a directory's kind, a door's name), not as the last
        // frame left them.
        self.home.apply_new_measurements();

        if self.info.documentation.is_open() {
            self.documentation_key(event);
            return None;
        }

        // Every plain character goes into the filter (`q` must type, to search
        // "quarterly"); Ctrl+C quits, handled earlier, as does Esc with nothing left to
        // back out of.
        if self.home.path_input_active {
            match event.code {
                KeyCode::Esc => {
                    self.home.path_input_active = false;
                    self.home.path_input.clear();
                    self.home.path_listing = None;
                    self.home.path_pick = None;
                    self.home.status = None;
                }
                // The list under the prompt is the typed directory: ↑↓ pick a name, which Enter
                // and Tab take.
                KeyCode::Up | KeyCode::Down => {
                    let n = self.home.path_candidates().len();
                    self.home.path_pick = match (event.code, self.home.path_pick) {
                        _ if n == 0 => None,
                        (KeyCode::Down, None) => Some(0),
                        (KeyCode::Down, Some(i)) => Some((i + 1).min(n - 1)),
                        (KeyCode::Up, Some(0)) | (KeyCode::Up, None) => None,
                        (KeyCode::Up, Some(i)) => Some(i - 1),
                        (_, pick) => pick,
                    };
                }
                KeyCode::Enter => {
                    if let Some(picked) = self.home.picked_path() {
                        self.home.path_input = picked;
                        self.home.path_pick = None;
                    }
                    let raw = self.home.path_input.trim().to_string();
                    if raw.is_empty() {
                        self.home.path_input_active = false;
                        return None;
                    }
                    let path = home::expand_user_path(&raw);
                    // A URL is not stat'ed (`exists` would look for a local `gs:`): its name decides,
                    // as for a recent: a file opens, anything else in a bucket is browsed.
                    if home::is_object_store_url(&path) || home::is_cloud_place(&path) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        let kind = if home::names_a_file(&path) {
                            discover::EntryKind::File
                        } else {
                            discover::EntryKind::Directory
                        };
                        return self.open_what_it_is(path, kind, true);
                    }
                    // And an HTTP URL is one file.
                    if !matches!(source::input_source(&path), source::InputSource::Local(_)) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        return self.open_what_it_is(path, discover::EntryKind::File, true);
                    }
                    // Existence, directory-ness and kind are filesystem calls, and a typed path may
                    // name a mount that stalls: a worker does them. A typo comes back to the
                    // prompt to be fixed in place.
                    self.home.path_input.clear();
                    self.home.path_input_active = false;
                    return Some(AppEvent::ClassifyThenOpen { path, jump: true });
                }
                KeyCode::Backspace => {
                    self.home.path_input.pop();
                    self.home.status = None;
                }
                KeyCode::Char('u') if ctrl => self.home.path_input.clear(),
                // Complete as a shell does (the listed names' common prefix), or take a name picked
                // with ↓. Before the listing is in, completion reads the directory on a worker.
                KeyCode::Tab => {
                    let completed = match self.home.path_pick {
                        Some(i) if i > 0 => self.home.picked_path(),
                        _ => self.home.path_completion(),
                    };
                    let listed = self
                        .home
                        .path_listing
                        .as_ref()
                        .is_some_and(|l| l.dir == home::typed_dir(&self.home.path_input));
                    match completed {
                        Some(completed) => self.home.path_input = completed,
                        None if !listed => self.request_path_completion(),
                        None => {}
                    }
                }
                KeyCode::Char(c) if !ctrl => {
                    self.home.path_input.push(c);
                    self.home.status = None;
                }
                _ => {}
            }
            // Any change to the typed text resets the pick to the first match and lists the
            // new directory.
            self.list_the_typed_directory();
            if !matches!(event.code, KeyCode::Up | KeyCode::Down) {
                self.home.pick_first_path();
            }
            return None;
        }

        // A kept filter is selected: a character, Backspace or Delete replaces it as in any
        // field; other keys keep it.
        if std::mem::take(&mut self.home.filter_selected) {
            let replaces = match event.code {
                KeyCode::Char(_) => !ctrl,
                KeyCode::Backspace => true,
                _ => false,
            };
            if replaces {
                self.home.filter.clear();
                self.home.sync_search_section();
                self.home.select_first_entry();
                if event.code == KeyCode::Backspace {
                    return None;
                }
            }
        }

        // Every plain character types into the filter ("json" must not move on "j");
        // navigation is arrows and Ctrl chords.
        match event.code {
            KeyCode::Esc => return self.home_escape(),
            KeyCode::Enter => return self.home_open_selected(),
            // Section to section, past however many rows the current one holds.
            KeyCode::Down if ctrl => self.home.jump_section(1),
            KeyCode::Up if ctrl => self.home.jump_section(-1),
            KeyCode::Up => self.home.move_selection(-1),
            KeyCode::Down => self.home.move_selection(1),
            KeyCode::Char('n') if ctrl => self.home.move_selection(1),
            KeyCode::Char('p') if ctrl => self.home.move_selection(-1),
            // ←→ fold the cursor's section from anywhere in it. Tab cycles the sort (letters
            // are all filter input).
            KeyCode::Tab => {
                self.home.sort = self.home.sort.next();
                self.home.select_first_entry();
            }
            KeyCode::Left => self.home_collapse(true),
            KeyCode::Right => match self.selected_directory_to_enter() {
                // Into a directory that opens as one dataset, to reach one partition or file; this
                // clears the filter, as browsing does.
                Some(directory) => {
                    // The heading Enter shows, so arriving inside a lake table is explained.
                    if let Some(format) = self
                        .home
                        .selected_entry()
                        .and_then(|entry| entry.kind.lake_name())
                    {
                        self.home.lake_here = Some((directory.clone(), format));
                    }
                    self.home_browse_into(directory);
                }
                None => self.home_collapse(false),
            },
            // A screenful, matching the table; the renderer keeps view_height current.
            KeyCode::PageUp => {
                let page = self.home.view_height.max(1) as isize;
                self.home.page_selection(-page);
            }
            KeyCode::PageDown => {
                let page = self.home.view_height.max(1) as isize;
                self.home.page_selection(page);
            }
            KeyCode::Home => self.home.page_selection(isize::MIN),
            KeyCode::End => self.home.page_selection(isize::MAX),
            KeyCode::Char('u') if ctrl => {
                self.home.filter.clear();
                self.home.sync_search_section();
                self.home.select_first_entry();
                #[cfg(feature = "cloud")]
                self.narrow_cloud_listing();
            }
            KeyCode::Char('r') if ctrl => self.home_reload(),
            // A browser's bookmark key: add the row to catalog.toml, or forget it.
            KeyCode::Char('d') if ctrl => self.home_toggle_catalog(),
            KeyCode::Char('e') if ctrl => self.home_open_documentation(),
            // Any local file's bytes, whatever datui would read it as.
            KeyCode::Char('x') if ctrl => {
                let local = |path: &Path| {
                    matches!(source::input_source(path), source::InputSource::Local(_))
                };
                match self.home.selected_entry().cloned() {
                    Some(entry)
                        if !self.home.selection_is_the_door()
                            && entry.table.is_none()
                            && matches!(
                                entry.kind,
                                discover::EntryKind::File
                                    | discover::EntryKind::Other
                                    | discover::EntryKind::Unknown
                            )
                            && local(&entry.path) =>
                    {
                        self.open_hex(entry.path, crate::app::hex_view::Origin::Home, false, None);
                    }
                    _ => self.flash_note("Ctrl+X shows a local file's bytes".to_string()),
                }
            }
            KeyCode::Char('a') if ctrl => {
                let on = self.home.selected_key();
                self.home.hide_unreadable = !self.home.hide_unreadable;
                // Inside a database, what is hidden is its own tables.
                let tables = self
                    .home
                    .sections
                    .iter()
                    .any(|section| section.rows.iter().any(|row| row.table.is_some()));
                self.flash_note(
                    match (self.home.hide_unreadable, tables) {
                        (true, true) => "Hiding internal tables",
                        (false, true) => "Showing internal tables",
                        (true, false) => "Hiding files with no reader",
                        (false, false) => "Showing files with no reader",
                    }
                    .to_string(),
                );
                // The same row where it is still there; the cursor stays put otherwise.
                self.home.reselect(on);
            }
            KeyCode::Backspace => {
                if self.home.filter.is_empty() {
                    self.home_ascend();
                } else {
                    self.home.filter.pop();
                    self.home.sync_search_section();
                    self.home.select_first_entry();
                    #[cfg(feature = "cloud")]
                    self.narrow_cloud_listing();
                }
            }
            // Delete forgets the highlighted entry, only in Recent (elsewhere rows are real
            // files, and datui does not delete). Shift+Delete forgets all, asking first since
            // it sits beside Delete.
            KeyCode::Delete if event.modifiers.contains(KeyModifiers::SHIFT) => {
                let count = self.cache.load_recents().len();
                if count == 0 {
                    self.home.status = Some("Nothing to forget".into());
                } else {
                    self.confirmation_modal.show(
                        format!("Forget all {count} recently opened datasets?"),
                        Confirm::ClearRecents,
                    );
                }
            }
            KeyCode::Delete => self.home_forget_selected(),
            KeyCode::Char('~') if self.home.filter.is_empty() => {
                self.home.path_input_active = true;
                self.home.status = None;
                self.home.path_listing = None;
                self.home.path_pick = None;
                self.list_the_typed_directory();
                self.home.pick_first_path();
            }
            // `?` is a key only before typing starts (a filter starting with `?` matches
            // nothing); F1 opens help mid-filter.
            KeyCode::Char('?') if self.home.filter.is_empty() && !ctrl => {
                self.open_help_overlay();
            }
            // Space before typing folds a header, as Enter does, and otherwise does nothing: a
            // one-space filter is invisible and matches every name with a space.
            KeyCode::Char(' ') if self.home.filter.is_empty() && !ctrl => {
                if self.home.selection_is_header() {
                    self.home_toggle_fold();
                }
            }
            KeyCode::Char(c) if !ctrl => {
                self.home.filter.push(c);
                // Typing starts the recursive search, so only someone looking pays for the walk.
                self.spawn_home_search();
                self.home.sync_search_section();
                self.home.select_first_entry();
                #[cfg(feature = "cloud")]
                self.narrow_cloud_listing();
            }
            _ => {}
        }
        None
    }
}
