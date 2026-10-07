//! The home screen's keys.

use crate::feedback::Confirm;
use crate::{App, AppEvent, discover, home, source};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::Path;

impl App {
    /// Key handling for the home screen.
    pub(crate) fn home_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // The line beside the prompt answers the last key, and this one replaces it: a
        // key with something to say sets it again below. Left up, "Forgot laps.parquet"
        // stayed until the next time the listing changed.
        self.home.status = None;

        if self.documentation.is_open() {
            self.documentation_key(event);
            return None;
        }

        // The home screen puts every plain character into the filter — `q` has to
        // type a `q`, or you could never search for "quarterly". Quitting is Ctrl+C,
        // handled before this is reached, and Esc once there is no context left to
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
                // The list under the prompt is the directory being typed: ↑↓ pick a
                // name in it, which Enter and Tab then take.
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
                    // A URL is not stat'ed: `exists` asks the working directory about a
                    // file called `gs:`. Its name decides, as it does for a recent — a
                    // file opens, and anything else in a bucket is browsed, where the
                    // listing says what is there.
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
                    // Whether it is there, whether it is a directory and what kind of one
                    // are three filesystem calls, and a typed path is exactly where a
                    // dead mount gets named. All three go to a worker when the mount is
                    // one that might not answer.
                    if self.looking_could_block(&path) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        return Some(AppEvent::ClassifyThenOpen { path, jump: true });
                    }
                    // Before the prompt closes: a typo is worth fixing where it was
                    // typed, rather than retyping the whole path.
                    if !path.exists()
                        && crate::members::split(&path).is_none()
                        && crate::members::split_variant(&path, &self.formats).is_none()
                    {
                        self.home.status = Some(format!("No such path: {}", path.display()));
                        return None;
                    }
                    self.home.path_input.clear();
                    self.home.path_input_active = false;
                    let kind = if path.is_dir() {
                        discover::classify_directory(&path)
                    } else {
                        discover::EntryKind::File
                    };
                    return self.open_what_it_is(path, kind, true);
                }
                KeyCode::Backspace => {
                    self.home.path_input.pop();
                    self.home.status = None;
                }
                KeyCode::Char('u') if ctrl => self.home.path_input.clear(),
                // What the names listed agree on, as a shell completes, or a name
                // picked further down with ↓. Before the listing is in, completion
                // reads the directory on a worker.
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
            // Whatever changed what is typed puts the pick back on the first name
            // that matches, and a new directory is listed.
            self.list_the_typed_directory();
            if !matches!(event.code, KeyCode::Up | KeyCode::Down) {
                self.home.pick_first_path();
            }
            return None;
        }

        // A filter kept from before is selected: a character, Backspace or Delete
        // replaces it, as a selection in any field; any other key keeps it.
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

        // Every plain character types into the filter, so no letter or bracket is
        // a key here: typing "json" must not move the cursor on the "j". Navigation
        // is the arrows and the Ctrl chords, which cannot be part of a name.
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
            // Left/right fold the section the cursor is in, wherever in it the cursor
            // happens to be — so collapsing does not require first finding the header.
            // Tab cycles the sort. Every plain key goes into the filter, so an
            // ordinary letter is not available for this.
            KeyCode::Tab => {
                self.home.sort = self.home.sort.next();
                self.home.select_first_entry();
            }
            KeyCode::Left => self.home_collapse(true),
            KeyCode::Right => match self.selected_directory_to_enter() {
                // Into a directory that opens as one dataset rather than opening it, to
                // reach one partition or one file. This clears the filter, as browsing
                // anywhere does.
                Some(directory) => {
                    // The heading Enter leaves, for the same reason: this is the door
                    // the footer advertises on a lake row, and arriving inside one
                    // with no explanation is the silent wrong answer #237 is about.
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
                        self.open_hex(entry.path, crate::hex_view::Origin::Home, false, None);
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
            // Forget the highlighted entry. Only meaningful in Recent — elsewhere the
            // row is a real directory listing, and datui does not delete files.
            // Shift+Delete forgets the lot. It sits next to the key that forgets
            // one, so it asks first — an accidental press should not silently throw
            // away every place the user has been.
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
            // The one printable that is a key, and only before typing starts: a
            // filter beginning with a literal `?` matches nothing anyway, and this
            // is where a new user asks for the keys. F1 opens help mid-filter.
            KeyCode::Char('?') if self.home.filter.is_empty() && !ctrl => {
                self.open_help_overlay();
            }
            // Space before typing starts folds a header, as Enter does, and is otherwise
            // nothing: a filter of one space is invisible at the prompt and matched every
            // name with a space in it, below the working directory too.
            KeyCode::Char(' ') if self.home.filter.is_empty() && !ctrl => {
                if self.home.selection_is_header() {
                    self.home_toggle_fold();
                }
            }
            KeyCode::Char(c) if !ctrl => {
                self.home.filter.push(c);
                // Typing is what asks for the recursive search. Starting it here and
                // not on open means the walk is only ever paid for by someone who is
                // actually looking for something.
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
