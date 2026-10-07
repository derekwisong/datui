//! The hex view's App side: opening a file in it, its keys, and its find. The view
//! itself is [`crate::app::hex_view`].

use crate::app::hex_view::{
    Found, HexFindRun, HexHit, HexSource, HexView, MAX_RECORD_SIZE, Origin, PromptKind,
};
use crate::app::jobs::{Answer, Job, Progress};
use crate::{App, AppEvent, InputMode, Overlay};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The hex view, and the number its next read is tagged with.
#[derive(Default)]
pub struct HexState {
    /// The hex view (`InputMode::Hex`), kept while it is up.
    pub view: Option<crate::app::hex_view::HexView>,
    /// Bumped per hex view opened, so a find's answer for another is dropped.
    pub(crate) serial: u64,
}

impl App {
    /// Whether any format spec is on the search path: `B` has something to offer.
    pub(crate) fn has_format_specs(&self) -> bool {
        !self.formats.is_empty()
    }

    /// The hex view on screen, if it is.
    pub fn hex_view(&self) -> Option<&HexView> {
        self.hex
            .view
            .as_ref()
            .filter(|_| self.overlay == Overlay::Hex)
    }

    /// The one local file the dataset was read from, which Info's `x` shows as hex: not
    /// a glob, remote source, stdin, or several paths.
    pub(crate) fn hex_target(&self) -> Option<PathBuf> {
        let several = self
            .source
            .opened
            .as_ref()
            .is_some_and(|(paths, options)| paths.len() > 1 || options.hive);
        self.path.clone().filter(|path| {
            self.data_table_state.is_some()
                && !several
                && !self.reads_stdin()
                && !crate::cloud::source::is_remote_url(path)
                && !crate::cloud::source::is_prefix_or_glob(&path.to_string_lossy())
        })
    }

    /// Show `path` in the hex view once a worker maps it. `fallback`: no reader or spec
    /// took it.
    pub(crate) fn open_hex(
        &mut self,
        path: PathBuf,
        origin: Origin,
        fallback: bool,
        record_size: Option<usize>,
    ) {
        self.stop_hex_find();
        let job = Job::HexOpen {
            origin,
            fallback,
            record_size,
        };
        self.spawn_job(job, Some("Opening as hex..."), move |_| {
            HexSource::open(path.clone())
                .map(|source| Answer::HexOpened(Box::new(source)))
                .map_err(|e| format!("{}: {e}", path.display()))
        });
    }

    /// The file is mapped: the view opens on it.
    pub(crate) fn hex_opened(
        &mut self,
        origin: Origin,
        fallback: bool,
        record_size: Option<usize>,
        source: HexSource,
    ) {
        self.hex.serial += 1;
        let mut view = HexView::new(source, origin, fallback, self.hex.serial);
        view.record_size = record_size.map(|n| n.clamp(1, MAX_RECORD_SIZE));
        view.input = crate::widgets::text_input::TextInput::new().with_theme(&self.theme);
        self.hex.view = Some(view);
        self.open_overlay(Overlay::Hex);
    }

    /// An open found a local file nothing reads, or was asked for its bytes.
    pub(crate) fn land_on_hex(&mut self, hex: crate::loading::Hex) {
        let crate::loading::Hex {
            file,
            from_home,
            asked,
            record_size,
        } = hex;
        self.status_message = None;
        self.busy = false;
        let origin = if from_home {
            Origin::Home
        } else if self.data_table_state.is_some() {
            Origin::Table
        } else {
            Origin::Launch
        };
        self.open_hex(file, origin, !asked, record_size);
    }

    /// Leave the hex view the way it was entered: home, the table, or (from the command
    /// line) out of datui.
    fn leave_hex(&mut self, quit: bool) -> Option<AppEvent> {
        let origin = self.hex.view.as_ref()?.origin;
        match origin {
            Origin::Home => {
                self.enter_home();
                None
            }
            Origin::Table | Origin::Info if quit && !self.source.opened_from_home => {
                Some(AppEvent::Exit)
            }
            Origin::Table | Origin::Info if quit => {
                self.enter_home();
                None
            }
            Origin::Table | Origin::Info => {
                self.close_overlay();
                if self.data_table_state.is_none() {
                    self.input_mode = InputMode::Home;
                }
                // Back to the panel it was opened from, on the tab it was on.
                if origin == Origin::Info && self.data_table_state.is_some() {
                    self.open_overlay(Overlay::Info);
                }
                None
            }
            Origin::Launch if quit => Some(AppEvent::Exit),
            Origin::Launch => None,
        }
    }

    /// What `q` says it does in the hex view.
    pub(crate) fn hex_q_label(&self) -> &'static str {
        match self.hex.view.as_ref().map(|v| v.origin) {
            Some(Origin::Home) => "Home",
            Some(Origin::Table | Origin::Info) if self.source.opened_from_home => "Home",
            _ => "Quit",
        }
    }

    /// Keys in the hex view.
    pub(crate) fn hex_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        let view = self.hex.view.as_mut()?;

        // The spec picker owns the keys while it is open.
        if let Some(picker) = view.picker.as_mut() {
            match event.code {
                KeyCode::Esc => view.picker = None,
                KeyCode::Enter => {
                    let name = picker
                        .selected_original()
                        .map(|i| picker.items()[i].clone());
                    view.picker = None;
                    return name.and_then(|name| self.read_hex_with_spec(name));
                }
                KeyCode::Up => picker.move_up(),
                KeyCode::Down => picker.move_down(),
                KeyCode::Backspace => picker.backspace(),
                KeyCode::Char(c) => picker.filter_key(c, event.modifiers),
                _ => {}
            }
            return None;
        }

        // So does the prompt.
        if let Some(kind) = view.prompt {
            match event.code {
                KeyCode::Esc => {
                    view.prompt = None;
                    view.prompt_error = None;
                    view.input.set_focused(false);
                }
                KeyCode::Enter => return self.hex_prompt_submit(kind),
                KeyCode::Char('u') if ctrl && kind == PromptKind::Find => {
                    view.utf16 = !view.utf16;
                    view.prompt_error = None;
                }
                _ => {
                    view.input.handle_key(event, None);
                    view.prompt_error = None;
                }
            }
            return None;
        }

        let page = view.geometry.rows.max(1) as i64;
        match event.code {
            KeyCode::Esc => {
                if view.inspector_open {
                    view.inspector_open = false;
                } else if view.mark.is_some() {
                    view.mark = None;
                } else {
                    return self.leave_hex(false);
                }
            }
            KeyCode::Char('q') => return self.leave_hex(true),
            KeyCode::Char('?') => self.open_help_overlay(),
            KeyCode::Char('f') if ctrl => view.step_rows(page),
            KeyCode::Char('b') if ctrl => view.step_rows(-page),
            KeyCode::Char('d') if ctrl => view.step_rows((page / 2).max(1)),
            KeyCode::Char('u') if ctrl => view.step_rows(-(page / 2).max(1)),
            _ if ctrl => {}
            KeyCode::Left | KeyCode::Char('h') => view.step(-1),
            KeyCode::Right | KeyCode::Char('l') => view.step(1),
            KeyCode::Up | KeyCode::Char('k') => view.step_rows(-1),
            KeyCode::Down | KeyCode::Char('j') => view.step_rows(1),
            KeyCode::Char('w') => view.next_group(),
            KeyCode::Char('b') => view.previous_group(),
            KeyCode::Char('0') => view.row_start(),
            KeyCode::Char('$') => view.row_end(),
            KeyCode::Home | KeyCode::Char('g') => view.go(0),
            KeyCode::End | KeyCode::Char('G') => view.go(u64::MAX),
            KeyCode::PageDown => view.step_rows(page),
            KeyCode::PageUp => view.step_rows(-page),
            KeyCode::Char('v') => {
                view.mark = match view.mark {
                    Some(_) => None,
                    None if view.is_empty() => None,
                    None => Some(view.cursor),
                };
            }
            KeyCode::Char('#') => view.decimal = !view.decimal,
            KeyCode::Char('i') | KeyCode::Enter => {
                if view.geometry.room_for_panel {
                    view.inspector = !view.inspector;
                } else {
                    view.inspector_open = !view.inspector_open;
                }
            }
            KeyCode::Char(':') => open_prompt(view, PromptKind::GoTo, String::new()),
            KeyCode::Char('f') | KeyCode::Char('/') => {
                let last = view
                    .found
                    .as_ref()
                    .map(|f| f.pattern.label.clone())
                    .unwrap_or_default();
                open_prompt(view, PromptKind::Find, last);
            }
            KeyCode::Char('r') => {
                let now = view.record_size.map(|n| n.to_string()).unwrap_or_default();
                open_prompt(view, PromptKind::RecordSize, now);
            }
            KeyCode::Char('R') => {
                if let Some(stride) = view.found.as_ref().and_then(|f| f.stride)
                    && (1..=MAX_RECORD_SIZE as u64).contains(&stride)
                {
                    view.record_size = Some(stride as usize);
                }
            }
            KeyCode::Char('n') => return self.hex_find_again(true),
            KeyCode::Char('N') => return self.hex_find_again(false),
            KeyCode::Char('B') => self.open_hex_spec_picker(),
            _ => {}
        }
        None
    }

    /// Enter in a prompt: go to the offset, set the bytes per row, or find.
    fn hex_prompt_submit(&mut self, kind: PromptKind) -> Option<AppEvent> {
        let view = self.hex.view.as_mut()?;
        let text = view.input.value().to_string();
        match kind {
            PromptKind::GoTo => {
                match crate::app::hex_view::parse_offset(&text, view.cursor, view.len()) {
                    Ok(at) => {
                        view.go(at);
                        close_prompt(view);
                    }
                    Err(e) => view.prompt_error = Some(e),
                }
            }
            PromptKind::RecordSize => {
                let text = text.trim();
                if text.is_empty() || text == "0" || text.eq_ignore_ascii_case("auto") {
                    view.record_size = None;
                    close_prompt(view);
                } else {
                    match text.parse::<usize>() {
                        Ok(n) if (1..=MAX_RECORD_SIZE).contains(&n) => {
                            view.record_size = Some(n);
                            close_prompt(view);
                        }
                        _ => {
                            view.prompt_error = Some(format!(
                                "{text}: bytes per row are 1 to {MAX_RECORD_SIZE}, or empty for as many as fit"
                            ));
                        }
                    }
                }
            }
            PromptKind::Find => match crate::app::hex_view::parse_pattern(&text, view.utf16) {
                Ok(pattern) => {
                    close_prompt(view);
                    let from = view.cursor;
                    self.start_hex_find(pattern, from, true);
                }
                Err(e) => view.prompt_error = Some(e),
            },
        }
        None
    }

    /// `n` and `N`: the next or previous match of the last pattern, from the cursor.
    fn hex_find_again(&mut self, forward: bool) -> Option<AppEvent> {
        let view = self.hex.view.as_ref()?;
        let pattern = view.found.as_ref()?.pattern.clone();
        let len = view.len();
        if len == 0 {
            return None;
        }
        let from = if forward {
            if view.cursor + 1 >= len {
                0
            } else {
                view.cursor + 1
            }
        } else if view.cursor == 0 {
            len - 1
        } else {
            view.cursor - 1
        };
        self.start_hex_find(pattern, from, forward);
        None
    }

    /// Find `pattern` from `from` on a worker. The keys wait, and Esc stops it.
    fn start_hex_find(&mut self, pattern: crate::app::hex_view::Pattern, from: u64, forward: bool) {
        self.stop_hex_find();
        let Some(view) = self.hex.view.as_mut() else {
            return;
        };
        let stop = Arc::new(AtomicBool::new(false));
        let run = HexFindRun {
            stop: stop.clone(),
            view: view.serial,
            pattern: pattern.clone(),
        };
        let bytes = view.bytes.clone();
        let total = view.len();
        let status = format!("Finding {}...", pattern.label);
        self.spawn_job(Job::HexFind(run), Some(&status), move |worker| {
            let report = worker.reporter();
            let hay = bytes.as_slice();
            let mut hit = crate::app::hex_view::find(hay, &pattern, from, forward, &stop, |read| {
                report(Progress::HexFinding { read, total })
            })
            .map_err(|_| crate::find::CANCELLED.to_string())?;
            hit.stride = hit
                .at
                .and_then(|at| crate::app::hex_view::stride(hay, &pattern, at, &stop));
            Ok(Answer::HexFound(hit))
        });
    }

    /// A find's progress through the file.
    pub(crate) fn hex_find_progress(&mut self, read: u64, total: u64) {
        let Some(label) = self
            .jobs
            .current(|job| matches!(job, Job::HexFind(_)))
            .and_then(|(_, job)| match job {
                Job::HexFind(run) => Some(run.pattern.label.clone()),
                _ => None,
            })
        else {
            return;
        };
        let percent = (read.min(total) * 100).checked_div(total).unwrap_or(100);
        self.status_message = Some(format!("Finding {label}... {percent}%"));
    }

    /// A find answered: the cursor goes to the match.
    pub(crate) fn hex_found(&mut self, run: HexFindRun, hit: HexHit) {
        self.status_message = None;
        let Some(view) = self.hex.view.as_mut().filter(|v| v.serial == run.view) else {
            return;
        };
        view.found = Some(Found {
            pattern: run.pattern.clone(),
            hit: hit.at,
            stride: hit.stride,
        });
        match hit.at {
            Some(at) => {
                view.go(at);
                if hit.wrapped {
                    self.flash_note(format!("Found {}, round past the end", run.pattern.label));
                }
            }
            None => self.flash_note(format!("No match for {}", run.pattern.label)),
        }
    }

    /// Stop the hex view's find, if one is running. Returns whether one was.
    pub(crate) fn stop_hex_find(&mut self) -> bool {
        let Some((_, Job::HexFind(run))) = self.jobs.current(|job| matches!(job, Job::HexFind(_)))
        else {
            return false;
        };
        run.stop.store(true, Ordering::Relaxed);
        self.jobs.cancel(|job| matches!(job, Job::HexFind(_)));
        self.status_message = None;
        true
    }

    /// `B`: every spec on the search path, to read the file with.
    fn open_hex_spec_picker(&mut self) {
        if self.formats.is_empty() {
            self.flash_note(
                "No format specs on the search path; `datui formats` lists where it looks"
                    .to_string(),
            );
            return;
        }
        let names: Vec<String> = self
            .formats
            .specs
            .iter()
            .map(|found| found.spec.name.clone())
            .collect();
        if let Some(view) = self.hex.view.as_mut() {
            view.picker = Some(crate::widgets::ui::PickerState::new(names));
        }
    }

    /// Read the hex view's file with the spec `name`.
    fn read_hex_with_spec(&mut self, name: String) -> Option<AppEvent> {
        let view = self.hex.view.as_ref()?;
        let path = view.path.clone();
        let from_home = view.origin == Origin::Home;
        let options = crate::OpenOptions {
            spec_name: Some(name),
            ..self
                .source
                .opened
                .as_ref()
                .filter(|(paths, _)| paths.first() == Some(&path))
                .map(|(_, options)| options.clone())
                .unwrap_or_default()
        };
        let options = crate::OpenOptions {
            hex: false,
            spec_file: None,
            spec_fetched: None,
            format_read: None,
            format: None,
            ..options
        };
        self.source.opened_from_home = from_home || self.source.opened_from_home;
        // The table takes the screen; a read that fails says why over it.
        self.close_overlay();
        self.set_loading_phase("Scanning input", 10);
        self.name_what_is_loading(path.clone());
        Some(AppEvent::Open(vec![path], options))
    }
}

fn open_prompt(view: &mut HexView, kind: PromptKind, text: String) {
    view.prompt = Some(kind);
    view.prompt_error = None;
    view.input.set_value(text);
    view.input.select_all();
    view.input.set_focused(true);
}

fn close_prompt(view: &mut HexView) {
    view.prompt = None;
    view.prompt_error = None;
    view.input.set_focused(false);
}
