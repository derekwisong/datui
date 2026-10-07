use super::*;
use crate::export_modal::{ExportFocus, ExportFormat};
use crate::{InputMode, OpenOptions};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use std::io::Write;
use std::sync::mpsc;

fn plain(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn ctrl(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
}

fn held(pump: &EventPump) -> Vec<KeyCode> {
    pump.held_keys().map(|k| k.code).collect()
}

fn pump() -> EventPump {
    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), crate::tests::test_runtime());
    EventPump::new(app, tx, rx)
}

/// Type each character as the terminal would deliver it.
fn type_keys(pump: &mut EventPump, text: &str) {
    for c in text.chars() {
        pump.terminal_key(plain(KeyCode::Char(c))).unwrap();
    }
}

/// Run the loop the way `run()` does, without a terminal, until the app is idle
/// with nothing held, nothing on the channel and no background work still to
/// report, or it exits.
///
/// Waits on the work rather than on a quiet spell: a result slower than the spell
/// on a loaded machine is not a result that never comes, and a row count left
/// running lands in the middle of whatever the test does next.
///
/// Anything holding the generation is work still to report: a job lets go in the
/// step that handles its answer, so an idle app holding it is a job still running
/// quietly, or a continuation. The download confirmation's hold is not waited on:
/// that one waits on the user.
fn settle(pump: &mut EventPump) -> Drained {
    fn owed(app: &App) -> bool {
        crate::tests::work_pending(app)
            || (app.work_a_bump_would_strand() && !app.awaiting_open_confirmation())
    }
    // Only a hang guard; nothing here is timed.
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the loop did not settle"
        );
        let replayed = pump.replay_one().unwrap();
        // As `run()` paints after every update.
        pump.app.frame_painted();
        let drained = if owed(&pump.app) && !replayed {
            pump.wait_and_drain(Duration::from_millis(50))
        } else {
            pump.drain()
        }
        .unwrap();
        let updated = match &drained {
            Drained::Continue { updated, .. } => *updated,
            _ => return drained,
        };
        if replayed || updated || owed(&pump.app) {
            continue;
        }
        if pump.held_keys().next().is_none() {
            return drained;
        }
    }
}

/// A pump with a three-row CSV loaded, the way `run()` loads one.
fn loaded_pump() -> (EventPump, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("people.csv");
    let mut file = std::fs::File::create(&path).expect("create csv");
    writeln!(file, "name,age\nada,36\ngrace,45\nalan,41").expect("write csv");
    drop(file);

    let mut pump = pump();
    // The queries typed here are q.
    pump.app.app_config.query.default_mode = crate::QueryMode::Q;
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut pump);
    assert!(
        pump.app.data_table_state.is_some(),
        "the CSV should have loaded"
    );
    assert_eq!(pump.app.input_mode, InputMode::Normal);
    // A frame sets the visible row count, as it has before any key in `run()`.
    rendered(&mut pump.app);
    (pump, dir)
}

/// While a find reads, an `n` typed meanwhile waits and replays once it lands;
/// Esc jumps the queue, stops the find in flight, and drops the `n` held behind it.
#[test]
fn keys_typed_while_a_find_reads_wait_and_esc_stops_it() {
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('f'))).unwrap();
    type_keys(&mut p, "a");
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert!(p.app.finding(), "the find reads in the background");
    p.terminal_key(plain(KeyCode::Char('n'))).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('n')]);
    settle(&mut p);
    assert!(held(&p).is_empty());
    assert_eq!(
        p.app.find_hit(),
        Some((1, "name".to_string())),
        "the held n moved on from the first match"
    );

    p.terminal_key(plain(KeyCode::Char('n'))).unwrap();
    assert!(p.app.finding());
    p.terminal_key(plain(KeyCode::Char('n'))).unwrap();
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    assert!(!p.app.finding(), "Esc acted at once");
    assert!(held(&p).is_empty(), "and dropped the held n");
    settle(&mut p);
    assert!(!p.app.finding(), "no held find started");
    assert_eq!(
        p.app.find_hit(),
        Some((1, "name".to_string())),
        "the cancelled find moved nothing"
    );
}

/// Esc stopping a find drops only the `n` and `N` among the cursor keys held at
/// the front; the other keys keep their place, and an `n` typed after `/` is text.
#[test]
fn esc_stopping_a_find_keeps_the_other_held_keys() {
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('f'))).unwrap();
    type_keys(&mut p, "a");
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert!(p.app.finding());
    type_keys(&mut p, "njN/n");
    assert_eq!(
        held(&p),
        [
            KeyCode::Char('n'),
            KeyCode::Char('j'),
            KeyCode::Char('N'),
            KeyCode::Char('/'),
            KeyCode::Char('n'),
        ]
    );
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    assert!(!p.app.finding());
    assert_eq!(
        held(&p),
        [KeyCode::Char('j'), KeyCode::Char('/'), KeyCode::Char('n')]
    );
}

/// Each `n` typed while a find reads is held and replayed, as the column cursor's
/// keys are: five typed move five matches, not one.
#[test]
fn every_n_typed_while_a_find_reads_moves_a_match() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("matches.csv");
    let mut file = std::fs::File::create(&path).expect("create csv");
    writeln!(file, "id,v").expect("write csv");
    for row in 0..10 {
        writeln!(file, "{row},x{row}").expect("write csv");
    }
    drop(file);
    let mut p = pump();
    p.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut p);
    rendered(&mut p.app);

    p.terminal_key(plain(KeyCode::Char('f'))).unwrap();
    type_keys(&mut p, "x");
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert!(p.app.finding());
    for _ in 0..5 {
        p.terminal_key(plain(KeyCode::Char('n'))).unwrap();
    }
    assert_eq!(held(&p), [KeyCode::Char('n'); 5], "none collapsed");
    settle(&mut p);
    assert!(held(&p).is_empty());
    assert_eq!(p.app.find_hit(), Some((5, "v".to_string())));

    p.terminal_key(plain(KeyCode::Char('N'))).unwrap();
    assert!(p.app.finding());
    p.terminal_key(plain(KeyCode::Char('N'))).unwrap();
    p.terminal_key(plain(KeyCode::Char('N'))).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('N'); 2]);
    settle(&mut p);
    assert_eq!(p.app.find_hit(), Some((2, "v".to_string())));
}

/// Enter held behind `n` waits as Space, as behind any key that moves the cursor.
#[test]
fn enter_held_behind_n_waits_as_space() {
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('f'))).unwrap();
    type_keys(&mut p, "a");
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert!(p.app.finding());
    p.terminal_key(plain(KeyCode::Char('n'))).unwrap();
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('n'), KeyCode::Char(' ')]);
}

/// Keys typed at the inspector while it reads a row's hidden fields wait,
/// Esc among them, and replay in order once the read lands.
#[test]
fn keys_typed_while_the_inspector_reads_wait_their_turn() {
    let (mut p, _dir) = loaded_pump();
    let state = p.app.data_table_state.as_mut().unwrap();
    state.set_column_order(vec!["name".to_string()]);
    rendered(&mut p.app);
    p.terminal_key(plain(KeyCode::Char(' '))).unwrap();
    assert_eq!(p.app.input_mode, InputMode::Inspect);
    p.terminal_key(plain(KeyCode::End)).unwrap();
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert!(
        p.app.is_busy(),
        "the hidden field is read in the background"
    );
    p.terminal_key(plain(KeyCode::Char('k'))).unwrap();
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('k'), KeyCode::Esc]);
    assert_eq!(p.app.input_mode, InputMode::Inspect, "nothing acted yet");
    settle(&mut p);
    assert!(held(&p).is_empty());
    assert_eq!(
        p.app.input_mode,
        InputMode::Normal,
        "Esc closed it, in turn"
    );
    assert_eq!(
        p.app.inspector_modal.focused().map(|f| f.name.as_str()),
        Some("name"),
        "k moved first"
    );
}

/// #615: keys typed while the inspector parses long JSON text wait, then
/// replay inside the level it opens.
#[test]
fn keys_typed_while_json_parses_replay_inside_the_level() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("docs.csv");
    // Over the 64 KB parsed on the key.
    let doc = format!("[{}0]", "0, ".repeat(30_000));
    std::fs::write(&path, format!("doc\n\"{doc}\"\n")).expect("write csv");
    let mut p = pump();
    p.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut p);
    rendered(&mut p.app);
    p.terminal_key(plain(KeyCode::Char(' '))).unwrap();
    rendered(&mut p.app);
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert!(p.app.is_busy(), "the text is parsed in the background");
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('j'), KeyCode::Char('j')]);
    settle(&mut p);
    let drill = p
        .app
        .inspector_modal
        .drill
        .as_ref()
        .expect("the array opened");
    assert_eq!(drill.level().node.len(), 30_001);
    assert_eq!(drill.level().selected, 2, "both j moved inside it");
}

fn rendered(app: &mut App) -> String {
    let area = Rect::new(0, 0, 100, 20);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    buf.content().iter().map(|c| c.symbol()).collect()
}

fn mouse(kind: crossterm::event::MouseEventKind, (x, y): (u16, u16)) -> MouseEvent {
    MouseEvent {
        kind,
        column: x,
        row: y,
        modifiers: KeyModifiers::NONE,
    }
}

fn click(at: (u16, u16)) -> MouseEvent {
    use crossterm::event::{MouseButton, MouseEventKind};
    mouse(MouseEventKind::Down(MouseButton::Left), at)
}

/// Where `text` is drawn in a 100×20 frame, in cells.
fn on_screen(app: &mut App, text: &str) -> (u16, u16) {
    let area = Rect::new(0, 0, 100, 20);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    for y in 0..area.height {
        let line: String = (0..area.width).map(|x| buf[(x, y)].symbol()).collect();
        if let Some(at) = line.find(text) {
            return (line[..at].chars().count() as u16, y);
        }
    }
    panic!("{text:?} is not on screen");
}

/// The table's cursor: the row on screen and the column.
fn cell(pump: &EventPump) -> (Option<usize>, Option<String>) {
    let state = pump.app.data_table_state.as_ref().expect("a dataset");
    (
        state.table_state.selected(),
        state.current_column().map(str::to_string),
    )
}

/// A click on a cell puts the cursor on its row and column; a second click there
/// is a double click, which presses Enter and inspects the row.
#[test]
fn a_click_moves_the_cursor_and_a_double_click_inspects() {
    let (mut p, _dir) = loaded_pump();
    assert_eq!(cell(&p), (Some(0), Some("name".to_string())));
    let at = on_screen(&mut p.app, "45");
    assert!(p.terminal_mouse(click(at)).unwrap());
    assert_eq!(cell(&p), (Some(1), Some("age".to_string())));
    assert_eq!(p.app.input_mode, InputMode::Normal, "one click only moves");

    // A click on the other row, then twice on it.
    let at = on_screen(&mut p.app, "alan");
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(cell(&p), (Some(2), Some("name".to_string())));
    assert_eq!(p.app.input_mode, InputMode::Normal);
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(p.app.input_mode, InputMode::Inspect, "Enter inspects");

    // In the inspector the table is not under the pointer: nothing to click.
    let before = cell(&p);
    p.terminal_mouse(click((3, 3))).unwrap();
    assert_eq!(cell(&p), before);
}

/// The wheel presses the arrows: down three rows at a time, stopping at the last,
/// and across, or with Shift, the column cursor.
#[test]
fn the_wheel_scrolls_rows_and_across_moves_the_column() {
    use crossterm::event::MouseEventKind;
    let (mut p, _dir) = numbered_pump(50);
    let (mut p2, _dir2) = loaded_pump();
    let rows = |p: &EventPump| cell(p).0;
    assert!(
        p.terminal_mouse(mouse(MouseEventKind::ScrollDown, (5, 5)))
            .unwrap()
    );
    assert_eq!(rows(&p), Some(3));
    p.terminal_mouse(mouse(MouseEventKind::ScrollUp, (5, 5)))
        .unwrap();
    assert_eq!(rows(&p), Some(0));

    let mut shift = mouse(MouseEventKind::ScrollDown, (5, 5));
    shift.modifiers = KeyModifiers::SHIFT;
    p2.terminal_mouse(shift).unwrap();
    assert_eq!(cell(&p2), (Some(0), Some("age".to_string())));
    p2.terminal_mouse(mouse(MouseEventKind::ScrollLeft, (5, 5)))
        .unwrap();
    assert_eq!(cell(&p2), (Some(0), Some("name".to_string())));
    p2.terminal_mouse(mouse(MouseEventKind::ScrollRight, (5, 5)))
        .unwrap();
    assert_eq!(cell(&p2).1.as_deref(), Some("age"));
    // Three rows down from the first, of three: the last, as ↓ stops there.
    p2.terminal_mouse(mouse(MouseEventKind::ScrollDown, (5, 5)))
        .unwrap();
    rendered(&mut p2.app);
    assert_eq!(rows(&p2), Some(2));
}

/// Mouse input is never held. At a busy table the wheel across moves the column
/// cursor at once, as ←→ do; the wheel down, which waits as a typed ↓ would, and a
/// click, aimed at a screen that may be gone by the time it could act, are dropped.
/// Behind held keys a click is dropped too, so it cannot overtake them.
#[test]
fn mouse_input_is_never_held() {
    use crossterm::event::MouseEventKind;
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    p.app.busy = true;
    assert!(
        !p.terminal_mouse(mouse(MouseEventKind::ScrollDown, (5, 5)))
            .unwrap()
    );
    assert!(!p.terminal_mouse(click(alan)).unwrap());
    assert!(held(&p).is_empty(), "nothing held");
    assert_eq!(cell(&p), (Some(0), Some("name".to_string())));
    assert!(
        p.terminal_mouse(mouse(MouseEventKind::ScrollRight, (5, 5)))
            .unwrap()
    );
    assert_eq!(cell(&p), (Some(0), Some("age".to_string())), "at once");

    // Idle again with a key held: the key goes first, the click is dropped.
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('j')]);
    p.app.busy = false;
    assert!(!p.terminal_mouse(click(alan)).unwrap());
    settle(&mut p);
    assert_eq!(cell(&p).0, Some(1), "the held j, and no click");
    assert!(p.terminal_mouse(click(alan)).unwrap());
    assert_eq!(cell(&p).0, Some(2));
}

/// A click dropped while busy is not the first of a double click: the next click
/// on the same cell only moves the cursor, and does not inspect.
#[test]
fn a_dropped_click_does_not_make_the_next_a_double_click() {
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    p.app.busy = true;
    assert!(!p.terminal_mouse(click(alan)).unwrap());
    p.app.busy = false;
    assert!(p.terminal_mouse(click(alan)).unwrap());
    assert_eq!(cell(&p).0, Some(2));
    assert_eq!(p.app.input_mode, InputMode::Normal, "one click, no Enter");
}

/// A click read behind an event that may have changed the screen waits for the
/// frame that shows the change, so it lands on what the user sees.
#[test]
fn a_click_waits_for_the_frame_after_a_change() {
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    p.app.frame_painted();
    p.send(AppEvent::Terminal(Event::Resize(100, 20))).unwrap();
    p.send(AppEvent::Terminal(Event::Mouse(click(alan))))
        .unwrap();
    let drained = p.drain().unwrap();
    assert!(
        matches!(
            drained,
            Drained::Continue {
                updated: true,
                progress_only: false
            }
        ),
        "a frame is asked for: {drained:?}"
    );
    assert_eq!(cell(&p).0, Some(0), "not yet");
    rendered(&mut p.app);
    p.app.frame_painted();
    p.drain().unwrap();
    assert_eq!(cell(&p).0, Some(2), "on the frame that shows the resize");
}

/// A key the footer shows presses it, as typed: `? keys` opens help, and
/// while the help is up the wheel moves its selection.
#[test]
fn a_chip_presses_its_key() {
    use crossterm::event::MouseEventKind;
    let (mut p, _dir) = loaded_pump();
    let help = on_screen(&mut p.app, "? keys");
    assert!(p.terminal_mouse(click(help)).unwrap());
    assert!(p.app.help_visible(), "the Help chip opened help");
    rendered(&mut p.app);
    p.terminal_mouse(mouse(MouseEventKind::ScrollDown, (5, 5)))
        .unwrap();
    assert_eq!(p.app.help.selected, 3);
    // No table under the help: a click there moves nothing.
    p.terminal_mouse(click((5, 5))).unwrap();
    assert_eq!(cell(&p), (Some(0), Some("name".to_string())));
}

/// A dialog's footer chips press their keys as the status footer's do: Esc
/// Cancel closes the export form, and over a question only its own chips take
/// clicks.
#[test]
fn a_dialog_s_footer_chips_press_their_keys() {
    let (mut p, _dir) = loaded_pump();
    p.send(AppEvent::Terminal(Event::Key(plain(KeyCode::Char('e')))))
        .unwrap();
    settle(&mut p);
    assert!(p.app.export_modal.active);
    let cancel = on_screen(&mut p.app, "Cancel");
    assert!(p.terminal_mouse(click(cancel)).unwrap());
    settle(&mut p);
    assert!(!p.app.export_modal.active, "Esc Cancel closed the form");

    // A question: its Cancel chip answers it; a click beside it does nothing.
    p.app.confirmation_modal.show(
        "Delete it?".to_string(),
        crate::feedback::Confirm::ClearRecents,
    );
    p.terminal_mouse(click((0, 2))).unwrap();
    settle(&mut p);
    assert!(p.app.confirmation_modal.active, "outside the question");
    let cancel = on_screen(&mut p.app, "Cancel");
    p.terminal_mouse(click(cancel)).unwrap();
    settle(&mut p);
    assert!(!p.app.confirmation_modal.active, "its Esc chip answered it");

    // Help's own footer: Esc Close.
    p.send(AppEvent::Terminal(Event::Key(plain(KeyCode::Char('?')))))
        .unwrap();
    settle(&mut p);
    assert!(p.app.help_visible());
    let close = on_screen(&mut p.app, "Close");
    p.terminal_mouse(click(close)).unwrap();
    settle(&mut p);
    assert!(!p.app.help_visible(), "help's Close chip closed it");
}

/// Mouse events reach the app through the channel, in order with the keys typed
/// around them, as `run()` reads them.
#[test]
fn mouse_events_keep_their_place_among_the_keys() {
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    p.send(AppEvent::Terminal(Event::Key(plain(KeyCode::Char('l')))))
        .unwrap();
    p.send(AppEvent::Terminal(Event::Mouse(click(alan))))
        .unwrap();
    settle(&mut p);
    assert_eq!(
        cell(&p),
        (Some(2), Some("name".to_string())),
        "the l moved to age, then the click came back to name"
    );
}

/// A continuation never goes to the back of the channel.
///
/// The gap #221 was left short by is created by exactly one thing: putting a
/// handler's follow-up behind whatever arrived while that handler ran.
/// `queue_continuation` is the only way a follow-up should travel, holding the
/// generation, and `EventPump::send`, for callers pushing an event of their own, is
/// the only `tx.send` that belongs in this file.
#[test]
fn a_continuation_never_goes_to_the_back_of_the_channel() {
    let source = include_str!("../event_pump.rs");
    let needle = "self.tx.send(";
    assert_eq!(
        source.matches(needle).count(),
        1,
        "the one send left should be `EventPump::send`. A follow-up sent to the \
         channel holds nothing while it waits, and the generation reads free in the \
         middle of an errand — see `jobs::Hold`."
    );
}

/// A continuation holds the generation until it has been dispatched.
///
/// This is the gap #221 was left short by. An errand of several phases hands off
/// through a returned event, and the pump breaks there so a frame can be drawn —
/// so for one iteration of the loop the phase that finished has let go of the
/// generation and the phase that follows has not taken it. A collect starting in
/// that window bumps `task_generation` out from under the errand, whose answer is
/// then thrown away and never asked for again.
///
/// An export is the errand used here: `Export` draws its progress and returns
/// `DoExport` before anything has been spawned at all.
#[test]
fn a_continuation_holds_the_generation_until_it_is_dispatched() {
    let (mut p, dir) = loaded_pump();
    let out = dir.path().join("out.csv");
    assert!(!p.app.work_a_bump_would_strand(), "nothing is running yet");
    // The export's worker waits until it has been seen running.
    let (waits, release) = crate::tests::worker_waits_once(|job| matches!(job, crate::Job::Export));
    p.app.jobs.worker_waits = waits;

    p.send(AppEvent::Export(csv_export(&out))).unwrap();
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));

    // `Export` has returned `DoExport` and nothing has been spawned: this is the
    // moment the loop draws a frame and reads the terminal.
    assert!(
        !p.next_up.is_empty(),
        "the continuation is waiting to be dispatched"
    );
    assert!(
        p.app.work_a_bump_would_strand(),
        "and the generation is held while it waits"
    );

    // Dispatching it hands the generation to the phase it starts, rather than
    // letting go before the other takes it.
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
    assert!(
        p.app.work_a_bump_would_strand(),
        "the export it started is running now, and holds it in turn"
    );

    release.send(()).unwrap();
    settle(&mut p);
    assert!(out.exists(), "and the export finishes, which is the point");
    assert!(
        !p.app.work_a_bump_would_strand(),
        "with the generation free again afterwards"
    );
}

/// A key handled in that window does not find the generation free either.
///
/// The pump breaks so a frame can be drawn and the terminal polled, so exactly one
/// key can be handled between a continuation being queued and being dispatched.
/// Ctrl-C and the other hard escapes act even while busy, and `App::handle` runs the
/// deferred errands at its tail whatever the key was.
#[test]
fn a_key_in_the_handoff_window_does_not_find_the_generation_free() {
    let (mut p, dir) = loaded_pump();
    let out = dir.path().join("out.csv");

    // An errand waiting for the generation to come free. Without one the tail of
    // `App::handle` has nothing to run, and the key below would prove nothing.
    p.app.owe_rows_for_tests("Loading buffer...");

    // And the user exports, which after one drain is mid-handoff.
    p.send(AppEvent::Export(csv_export(&out))).unwrap();
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
    assert!(!p.next_up.is_empty(), "mid-handoff");
    let held_at = p.app.task_generation();

    p.terminal_key(plain(KeyCode::Char('?'))).unwrap();

    assert_eq!(
        p.app.task_generation(),
        held_at,
        "the owed collect did not go in on the back of a key handled in the window"
    );
    assert!(
        p.app.rows_owed(),
        "it is still owed, waiting for the export in front of it"
    );
}

/// An errand of several phases never lets go of the generation until it is done.
///
/// The invariant #221 actually wants, asserted at the boundary rather than through a
/// proxy: every time the pump breaks — which is every time a frame is drawn and a key
/// could be handled — an errand still in progress holds the generation. What used to
/// cover this was a list of three flags in the predicate; what covers it now is the
/// continuation's hold, and that has to be true at each phase change rather than only
/// at the first.
///
/// An export hands off once, from drawing its progress to the job, which then
/// runs from plan to committed file holding the generation itself, and lets go in
/// the step that handles its answer.
#[test]
fn an_export_holds_the_generation_at_every_phase_change() {
    let (mut p, dir) = loaded_pump();
    let out = dir.path().join("out.csv");

    p.send(AppEvent::Export(csv_export(&out))).unwrap();

    let mut breaks = 0;
    for _ in 0..10_000 {
        let drained = if p.app.is_busy() {
            p.wait_and_drain(Duration::from_secs(10)).unwrap()
        } else {
            p.drain().unwrap()
        };
        match drained {
            Drained::Continue { updated, .. } => {
                // Mid-errand, at the moment the loop would draw and poll.
                if !p.next_up.is_empty() {
                    breaks += 1;
                }
                if !p.next_up.is_empty() || p.app.is_busy() {
                    assert!(
                        p.app.work_a_bump_would_strand(),
                        "the export left the generation free after {breaks} hand-offs"
                    );
                }
                if !updated && p.next_up.is_empty() && !p.app.is_busy() {
                    break;
                }
            }
            other => panic!("the export should not end the loop: {other:?}"),
        }
    }

    assert!(
        breaks >= 1,
        "the export handed off to its job, and that was checked; saw {breaks}"
    );
    assert!(out.exists(), "and the file was written, which is the point");
    // The answer that cleared `busy` released the generation in the same step: no
    // release is left to arrive behind it (#490).
    assert!(
        !p.app.work_a_bump_would_strand(),
        "the generation is free the moment the export is done"
    );
}

fn csv_export(path: &std::path::Path) -> crate::ExportRequest {
    crate::ExportRequest {
        path: path.to_path_buf(),
        format: ExportFormat::Csv,
        options: crate::ExportOptions {
            csv_delimiter: b',',
            csv_include_header: true,
            source_file: false,
            csv_compression: None,
            json_compression: None,
            ndjson_compression: None,
        },
        overwrite: crate::output_file::Overwrite::Forbid,
    }
}

/// #455: an export whose worker dies ends: the reason on the export dialog's
/// status line, the keyboard back, nothing of its own left set, no file and
/// the generation free. The next export writes its file.
#[test]
fn an_export_whose_worker_dies_ends_and_the_next_one_writes() {
    let (mut p, dir) = loaded_pump();
    let out = dir.path().join("out.csv");
    let mut exports = 0;
    p.app.jobs.worker_dies = Some(Box::new(move |job| {
        exports += usize::from(matches!(job, crate::Job::Export));
        exports == 1
    }));
    p.send(AppEvent::Export(csv_export(&out))).unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    while p.app.export_modal.path_error.is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the export never ended"
        );
        p.wait_and_drain(Duration::from_millis(50)).unwrap();
    }
    let reason = p.app.export_modal.path_error.clone().unwrap_or_default();
    assert!(reason.contains("worker died"), "{reason}");
    assert!(p.app.export_modal.active);
    assert!(!p.app.error_modal.active);
    assert!(!p.app.is_busy());
    assert!(p.app.status_message.is_none());
    assert!(p.app.nothing_loading());
    assert!(!out.exists());
    assert!(
        !p.app.work_a_bump_would_strand(),
        "the generation is free as the failure is shown"
    );

    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    assert!(!p.app.export_modal.active);
    p.send(AppEvent::Export(csv_export(&out))).unwrap();
    settle(&mut p);
    assert!(!p.app.error_modal.active, "{}", p.app.error_modal.message);
    assert_eq!(p.app.export_modal.path_error, None);
    assert!(out.exists(), "the next export writes its file");
    assert!(p.app.nothing_loading());
}

/// #455: a drill whose row read dies says so in one line, pointing at the log
/// rather than spilling the internal error into the flash, and the next Enter
/// drills.
#[test]
fn a_drill_whose_read_dies_flashes_one_line_and_the_next_one_drills() {
    let (mut p, _dir) = loaded_pump();
    p.send(AppEvent::QQuery("select n: count age by name".to_string()))
        .unwrap();
    settle(&mut p);
    // With the key hidden the buffer cannot say which group a row is, so Enter
    // reads it on a worker.
    p.app
        .data_table_state
        .as_mut()
        .unwrap()
        .set_column_order(vec!["n".to_string()]);
    settle(&mut p);
    rendered(&mut p.app);

    p.app.jobs.worker_dies =
        crate::tests::worker_dies_once(|job| matches!(job, crate::Job::DrillRow));
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    settle(&mut p);
    assert_eq!(
        p.app.flash_message(),
        Some("Could not drill in; see the log")
    );
    assert!(!p.app.is_busy());
    assert!(p.app.status_message.is_none());
    assert!(!p.app.error_modal.active);
    assert!(!p.app.data_table_state.as_ref().unwrap().is_drilled_down());

    rendered(&mut p.app);
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    settle(&mut p);
    assert!(
        p.app.data_table_state.as_ref().unwrap().is_drilled_down(),
        "the next one drills"
    );
}

/// #455: a pivot whose worker dies says so and leaves nothing waiting on it — the
/// form is not stuck computing — and the next pivot is installed.
#[test]
fn a_pivot_whose_worker_dies_is_shown_and_the_next_one_installs() {
    use crate::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("long.csv");
    std::fs::write(&path, "day,key,val\n1,a,10\n1,b,20\n2,a,30\n2,b,40\n").unwrap();
    let mut p = pump();
    p.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut p);
    rendered(&mut p.app);
    let spec = PivotSpec {
        index: vec!["day".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };

    p.app.input_mode = InputMode::PivotMelt;
    p.app.jobs.worker_dies = crate::tests::worker_dies_once(|job| matches!(job, crate::Job::Pivot));
    p.send(AppEvent::Pivot(spec.clone())).unwrap();
    settle(&mut p);
    assert!(p.app.error_modal.active, "the user is told");
    assert!(!p.app.is_busy());
    assert!(!p.app.pivot_computing(), "the form is not left computing");
    assert_eq!(p.app.input_mode, InputMode::PivotMelt, "and keeps the spec");
    let state = p.app.data_table_state.as_ref().unwrap();
    assert!(state.last_pivot_spec().is_none(), "the table is as it was");

    p.app.error_modal.hide();
    p.send(AppEvent::Pivot(spec)).unwrap();
    settle(&mut p);
    assert!(!p.app.error_modal.active, "{}", p.app.error_modal.message);
    let state = p.app.data_table_state.as_ref().unwrap();
    assert!(
        state.last_pivot_spec().is_some(),
        "the next pivot is installed"
    );
    assert_eq!(p.app.input_mode, InputMode::Normal);
}

/// #455: failures from work an open has passed are dropped: the open goes on to its
/// rows, with no error over them, whatever kind of job the failure names.
#[test]
fn stale_failures_leave_a_newer_open_alone() {
    let (mut p, dir) = loaded_pump();
    let passed = p.app.task_generation();
    let shown = p.app.dataset_generation;
    // An open long since replaced.
    let gone = crate::loading::LoadId::for_tests(u64::MAX);
    let jobs = [
        crate::Job::Load(gone),
        crate::Job::OpenNamed(gone),
        crate::Job::Rows(crate::InflightCollect::for_tests(0, 3)),
        crate::Job::Analysis(crate::jobs::AnalysisRun::default()),
        crate::Job::SampleRows,
        crate::Job::Pivot,
        crate::Job::DrillRow,
        crate::Job::InspectRow { frame: 0, row: 0 },
        crate::Job::InspectJson { token: 0 },
        crate::Job::Export,
        crate::Job::Copy,
        crate::Job::QualityReport,
    ];
    let running: Vec<_> = jobs
        .into_iter()
        .map(|job| p.app.job_for_tests(job, None))
        .collect();
    // The open's first phase waits until the failures are in, so it is under way
    // when they land.
    let (waits, release) = crate::tests::worker_waits_once(|job| {
        matches!(job, crate::Job::OpenNamed(_) | crate::Job::Load(_))
    });
    p.app.jobs.worker_waits = waits;
    p.send(AppEvent::Open(
        vec![dir.path().join("people.csv")],
        OpenOptions::default(),
    ))
    .unwrap();
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
    assert!(p.app.awaiting_dataset(), "the open is under way");
    assert_ne!(p.app.task_generation(), passed);
    for job in running {
        job.end(crate::Outcome::Failed {
            message: "from work long gone".to_string(),
            panicked: true,
        });
    }
    release.send(()).unwrap();
    settle(&mut p);
    assert!(!p.app.error_modal.active, "{}", p.app.error_modal.message);
    assert_ne!(p.app.dataset_generation, shown, "the open finished");
    assert!(!p.app.awaiting_dataset());
    assert!(p.app.home_app.last_load_error.is_none());
    assert!(p.app.nothing_loading());
}

/// #455: an open whose schema read dies — its second worker, after the scan —
/// holds the keys and the generation in every frame drawn on the way, then ends
/// where a failed open ends: the reason shown, the dataset before it still up.
/// Opening again works.
#[test]
fn an_open_whose_schema_read_dies_ends_and_the_next_one_opens() {
    let (mut p, dir) = loaded_pump();
    let shown = p.app.dataset_generation;
    let path = dir.path().join("people.csv");
    // The scan is the first load job; the schema read is the second.
    let mut loads = 0;
    p.app.jobs.worker_dies = Some(Box::new(move |job| {
        loads += usize::from(matches!(job, crate::Job::Load(_)));
        loads == 2
    }));
    // The first phase waits for the first frame, so the open is seen under way.
    let (waits, release) = crate::tests::worker_waits_once(|job| {
        matches!(job, crate::Job::OpenNamed(_) | crate::Job::Load(_))
    });
    p.app.jobs.worker_waits = waits;
    let mut release = Some(release);
    p.send(AppEvent::Open(vec![path.clone()], OpenOptions::default()))
        .unwrap();
    let mut frames = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    while !p.app.error_modal.active {
        assert!(std::time::Instant::now() < deadline, "the open never ended");
        p.wait_and_drain(Duration::from_millis(50)).unwrap();
        if let Some(release) = release.take() {
            release.send(()).unwrap();
        }
        if p.app.awaiting_dataset() {
            frames += 1;
            assert!(p.app.is_busy(), "busy at frame {frames}");
            assert!(p.app.work_a_bump_would_strand(), "held at frame {frames}");
        }
    }
    assert!(frames >= 1, "the open was under way between frames");
    assert!(
        p.app.error_modal.message.contains("worker died"),
        "{}",
        p.app.error_modal.message
    );
    settle(&mut p);
    assert!(!p.app.is_busy());
    assert!(!p.app.awaiting_dataset(), "nothing is waited on");
    assert!(p.app.nothing_loading());
    assert_eq!(
        p.app.dataset_generation, shown,
        "the dataset before it stays"
    );

    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    p.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut p);
    assert!(!p.app.error_modal.active, "{}", p.app.error_modal.message);
    assert_ne!(p.app.dataset_generation, shown, "the next open opens");
}

/// A deferred collect that turns out to have nothing to do still ends an open's
/// wait for its first rows.
///
/// An open's first rows are owed rather than read when other work holds the
/// generation as the dataset installs. The retry that runs then finds the buffer
/// already serves the view; left to the open alone, the bar read "Loading
/// buffer... 70%" with the app idle, for the rest of the session.
#[test]
fn a_deferred_collect_with_nothing_to_do_takes_the_loading_screen_down() {
    let (mut p, _dir) = loaded_pump();
    // The buffer already holds every row, so the collect will find nothing to do.
    p.app.owe_rows_for_tests("Loading buffer...");
    p.app.loading.first_rows_for_tests();
    p.app.busy = true;

    p.send(AppEvent::Update).unwrap();
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));

    assert!(!p.app.rows_owed(), "the errand is done either way");
    assert!(
        p.app.nothing_loading(),
        "and the loading screen is down rather than stuck at 70%"
    );
    assert!(!p.app.is_busy(), "with the keyboard back");
}

/// While rows are being read, keys that only redraw the table act at once (#646):
/// `#` toggles row numbers and `j` moves inside the rows held. Busy with anything
/// else, `j` waits in the order typed.
#[test]
fn view_keys_act_while_rows_load() {
    let (mut p, _dir) = loaded_pump();
    p.app.owe_rows_for_tests("Loading buffer...");
    assert!(p.app.is_busy());
    let numbered = p.app.data_table_state.as_ref().unwrap().row_numbers();

    p.terminal_key(plain(KeyCode::Char('#'))).unwrap();
    assert_ne!(
        p.app.data_table_state.as_ref().unwrap().row_numbers(),
        numbered,
        "# toggles at once"
    );
    let row = p.app.data_table_state.as_ref().unwrap().cursor_row();
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    assert_eq!(
        p.app.data_table_state.as_ref().unwrap().cursor_row(),
        row + 1,
        "j inside the rows held moves at once"
    );
    assert!(p.held.is_empty(), "nothing waits");

    // Busy with more than rows, a key waits, and the j typed after it waits
    // behind it.
    p.app.busy = true;
    p.terminal_key(plain(KeyCode::Char('s'))).unwrap();
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    assert_eq!(p.held.len(), 2);
    assert_eq!(
        p.app.data_table_state.as_ref().unwrap().cursor_row(),
        row + 1
    );
}

/// The app hands a key it cannot act on back to the caller rather than dropping
/// it; `event()`, for callers with nowhere to hold one, drops it as before.
#[test]
fn a_key_while_busy_comes_back_deferred() {
    let mut p = pump();
    p.app.busy = true;
    assert!(matches!(
        p.app.handle(&AppEvent::Key(plain(KeyCode::Char('j')))),
        Err(k) if k.code == KeyCode::Char('j')
    ));
    assert!(
        p.app
            .event(&AppEvent::Key(plain(KeyCode::Char('j'))))
            .is_none()
    );
    p.app.busy = false;
    assert!(matches!(
        p.app.handle(&AppEvent::Key(plain(KeyCode::Char('j')))),
        Ok(None)
    ));
}

/// Typed at a spinner, `/hello` opens the query bar with "hello" in it once the
/// work is done. Nothing in it acts early: the `h` and `l` do not scroll a column
/// and the `q` in `/query` does not quit.
#[test]
fn a_query_typed_while_busy_lands_in_the_query_bar() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    type_keys(&mut p, ":hello");
    assert_eq!(
        p.app.input_mode,
        InputMode::Normal,
        "nothing acts while busy"
    );
    assert_eq!(held(&p).len(), 6);
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));

    p.app.busy = false;
    assert!(matches!(settle(&mut p), Drained::Continue { .. }));
    assert_eq!(p.app.input_mode, InputMode::Editing);
    assert_eq!(p.app.prompt.query_input.value(), "hello");

    p.app.busy = true;
    type_keys(&mut p, "query");
    p.app.busy = false;
    assert!(
        matches!(settle(&mut p), Drained::Continue { .. }),
        "q did not quit"
    );
    assert_eq!(p.app.prompt.query_input.value(), "helloquery");
}

/// Ctrl+T typed at a spinner waits its turn like the letters around it: the
/// prompt opens, switches mode, and the text lands in the mode switched to. A
/// build without SQL has one language, so the chord leaves the line on q.
#[test]
fn a_mode_chord_typed_while_busy_switches_before_the_text() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    type_keys(&mut p, ":");
    p.terminal_key(ctrl('t')).unwrap();
    type_keys(&mut p, "ada");
    assert_eq!(held(&p).len(), 5, "the chord is held with the text");
    p.app.busy = false;
    settle(&mut p);

    let mode = crate::QueryMode::Q.next();
    assert_eq!(p.app.query_prompt_mode(), Some(mode));
    let typed = match mode {
        crate::QueryMode::Sql => &p.app.prompt.sql_input,
        crate::QueryMode::Q => &p.app.prompt.query_input,
    };
    assert_eq!(typed.value(), "ada");
    if mode != crate::QueryMode::Q {
        assert_eq!(p.app.prompt.query_input.value(), "");
    }
}

/// A key that arrives behind the event that ended the busy state is handled after
/// the keys typed before it, not before them.
#[test]
fn a_fresh_key_waits_behind_the_held_ones() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    type_keys(&mut p, ":abc");
    // The busy-clearing event was handled; the next key read is still behind.
    p.app.busy = false;
    type_keys(&mut p, "d");
    assert_eq!(p.app.input_mode, InputMode::Normal, "d waited its turn");

    settle(&mut p);
    assert_eq!(p.app.prompt.query_input.value(), "abcd");
}

/// A held Enter runs its search before the key typed after it is offered: the G
/// stays held while the search collects, then lands on the last row of the result.
#[test]
fn a_held_enter_finishes_its_search_before_the_next_key() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    type_keys(&mut p, ":select name where age > 40");
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    p.terminal_key(plain(KeyCode::Char('G'))).unwrap();
    p.app.busy = false;

    // Replay up to and including the Enter; the Search it asks for is on the channel.
    while held(&p).len() > 1 {
        assert!(p.replay_one().unwrap());
        if held(&p).len() > 1 {
            assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
        }
    }
    assert_eq!(
        p.app.input_mode,
        InputMode::Editing,
        "the search has not run"
    );
    assert_eq!(held(&p), vec![KeyCode::Char('G')], "G waits for it");

    // The drain runs the Search (and whatever it spawns) with G still held. The
    // prompt stays up until the result's rows are in.
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
    assert_eq!(
        p.app.data_table_state.as_ref().unwrap().get_active_query(),
        "select name where age > 40"
    );
    assert_eq!(
        held(&p),
        vec![KeyCode::Char('G')],
        "G is offered only after"
    );

    settle(&mut p);
    assert_eq!(p.app.input_mode, InputMode::Normal);
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select name where age > 40");
    assert_eq!(state.num_rows(), 2);
    assert_eq!(
        state.table_state.selected(),
        Some(1),
        "G went to the last row of the result"
    );
}

/// Past the cap the newest key is dropped and the user told; the oldest, which may
/// be the `/` the rest were typed into, is never evicted.
#[test]
fn the_cap_drops_the_newest_key_and_says_so() {
    let mut p = pump();
    p.app.busy = true;
    p.app.status_message = Some("Loading buffer...".to_string());
    type_keys(&mut p, "/");
    type_keys(&mut p, &"a".repeat(39));
    assert_eq!(held(&p).len(), MAX_HELD_KEYS);
    assert_eq!(held(&p)[0], KeyCode::Char('/'));
    assert!(p.app.input_dropped);
    assert!(rendered(&mut p.app).contains("input dropped while busy"));

    p.app.busy = false;
    settle(&mut p);
    assert!(held(&p).is_empty());
    assert!(
        !p.app.input_dropped,
        "the notice goes with the last held key"
    );
}

/// A held navigation key repeats fast; the repeats become one press, so releasing
/// PageDown after a collect moves one page rather than thirty-two.
#[test]
fn a_held_navigation_key_is_one_press() {
    let mut p = pump();
    p.app.busy = true;
    for _ in 0..50 {
        p.terminal_key(plain(KeyCode::PageDown)).unwrap();
    }
    assert_eq!(held(&p), vec![KeyCode::PageDown]);
    for _ in 0..10 {
        p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    }
    assert_eq!(held(&p), vec![KeyCode::PageDown, KeyCode::Char('j')]);
    // Repeated text is not navigation and is kept.
    type_keys(&mut p, "aa");
    assert_eq!(held(&p).len(), 4);
}

/// Work that ends by opening a modal from a background result drops the keys typed
/// before it: a held key was not an answer to a message the user has not seen.
#[test]
fn keys_held_before_an_error_modal_appears_are_dropped() {
    let mut p = pump();
    p.app.busy = true;
    // A typed sequence (starts with `/`, so it is held, not dropped).
    type_keys(&mut p, "/x");
    assert_eq!(held(&p).len(), 2);

    let analysis = p.app.job_for_tests(
        crate::Job::Analysis(crate::jobs::AnalysisRun::default()),
        Some("Running analysis..."),
    );
    analysis.end(crate::Outcome::Failed {
        message: "disk on fire".to_string(),
        panicked: false,
    });
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
    assert!(!p.app.is_busy());
    assert!(held(&p).is_empty());
    settle(&mut p);
    assert!(p.app.error_modal.active, "the error is still on screen");
}

/// A statement that fails while running puts its reason under it in the prompt.
/// Keys typed while it ran were not answers to that, and are not typed into it.
#[cfg(feature = "sql")]
#[test]
fn keys_held_while_a_statement_runs_are_dropped_when_it_fails() {
    let (mut p, _dir) = loaded_pump();
    p.app.app_config.query.default_mode = crate::QueryMode::Sql;
    p.terminal_key(plain(KeyCode::Char(':'))).unwrap();
    let sql = "SELECT CAST(name AS INT) AS n FROM df";
    type_keys(&mut p, sql);
    // Its worker waits until the keys are typed, so it is still running then.
    let (waits, release) =
        crate::tests::worker_waits_once(|job| matches!(job, crate::Job::Rows(_)));
    p.app.jobs.worker_waits = waits;
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    // The statement starts running on the next pass over the channel.
    p.drain().unwrap();
    assert!(p.app.is_busy());
    type_keys(&mut p, "jj");
    assert_eq!(held(&p).len(), 2);

    release.send(()).unwrap();
    settle(&mut p);
    assert!(held(&p).is_empty());
    assert!(!p.app.error_modal.active);
    assert_eq!(p.app.query_prompt_mode(), Some(crate::QueryMode::Sql));
    assert_eq!(p.app.query_prompt_text(), Some(sql));
    assert!(p.app.query_prompt_error().is_some());
}

// --- Fixes from the high-effort review of #162 --------------------------------

/// Ctrl-C quits from a focused text field too (#649), as Ctrl-Q always has; the
/// field copies with Alt+W instead.
#[test]
fn ctrl_c_in_the_query_bar_quits() {
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char(':'))).unwrap();
    assert_eq!(p.app.input_mode, InputMode::Editing);
    assert!(matches!(
        p.app.handle(&AppEvent::Key(ctrl('c'))),
        Ok(Some(AppEvent::Exit))
    ));
    let alt_w = KeyEvent::new(KeyCode::Char('w'), KeyModifiers::ALT);
    assert!(!matches!(
        p.app.handle(&AppEvent::Key(alt_w)),
        Ok(Some(AppEvent::Exit))
    ));
    assert_eq!(
        p.app.input_mode,
        InputMode::Editing,
        "Alt+W stays in the field"
    );
}

/// Ctrl-C quits from every search box and modal (#649): the Sort tab's column
/// search, the chart view's open column Picker, and the plain table.
#[test]
fn ctrl_c_quits_from_the_sort_and_chart_search_boxes() {
    use crate::chart_modal::ChartFocus;
    use crate::sort_filter_modal::{SortFilterField, SortFilterTab};

    let (mut p, _dir) = loaded_pump();
    p.app.input_mode = InputMode::SortFilter;
    p.app.sort_filter_modal.active = true;
    p.app.sort_filter_modal.active_tab = SortFilterTab::Columns;
    p.app.sort_filter_modal.focus = SortFilterField::Find;
    assert!(
        p.app.text_field_focused(),
        "the sort search is a text field"
    );
    assert!(matches!(
        p.app.handle(&AppEvent::Key(ctrl('c'))),
        Ok(Some(AppEvent::Exit))
    ));

    let (mut p2, _d) = loaded_pump();
    p2.app.input_mode = InputMode::Chart;
    p2.app.chart_modal.active = true;
    p2.app.chart_modal.open(
        crate::chart_modal::ChartColumns {
            numeric: &["a".to_string(), "b".to_string()],
            ..Default::default()
        },
        None,
        None,
        false,
        0,
    );
    p2.app.chart_modal.focus = ChartFocus::X;
    p2.app.chart_modal.open_picker();
    assert!(
        p2.app.text_field_focused(),
        "the open Picker narrows by typing"
    );
    assert!(matches!(
        p2.app.handle(&AppEvent::Key(ctrl('c'))),
        Ok(Some(AppEvent::Exit))
    ));

    let (mut p3, _d3) = loaded_pump();
    assert!(matches!(
        p3.app.handle(&AppEvent::Key(ctrl('c'))),
        Ok(Some(AppEvent::Exit))
    ));
}

/// Item 2: a `q` at the startup spinner (busy, plain table view, nothing held) quits
/// at once; a typed `/query` is still held and typed.
#[test]
fn q_at_the_startup_spinner_quits_but_slash_query_types() {
    let mut p = pump();
    p.app.busy = true;
    assert!(p.app.in_normal_table_view());
    p.terminal_key(plain(KeyCode::Char('q'))).unwrap();
    assert!(
        matches!(p.drain().unwrap(), Drained::Exit),
        "q quits at once"
    );

    let mut p2 = pump();
    p2.app.busy = true;
    type_keys(&mut p2, "/query");
    assert_eq!(held(&p2).len(), 6, "the q in /query is held, not a quit");
}

/// Item 3: coalescing never eats a doubled letter in typed text, but does collapse a
/// held navigation key in the plain table view.
#[test]
fn doubled_letters_survive_but_navigation_coalesces() {
    let mut p = pump();
    p.app.busy = true;
    type_keys(&mut p, "/bookkeeper");
    let typed: String = held(&p)
        .iter()
        .filter_map(|c| match c {
            KeyCode::Char(ch) => Some(*ch),
            _ => None,
        })
        .collect();
    assert_eq!(typed, "/bookkeeper", "both k's, o's and e's survive");

    let mut p2 = pump();
    p2.app.busy = true;
    type_keys(&mut p2, "jj");
    assert_eq!(
        held(&p2),
        vec![KeyCode::Char('j')],
        "a doubled navigation key collapses"
    );
}

/// Item 4: a hard escape typed during the replay window (idle, keys still held) acts
/// at once rather than queueing behind the held keys; a non-escape waits.
#[test]
fn escapes_act_during_the_replay_window() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    type_keys(&mut p, "/foo");
    p.app.busy = false; // replay window: idle with keys still held

    assert!(p.terminal_key(ctrl('o')).unwrap(), "Ctrl-O acts now");
    assert_eq!(p.app.input_mode, InputMode::Home);
    assert!(held(&p).is_empty(), "going home cleared the held keys");

    let (mut p2, _d) = loaded_pump();
    p2.app.busy = true;
    type_keys(&mut p2, "/foo");
    p2.app.busy = false;
    assert!(
        !p2.terminal_key(plain(KeyCode::Char('x'))).unwrap(),
        "a fresh non-escape key waits behind the held ones"
    );
    assert_eq!(held(&p2).last().copied(), Some(KeyCode::Char('x')));
}

/// Item 5: a replayed Enter's Search runs before a key typed in the same moment. The
/// run loop drains the channel after a replay, so the fresh key finds the app busy
/// and waits; it then acts on the search's result.
///
/// The search's rows are held on their worker until G is typed (#568): a result
/// back within the drain would leave nothing running for G to wait on.
#[test]
fn a_replayed_search_runs_before_a_fresh_key() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    type_keys(&mut p, ":select name where age > 40");
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    p.app.busy = false;
    let (waits, release) =
        crate::tests::worker_waits_once(|job| matches!(job, crate::Job::Rows(_)));
    p.app.jobs.worker_waits = waits;

    // Replay one key, then drain, exactly as the run loop does before polling.
    loop {
        let replayed = p.replay_one().unwrap();
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
        if p.app.is_busy() {
            break;
        }
        assert!(replayed, "should still be replaying the query");
    }
    assert!(held(&p).is_empty(), "the whole query replayed");
    // The Search is running: a key typed now must wait for it.
    assert!(
        !p.terminal_key(plain(KeyCode::Char('G'))).unwrap(),
        "G waits behind the running search"
    );
    assert_eq!(held(&p), [KeyCode::Char('G')]);
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
    assert!(!p.replay_one().unwrap(), "G is not replayed while it runs");

    release.send(()).unwrap();
    settle(&mut p);
    assert_eq!(p.app.input_mode, InputMode::Normal);
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select name where age > 40");
    assert_eq!(state.num_rows(), 2);
    assert_eq!(
        state.table_state.selected(),
        Some(1),
        "G ran after the search, on its result"
    );
}

/// Item 6: a bare Enter or Esc at a busy table confirms nothing and is dropped; the
/// Enter that submits a typed query is held because `/` is queued ahead of it.
#[test]
fn a_bare_enter_or_esc_at_a_busy_table_is_dropped() {
    let mut p = pump();
    p.app.busy = true;
    assert!(!p.terminal_key(plain(KeyCode::Enter)).unwrap());
    assert!(!p.terminal_key(plain(KeyCode::Esc)).unwrap());
    assert!(held(&p).is_empty(), "neither is queued");

    let (mut p2, _d) = loaded_pump();
    p2.app.busy = true;
    type_keys(&mut p2, "/x");
    p2.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert_eq!(
        held(&p2).last().copied(),
        Some(KeyCode::Enter),
        "the query's Enter is held"
    );
}

/// `H` / `L` move a column and `+` / `-` filter on the cell: both change the
/// view, so at a busy table they are held, and replay in order once it is idle.
#[test]
fn column_moves_and_quick_filters_wait_while_busy() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    for c in ['L', '+', 'H', '-'] {
        assert!(!p.app.key_acts_while_busy(&plain(KeyCode::Char(c))), "{c}");
    }
    type_keys(&mut p, "L+");
    assert_eq!(held(&p), [KeyCode::Char('L'), KeyCode::Char('+')]);
    p.app.busy = false;
    settle(&mut p);
    rendered(&mut p.app);
    assert!(held(&p).is_empty());
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["age", "name"], "L moved name right");
    assert_eq!(
        state.current_column(),
        Some("name"),
        "the cursor went with it"
    );
    let filters = state.view_filters();
    assert_eq!(filters.len(), 1);
    assert_eq!(
        (filters[0].column.as_str(), filters[0].value.as_str()),
        ("name", "ada"),
        "+ filtered on the cell the cursor reached"
    );
}

/// Enter with nothing to drill into is Space at a busy table too: held, as Space,
/// and replayed into the inspector. Where it would drill, a bare Enter is dropped.
#[test]
fn enter_with_nothing_to_drill_into_waits_as_space() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('j'), KeyCode::Char(' ')]);
    p.app.busy = false;
    settle(&mut p);
    assert!(held(&p).is_empty());
    assert_eq!(p.app.input_mode, InputMode::Inspect);
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_eq!(state.table_state.selected(), Some(1), "j moved first");
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    assert_eq!(p.app.input_mode, InputMode::Normal);

    p.send(AppEvent::QQuery("select n: count age by name".to_string()))
        .unwrap();
    settle(&mut p);
    rendered(&mut p.app);
    assert!(p.app.data_table_state.as_ref().unwrap().can_drill_down());
    p.app.busy = true;
    assert!(!p.terminal_key(plain(KeyCode::Enter)).unwrap());
    assert!(held(&p).is_empty(), "the drilling Enter is dropped");
}

/// A table loaded with a List column is not a group: Enter typed ahead while busy
/// is held as Space, and replays into the inspector rather than a drill.
#[test]
fn enter_over_a_loaded_list_column_waits_as_space() {
    use polars::prelude::{IntoLazy, ParquetWriter, col, df};
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("lists.parquet");
    let mut frame = df!("k" => &[1i64, 1, 2], "v" => &[10i64, 11, 12])
        .unwrap()
        .lazy()
        .group_by([col("k")])
        .agg([col("v")])
        .collect()
        .unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut frame)
        .unwrap();

    let mut p = pump();
    p.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut p);
    rendered(&mut p.app);
    assert!(!p.app.data_table_state.as_ref().unwrap().can_drill_down());
    p.app.busy = true;
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert_eq!(held(&p), [KeyCode::Char('j'), KeyCode::Char(' ')]);
    p.app.busy = false;
    settle(&mut p);
    assert_eq!(p.app.input_mode, InputMode::Inspect);
    assert!(!p.app.data_table_state.as_ref().unwrap().is_drilled_down());
}

/// Item 7: the column cursor acts live at a busy table rather than queueing, and a
/// held column cursor key is kept, each one, as it would act idle: it reads
/// nothing, so a burst costs nothing, and a per-column key held behind it acts on
/// the column the cursor reached.
#[test]
fn column_cursor_acts_live_and_held_keys_all_replay() {
    let cursor = |p: &EventPump| {
        p.app
            .data_table_state
            .as_ref()
            .unwrap()
            .current_column_index()
            .unwrap()
    };
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    let before = cursor(&p);
    assert!(
        p.terminal_key(plain(KeyCode::Right)).unwrap(),
        "Right moves the cursor live"
    );
    assert!(held(&p).is_empty(), "it did not queue");
    assert_eq!(cursor(&p), before + 1);

    let (mut p2, _d) = wide_pump();
    p2.app.busy = true;
    // A non-actor navigation key is held first, so the keys behind it queue.
    p2.terminal_key(plain(KeyCode::Char('k'))).unwrap();
    p2.terminal_key(plain(KeyCode::Char('k'))).unwrap();
    for _ in 0..2 {
        p2.terminal_key(plain(KeyCode::Char('l'))).unwrap();
    }
    p2.terminal_key(plain(KeyCode::Char('h'))).unwrap();
    p2.terminal_key(plain(KeyCode::Char('l'))).unwrap();
    assert_eq!(
        held(&p2),
        vec![
            KeyCode::Char('k'),
            KeyCode::Char('l'),
            KeyCode::Char('l'),
            KeyCode::Char('h'),
            KeyCode::Char('l')
        ],
        "the held k's collapse; every column key stays"
    );
    let before = cursor(&p2);
    p2.app.busy = false;
    while p2.replay_one().unwrap() {}
    assert_eq!(cursor(&p2), before + 2, "replayed in order, as typed");
}

/// A pump with a CSV of sixty narrow columns loaded and drawn at 100 wide: three
/// pages sideways.
fn wide_pump() -> (EventPump, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("wide.csv");
    let mut file = std::fs::File::create(&path).expect("create csv");
    let names: Vec<String> = (0..60).map(|i| format!("c{i:02}")).collect();
    writeln!(file, "{}", names.join(",")).expect("write csv");
    for row in 0..5 {
        let values: Vec<String> = (0..60).map(|i| format!("{}", row * 100 + i)).collect();
        writeln!(file, "{}", values.join(",")).expect("write csv");
    }
    drop(file);
    let mut pump = pump();
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut pump);
    rendered(&mut pump.app);
    (pump, dir)
}

fn first_scrolled(pump: &EventPump) -> usize {
    pump.app.data_table_state.as_ref().unwrap().termcol_index
}

/// Column paging acts at a busy table at once, as the column cursor does; behind a
/// held key it waits, and it replays in order.
#[test]
fn column_paging_acts_live_and_replays_in_order() {
    let (mut p, _dir) = wide_pump();
    let shift_right = KeyEvent::new(KeyCode::Right, KeyModifiers::SHIFT);
    let shift_left = KeyEvent::new(KeyCode::Left, KeyModifiers::SHIFT);
    p.app.busy = true;
    assert!(p.terminal_key(shift_right).unwrap());
    assert!(held(&p).is_empty(), "Shift+→ did not queue");
    rendered(&mut p.app);
    let page = first_scrolled(&p);
    assert!(page > 1, "a page moves more than a column: {page}");
    assert!(p.terminal_key(plain(KeyCode::Char('}'))).unwrap());
    rendered(&mut p.app);
    let last_page = first_scrolled(&p);
    assert!(last_page > page && last_page < 59, "{page} {last_page}");
    assert!(p.terminal_key(plain(KeyCode::Char('{'))).unwrap());
    assert_eq!(first_scrolled(&p), 0);
    assert!(p.terminal_key(shift_right).unwrap());
    assert_eq!(first_scrolled(&p), page);
    assert!(p.terminal_key(shift_left).unwrap());
    assert_eq!(first_scrolled(&p), 0);

    // Behind a held key they wait, each one: they read nothing.
    p.terminal_key(plain(KeyCode::Char('k'))).unwrap();
    for _ in 0..2 {
        p.terminal_key(shift_right).unwrap();
    }
    p.terminal_key(plain(KeyCode::Char('}'))).unwrap();
    p.terminal_key(shift_left).unwrap();
    assert_eq!(
        held(&p),
        vec![
            KeyCode::Char('k'),
            KeyCode::Right,
            KeyCode::Right,
            KeyCode::Char('}'),
            KeyCode::Left,
        ]
    );
    assert_eq!(first_scrolled(&p), 0, "nothing acted yet");
    p.app.busy = false;
    settle(&mut p);
    assert!(held(&p).is_empty());
    rendered(&mut p.app);
    // Shift+→ then } reach the last page; Shift+← is the page before it.
    let before_last = first_scrolled(&p);
    assert!(
        before_last > 0 && before_last < last_page,
        "{before_last} before {last_page}"
    );
    // And the column picker, typed while busy, waits with what was typed into it.
    p.app.busy = true;
    for key in [
        KeyCode::Char('g'),
        KeyCode::Char('c'),
        KeyCode::Char('0'),
        KeyCode::Char('5'),
        KeyCode::Enter,
    ] {
        p.terminal_key(plain(key)).unwrap();
    }
    assert_eq!(held(&p).len(), 5, "g is not a view key: everything waits");
    p.app.busy = false;
    settle(&mut p);
    rendered(&mut p.app);
    assert_eq!(first_scrolled(&p), 5, "g c05 Enter shows c05 first");
    assert_eq!(p.app.input_mode, InputMode::Normal);
}

/// Item 8: F1 and `?` open help during a long load, at once, with nothing held.
#[test]
fn help_opens_during_a_load() {
    let (mut p, _dir) = loaded_pump();
    p.app.busy = true;
    assert!(p.terminal_key(plain(KeyCode::F(1))).unwrap());
    assert!(p.app.help_visible(), "F1 opened help immediately");
    assert!(held(&p).is_empty());

    let (mut p2, _d) = loaded_pump();
    p2.app.busy = true;
    assert!(p2.terminal_key(plain(KeyCode::Char('?'))).unwrap());
    assert!(p2.app.help_visible(), "? opened help immediately");
    assert!(held(&p2).is_empty());
}

/// Item 9: a modal a replayed key opens itself (an overwrite prompt) does not discard
/// the answer keys queued behind it, so held Enter, Left, Enter completes the export.
#[test]
fn a_prompt_a_replayed_key_opens_keeps_its_answer_keys() {
    let (mut p, dir) = loaded_pump();
    let path = dir.path().join("out.csv");
    std::fs::write(&path, "old").expect("seed an existing file");

    // Stage the export modal on an existing path, focused on the path field.
    p.app.export_modal.active = true;
    p.app.export_modal.selected_format = ExportFormat::Csv;
    p.app.export_modal.focus = ExportFocus::PathInput;
    p.app
        .export_modal
        .path_input
        .set_value(path.display().to_string());
    p.app.input_mode = InputMode::Export;

    // Keys typed while busy in Export mode are all held (not a plain table view).
    p.app.busy = true;
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    p.terminal_key(plain(KeyCode::Left)).unwrap();
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert_eq!(held(&p).len(), 3);
    p.app.busy = false;

    // The first replayed Enter opens the overwrite confirmation.
    assert!(p.replay_one().unwrap());
    assert!(
        p.app.confirmation_modal.active,
        "the overwrite prompt is up"
    );
    assert_eq!(
        held(&p),
        vec![KeyCode::Left, KeyCode::Enter],
        "the answer keys were not discarded by the prompt"
    );

    settle(&mut p);
    assert!(
        p.app
            .flash
            .as_ref()
            .is_some_and(|f| f.message.starts_with("Exported to ")),
        "the held Left+Enter answered the prompt and the export ran"
    );
    assert!(
        std::fs::read(&path).unwrap().len() > 3,
        "the file was overwritten with exported data"
    );

    // The next key clears the flash: the bar's line about the last action
    // is stale the moment another key does something.
    p.terminal_key(plain(KeyCode::Down)).unwrap();
    assert!(p.app.flash.is_none(), "a keypress clears the flash");
}

/// Going home mid-load drops the keys typed at the load: replayed into the home
/// screen they could open a dataset nobody asked for.
#[test]
fn ctrl_o_during_a_load_drops_the_held_keys() {
    let mut p = pump();
    p.app.busy = true;
    p.app.loading.first_rows_for_tests();
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert_eq!(held(&p).len(), 2);

    p.terminal_key(ctrl('o')).unwrap();
    assert_eq!(p.app.input_mode, InputMode::Home);
    assert!(!p.app.is_busy());
    assert!(held(&p).is_empty());
}

/// A Ctrl+O typed while the settings were read is offered once the startup open
/// has gone out and before anything it sends back, so that open is put down however
/// fast it would have been: nothing opens behind the home screen.
#[test]
fn ctrl_o_typed_before_the_app_existed_puts_the_startup_open_down() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("people.csv");
    std::fs::write(&path, "name,age\nada,36\n").expect("write csv");

    // As `run_impl` sets it up: the open announced, then the keys from the settings
    // read handed over with the open behind them.
    let mut p = pump();
    p.app.set_loading_phase("Scanning input", 10);
    p.handle_first([
        AppEvent::Terminal(Event::Key(ctrl('o'))),
        AppEvent::OpenNamed(vec![path], OpenOptions::default()),
    ]);
    settle(&mut p);
    // Anything that went out anyway is for nobody: let its answer land.
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    while p.app.background_work_in_flight() {
        assert!(std::time::Instant::now() < deadline, "the look never ended");
        p.wait_and_drain(Duration::from_millis(50)).unwrap();
    }
    settle(&mut p);

    assert_eq!(p.app.input_mode, InputMode::Home);
    assert!(
        p.app.data_table_state.is_none(),
        "nothing opened behind home"
    );
    assert!(!p.app.error_modal.active, "{}", p.app.error_modal.message);
}

/// The same for a frame handed over from Python: it was on the channel too, and
/// installed behind the home screen once Ctrl+O was offered first.
#[test]
fn ctrl_o_typed_before_the_app_existed_puts_a_startup_frame_down() {
    use polars::prelude::IntoLazy;
    let lf = polars::df!("a" => [1i64, 2, 3]).expect("frame").lazy();
    let mut p = pump();
    p.app.set_loading_phase("Scanning input", 10);
    p.handle_first([
        AppEvent::Terminal(Event::Key(ctrl('o'))),
        AppEvent::OpenLazyFrame(Box::new(lf), OpenOptions::default()),
    ]);
    settle(&mut p);
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    while p.app.background_work_in_flight() {
        assert!(std::time::Instant::now() < deadline, "the read never ended");
        p.wait_and_drain(Duration::from_millis(50)).unwrap();
    }
    settle(&mut p);

    assert_eq!(p.app.input_mode, InputMode::Home);
    assert!(
        p.app.data_table_state.is_none(),
        "nothing opened behind home"
    );
}

/// Keys typed before the app existed meet the startup open as the loading screen:
/// a stray `G` or Esc is dropped, not replayed onto the table once it is up, and
/// `q` still quits.
#[test]
fn keys_typed_before_the_app_existed_meet_the_loading_screen() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("people.csv");
    std::fs::write(&path, "name,age\nada,36\nbob,41\n").expect("write csv");

    let mut p = pump();
    p.app.set_loading_phase("Scanning input", 10);
    p.handle_first([
        AppEvent::Terminal(Event::Key(plain(KeyCode::Char('G')))),
        AppEvent::Terminal(Event::Key(plain(KeyCode::Esc))),
        AppEvent::OpenNamed(vec![path.clone()], OpenOptions::default()),
    ]);
    assert!(matches!(settle(&mut p), Drained::Continue { .. }));
    assert!(held(&p).is_empty());
    assert_eq!(p.app.input_mode, InputMode::Normal);
    let state = p.app.data_table_state.as_ref().expect("the table opened");
    assert_eq!(state.table_state.selected(), Some(0), "G was not replayed");

    let mut p = pump();
    p.app.set_loading_phase("Scanning input", 10);
    p.handle_first([
        AppEvent::Terminal(Event::Key(plain(KeyCode::Char('q')))),
        AppEvent::OpenNamed(vec![path], OpenOptions::default()),
    ]);
    assert!(matches!(settle(&mut p), Drained::Exit), "q quits");
}

/// The loading screen has nothing to type ahead into, so nothing is held
/// there. Held once, a stray key queued `q` behind it for the whole load.
#[test]
fn the_loading_screen_never_holds_keys() {
    let mut p = pump();
    p.app.source.opened_from_home = true;
    p.app.set_loading_phase("Scanning input", 10);
    assert!(p.app.awaiting_dataset() && p.app.is_busy());

    // A stray key is dropped, not held.
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    assert!(held(&p).is_empty(), "the loading screen holds nothing");

    // And q still acts at once, stray keys or not.
    p.terminal_key(plain(KeyCode::Char('q'))).unwrap();
    assert_eq!(
        p.app.input_mode,
        InputMode::Home,
        "q pops home from the loading screen"
    );
}

/// Ctrl-C and Ctrl-Q quit from any mode while busy, including the chart view,
/// which has no CONTROL arm of its own.
#[test]
fn ctrl_c_quits_from_chart_mode_while_busy() {
    for c in ['c', 'q'] {
        let mut p = pump();
        p.app.input_mode = InputMode::Chart;
        p.app.chart_modal.active = true;
        p.app.busy = true;
        assert!(matches!(
            p.app.handle(&AppEvent::Key(ctrl(c))),
            Ok(Some(AppEvent::Exit))
        ));
        p.terminal_key(ctrl(c)).unwrap();
        assert!(matches!(p.drain().unwrap(), Drained::Exit));
    }
}

/// The home screen is never busy on its own account, so it keeps its keys while
/// an export left running behind it finishes.
#[test]
fn home_keys_act_while_background_work_runs() {
    let mut p = pump();
    p.app.enter_home();
    p.app.busy = true;
    p.terminal_key(plain(KeyCode::Char('x'))).unwrap();
    assert!(held(&p).is_empty());
    assert_eq!(p.app.home.filter, "x");
}

/// A pump with `rows` rows loaded, named `r0000` on, so a row on screen can be
/// told from every other.
fn numbered_pump(rows: usize) -> (EventPump, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("numbered.csv");
    let mut file = std::fs::File::create(&path).expect("create csv");
    writeln!(file, "name").unwrap();
    for i in 0..rows {
        writeln!(file, "r{i:04}").unwrap();
    }
    drop(file);

    let mut pump = pump();
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut pump);
    // The frame sizes the table, and the collect it owes runs, as in `run()`.
    rendered(&mut pump.app);
    if let Some(state) = pump.app.data_table_state.as_mut()
        && std::mem::take(&mut state.needs_recollect)
    {
        pump.app.spawn_async_collect(App::LOADING_BUFFER);
    }
    settle(&mut pump);
    rendered(&mut pump.app);
    (pump, dir)
}

fn table(pump: &EventPump) -> &crate::table::DataTableState {
    pump.app.data_table_state.as_ref().expect("a dataset")
}

/// Page inside the buffer until the view is near enough its end to load ahead.
fn page_to_the_edge(pump: &mut EventPump) {
    for _ in 0..50 {
        if table(pump).wants_to_load_ahead() {
            return;
        }
        pump.terminal_key(plain(KeyCode::PageDown)).unwrap();
        assert!(
            !pump.app.is_busy(),
            "paging inside the buffer fetches nothing"
        );
        rendered(&mut pump.app);
    }
    panic!("the view never came near the end of the buffer");
}

/// Paging towards the end of the buffer grows it before the view gets there, and
/// nothing waits on that: no `busy`, no message, keys still live.
#[test]
fn paging_near_the_end_of_the_buffer_loads_ahead_quietly() {
    let (mut p, _dir) = numbered_pump(2000);
    page_to_the_edge(&mut p);
    let end = table(&p).buffered_end();

    p.app.request_what_the_frame_needs();
    assert!(
        p.app.rows_in_flight().is_some() && !p.app.rows_waited_on(),
        "a load-ahead went out"
    );
    assert!(!p.app.is_busy(), "and nothing waits on it");
    assert_eq!(p.app.status_message, None);

    settle(&mut p);
    assert!(
        table(&p).buffered_end() > end,
        "the buffer grew ahead of the view"
    );
    assert!(!p.app.is_busy());

    // Asked once for a position: the next frame does not ask again.
    let generation = p.app.task_generation();
    p.app.request_what_the_frame_needs();
    p.app.request_what_the_frame_needs();
    assert!(p.app.task_generation() - generation <= 1);
}

/// A frame drawn while the generation is held with the app idle does not spend the
/// position: the load-ahead it turns away goes out once the hold is let go (#490).
/// A job no longer holds the generation past its answer, but a download waiting
/// on the user still does.
#[test]
fn a_load_ahead_turned_away_by_a_hold_goes_out_once_it_is_let_go() {
    let (mut p, _dir) = numbered_pump(2000);
    page_to_the_edge(&mut p);
    let end = table(&p).buffered_end();

    // Something idle that holds the generation.
    let hold = p.app.hold_the_generation();
    let generation = p.app.task_generation();
    assert!(!p.app.is_busy());
    p.app.request_what_the_frame_needs();
    assert_eq!(
        p.app.task_generation(),
        generation,
        "nothing went out past it"
    );

    drop(hold);
    p.drain().unwrap();
    assert!(!p.app.work_a_bump_would_strand());
    p.app.request_what_the_frame_needs();
    assert!(
        p.app.rows_in_flight().is_some() && !p.app.rows_waited_on(),
        "the load-ahead went out once the generation was free"
    );
    settle(&mut p);
    assert!(table(&p).buffered_end() > end, "and the buffer grew");
}

/// PageDown held while a load-ahead is out waits on it rather than fetching the same
/// rows again, and the repeats become one press: one fetch, nothing piled up.
#[test]
fn holding_pagedown_during_a_load_ahead_waits_on_it() {
    let (mut p, _dir) = numbered_pump(2000);
    page_to_the_edge(&mut p);
    let state = table(&p);
    let (start, end, page) = (
        state.buffered_start(),
        state.buffered_end(),
        state.visible_rows,
    );
    let dataset = state.len_generation();
    let columns = crate::InflightCollect::columns_of(state);
    // Out, and bringing the next few pages: no thread, so it stays out.
    let generation = p.app.task_generation();
    let _ahead = p.app.job_for_tests(
        crate::Job::Rows(crate::InflightCollect {
            began: std::time::Instant::now(),
            files: None,
            dataset,
            columns,
            start,
            end: end + 10 * page,
        }),
        None,
    );

    // As `run()` takes them: a key, then whatever it set going.
    for _ in 0..30 {
        p.terminal_key(plain(KeyCode::PageDown)).unwrap();
        p.drain().unwrap();
    }
    assert_eq!(
        p.app.task_generation(),
        generation,
        "no second fetch went out"
    );
    assert!(
        p.app.rows_waited_on(),
        "the page that left the buffer waits on the load-ahead"
    );
    assert!(p.app.is_busy());
    assert!(held(&p).len() <= 1, "held repeats coalesce: {:?}", held(&p));
}

/// A page whose rows are still coming draws the last page that was whole, not a
/// page of blanks under the new position.
#[test]
fn a_page_still_loading_draws_the_last_whole_one() {
    let (mut p, _dir) = numbered_pump(2000);
    assert!(rendered(&mut p.app).contains("r0000"));
    let state = p.app.data_table_state.as_mut().unwrap();
    assert!(
        state.slide_table(1500),
        "far past the buffer, so a fetch is owed"
    );

    let screen = rendered(&mut p.app);
    assert!(screen.contains("r0000"), "the page before stays up");
}

/// A fetch goes unmentioned while it is young, so paging does not blink a sentence
/// over the key chips; one that takes a while says what it is doing.
#[test]
fn the_bar_says_loading_only_once_a_fetch_takes_a_while() {
    let (mut p, _dir) = numbered_pump(200);
    let dataset = table(&p).len_generation();
    let columns = crate::InflightCollect::columns_of(table(&p));
    let _read = p.app.job_for_tests(
        crate::Job::Rows(crate::InflightCollect {
            began: std::time::Instant::now(),
            files: None,
            dataset,
            columns,
            start: 0,
            end: 200,
        }),
        Some(App::LOADING_BUFFER),
    );
    assert!(!rendered(&mut p.app).contains("Loading buffer"));

    if let Some(crate::Job::Rows(inflight)) = p
        .app
        .jobs
        .current_mut(|job| matches!(job, crate::Job::Rows(_)))
    {
        inflight.began -= Duration::from_secs(1);
    }
    assert!(rendered(&mut p.app).contains("Loading buffer"));
}

fn terminal(key: KeyEvent) -> AppEvent {
    AppEvent::Terminal(Event::Key(key))
}

/// A flash that outlives any test: the loop's sleep is bounded by it, so a loop that
/// only woke at deadlines would end the test with the flash gone instead of hanging.
fn long_flash(app: &mut App) {
    app.flash = Some(crate::Flash {
        message: "guard".to_string(),
        path_from: None,
        expires: Instant::now() + Duration::from_secs(120),
    });
}

/// Idle, the loop sleeps without a deadline; a spinner wakes it each frame.
#[test]
fn the_pacer_sleeps_until_a_deadline_and_has_none_when_idle() {
    let start = Instant::now();
    let mut pacer = Pacer::default();
    pacer.drew(start);
    assert_eq!(pacer.timeout(None, start), Duration::MAX, "idle: no tick");

    pacer.spinning(true, true, start);
    assert_eq!(pacer.timeout(None, start), SPINNER_FRAME);
    assert!(!pacer.turn_spinner(start + SPINNER_FRAME / 2), "not yet");
    assert!(pacer.turn_spinner(start + SPINNER_FRAME), "a frame on time");
    assert!(!pacer.turn_spinner(start + SPINNER_FRAME), "once per frame");

    pacer.spinning(false, true, start);
    let flash = start + Duration::from_secs(2);
    assert_eq!(pacer.timeout(Some(flash), start), Duration::from_secs(2));
}

/// Progress reports close behind a frame wait for the next one; anything else, and
/// progress once a frame's time has passed, is drawn at once.
#[test]
fn progress_reports_share_a_frame_and_nothing_else_waits() {
    let start = Instant::now();
    let mut pacer = Pacer::default();
    pacer.drew(start);
    let soon = start + PROGRESS_FRAME / 3;
    assert!(
        pacer.handled(true, false, soon),
        "a result is drawn at once"
    );
    assert!(!pacer.handled(true, true, soon), "progress waits");
    assert_eq!(
        pacer.timeout(None, soon),
        PROGRESS_FRAME - PROGRESS_FRAME / 3,
        "until the owed frame"
    );
    assert!(
        !pacer.handled(false, false, soon),
        "nothing new, still owed"
    );
    assert!(
        pacer.handled(false, false, start + PROGRESS_FRAME),
        "then drawn"
    );
    pacer.drew(start + PROGRESS_FRAME);
    assert!(!pacer.handled(false, false, start + 2 * PROGRESS_FRAME));
    assert!(
        pacer.handled(true, true, start + 3 * PROGRESS_FRAME),
        "progress long after a frame is drawn at once"
    );
}

/// Keys come through the channel now. They are classified on the way in exactly as
/// before: typed at a busy table they are held in order, and Ctrl-Q still jumps the
/// queue and quits.
#[test]
fn terminal_keys_on_the_channel_are_held_in_order_and_ctrl_q_jumps_them() {
    let mut p = pump();
    p.app.busy = true;
    for c in ['/', 'a', 'b'] {
        p.send(terminal(plain(KeyCode::Char(c)))).unwrap();
    }
    assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
    assert_eq!(
        held(&p),
        vec![KeyCode::Char('/'), KeyCode::Char('a'), KeyCode::Char('b')]
    );
    p.send(terminal(ctrl('q'))).unwrap();
    // The key acts at once; the exit it asks for is its continuation, the very next
    // thing handled.
    assert!(matches!(
        p.drain().unwrap(),
        Drained::Continue { updated: true, .. }
    ));
    assert!(matches!(p.drain().unwrap(), Drained::Exit));
}

/// A worker's answer that lands behind keys typed ahead is handled before the next
/// of them, as when the loop read the terminal itself. Taken in channel order, a
/// held-down key put the load-ahead's answer behind every repeat.
#[test]
fn an_answer_is_not_held_behind_keys_typed_ahead() {
    let mut p = pump();
    p.app.home.listing_in_flight = true;
    for _ in 0..3 {
        p.send(terminal(plain(KeyCode::Down))).unwrap();
    }
    p.send(AppEvent::HomeListingFailed).unwrap();
    let keys = p.app.debug.num_key_events;
    assert!(matches!(
        p.drain().unwrap(),
        Drained::Continue { updated: true, .. }
    ));
    assert!(!p.app.home.listing_in_flight, "the answer is in");
    assert_eq!(
        p.app.debug.num_key_events,
        keys + 1,
        "and one key, one frame"
    );
    p.drain().unwrap();
    p.drain().unwrap();
    assert_eq!(p.app.debug.num_key_events, keys + 3, "the rest, in turn");
}

/// A worker that reports faster than the loop handles it still lets a typed key
/// through.
#[test]
fn a_stream_of_reports_does_not_starve_a_typed_key() {
    let mut p = pump();
    p.send(terminal(plain(KeyCode::Down))).unwrap();
    for _ in 0..RESULTS_PER_KEY * 4 {
        p.send(AppEvent::Wake).unwrap();
    }
    let keys = p.app.debug.num_key_events;
    p.drain().unwrap();
    assert_eq!(p.app.debug.num_key_events, keys + 1);
}

/// A resize read from the terminal reaches the app as the resize event it always
/// was.
#[test]
fn a_resize_from_the_terminal_reaches_the_app() {
    let mut p = pump();
    p.send(AppEvent::Terminal(Event::Resize(90, 30))).unwrap();
    let events = p.app.debug.num_events;
    assert!(matches!(
        p.drain().unwrap(),
        Drained::Continue { updated: true, .. }
    ));
    assert_eq!(p.app.debug.num_events, events + 1, "handled once");
}

/// The loop sleeps until something arrives: a worker's answer wakes it at once,
/// with no deadline to wait out. The guard flash would end the sleep after two
/// minutes and clear itself; it is still there, so the event did it.
#[test]
fn a_worker_answer_wakes_the_sleeping_loop() {
    let mut p = pump();
    long_flash(&mut p.app);
    let tx = p.tx.clone();
    let mut frames = 0;
    let end = p
        .run(|_app| {
            frames += 1;
            if frames == 1 {
                // After the first frame the loop goes to sleep; the answer lands
                // while it does.
                let tx = tx.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(20));
                    let _ = tx.send(AppEvent::Exit);
                });
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(end, Ended::Quit);
    assert!(p.app.flash.is_some(), "woken by the event, not a deadline");
}

/// A frame that draws rows nothing has measured asks for them before the loop
/// sleeps. Asked on the next pass instead, they would wait for a key: with the
/// listing in and nothing turning, there is no next pass.
#[test]
fn rows_a_frame_draws_unmeasured_are_asked_for_before_the_loop_sleeps() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("people.csv");
    std::fs::write(&path, "name,age\nada,36\n").unwrap();
    let mut p = pump();
    long_flash(&mut p.app);
    p.app.enter_home();
    p.app.home_jump_into(dir.path().to_path_buf());
    let tx = p.tx.clone();
    let mut asked = false;
    let end = p
        .run(|app| {
            rendered(app);
            if !asked && app.home.enriched.contains_key(&path) {
                asked = true;
                let _ = tx.send(AppEvent::Exit);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(end, Ended::Quit);
    assert!(p.app.home.enriched.contains_key(&path), "measured");
    assert!(p.app.flash.is_some(), "without waiting for a deadline");
}

/// Each loading phase gets its frame, then the next phase runs: the open goes from
/// the spinner to rows through the loop itself, every phase drawn in order.
#[test]
fn an_open_draws_each_phase_and_then_the_rows() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("people.csv");
    std::fs::write(&path, "name,age\nada,36\ngrace,45\n").unwrap();
    let mut p = pump();
    p.app.set_loading_phase("Scanning input", 10);
    p.app.busy = true;
    p.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    let tx = p.tx.clone();
    let mut screens: Vec<String> = Vec::new();
    let end = p
        .run(|app| {
            let screen = rendered(app);
            if screen.contains("grace") && !screens.last().is_some_and(|s| s == &screen) {
                let _ = tx.send(AppEvent::Exit);
            }
            screens.push(screen);
            Ok(())
        })
        .unwrap();
    assert_eq!(end, Ended::Quit);
    let first_with = |needle: &str| screens.iter().position(|s| s.contains(needle));
    let scanning = first_with("Scanning input").expect("the first phase is drawn");
    let rows = first_with("grace").expect("the rows are drawn");
    assert!(scanning < rows, "the phase is drawn before the rows");
}

/// A pump with a wide, long CSV loaded: columns past the screen and rows past a
/// page, so the keys that move have somewhere to go.
/// A pump with a wide, long CSV loaded: columns past the screen, rows past a page
/// and numbers that group, so the keys that move and format have somewhere to go.
fn long_wide_pump() -> (EventPump, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("wide.csv");
    let mut file = std::fs::File::create(&path).expect("create csv");
    let names: Vec<String> = (0..12).map(|c| format!("column_{c}")).collect();
    writeln!(file, "name,{}", names.join(",")).unwrap();
    for row in 0..300 {
        let values: Vec<String> = (0..12)
            .map(|c| (((row * 7 + c) % 23) * 1234).to_string())
            .collect();
        writeln!(file, "n{},{}", row % 17, values.join(",")).unwrap();
    }
    drop(file);
    let mut pump = pump();
    pump.app.app_config.query.default_mode = crate::QueryMode::Q;
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    settle(&mut pump);
    paint(&mut pump);
    (pump, dir)
}

/// The whole frame, styles included: the column cursor is a tint.
fn frame(app: &mut App) -> Buffer {
    let area = Rect::new(0, 0, 100, 20);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    buf
}

/// Draw a frame and read what it asks for, as `run()` does after each update, until
/// nothing more is asked: the rows a page down needs.
fn paint(pump: &mut EventPump) {
    for _ in 0..200 {
        frame(&mut pump.app);
        pump.app.frame_painted();
        pump.app.request_what_the_frame_needs();
        let collect = pump
            .app
            .data_table_state
            .as_mut()
            .is_some_and(|s| std::mem::take(&mut s.needs_recollect));
        if collect {
            pump.app.spawn_async_collect(App::LOADING_BUFFER);
        }
        settle(pump);
        if !collect {
            return;
        }
    }
}

/// Every key the registry lists for a screen reached from the table, typed at that
/// screen, is taken: the frame changes, the screen changes, or the app ends. A key
/// listed where nothing handles it fails here. The groups checked are the ones the
/// screen opens on; a group of a state within it (a picker, a dialog) and the
/// listed exceptions need a state this test does not set up.
#[test]
fn every_registry_key_is_taken_where_it_is_listed() {
    use datui_cli::keys::{self, Context};
    let ch = |c: char| plain(KeyCode::Char(c));
    let shift = |c: char| KeyEvent::new(KeyCode::Char(c), KeyModifiers::SHIFT);
    let (up, down, right) = (
        plain(KeyCode::Up),
        plain(KeyCode::Down),
        plain(KeyCode::Right),
    );
    // How each screen is reached from the table, and the groups it opens on.
    let screens: Vec<(Context, Vec<KeyEvent>, &[&str])> = vec![
        (
            Context::Table,
            vec![],
            &["Explore", "Shape", "Analyze", "Output", "Display", "Go"],
        ),
        // Something typed, for the editing keys to edit.
        (Context::Query, vec![ch(':'), ch('a')], &["Run", "Edit"]),
        (Context::Find, vec![ch('/'), ch('7')], &["Find"]),
        (Context::GoToColumn, vec![ch('g'), ch('c')], &["Go"]),
        (Context::Inspector, vec![ch(' ')], &["Fields", "Output"]),
        (Context::Info, vec![ch('i')], &["Panel"]),
        (
            Context::ValueCounts,
            vec![shift('F')],
            &["Explore", "Output"],
        ),
        // What is in effect, where Sort & Filter opens; then the Columns tab's
        // list: up to the tab bar, across, and down past find.
        (
            Context::SortFilter,
            vec![ch('s')],
            &["Sidebar", "In effect"],
        ),
        (
            Context::SortFilter,
            vec![ch('s'), up, right, down, down],
            &["Columns"],
        ),
        // On the tab bar, a choice, for ← / → to step.
        (Context::PivotMelt, vec![ch('p')], &["Form"]),
        // From a number column: its histogram, which has a grid and bins.
        (Context::Chart, vec![ch('l'), ch('c')], &["Shelves", "Plot"]),
        // Back from the path to the format, a choice, for ← / → to step.
        (
            Context::Export,
            vec![ch('e'), plain(KeyCode::BackTab)],
            &["Form"],
        ),
        (Context::Copy, vec![ch('y')], &["Form"]),
        (Context::Views, vec![ch('v')], &["List"]),
        // Not the home screen: what it lists, and so what a key there changes, is
        // the machine's (the working directory, the desktop's recent places).
        // home_test covers its keys.
    ];
    // What needs a state the fixture does not have, or would leave the test.
    let mut exempt: Vec<(Context, &str)> = vec![
        // Only where the row count is an estimate.
        (Context::Info, "c"),
        // Only on a file read through a format spec.
        (Context::Table, "b"),
        // Nothing to leave at the plain table.
        (Context::Table, "Esc"),
        // Every column fits already.
        (Context::Table, "= / w"),
        // Nothing to reset, or to take out of a sort.
        (Context::Table, "R"),
        (Context::SortFilter, "Del"),
        (Context::SortFilter, "[ / ]"),
        // The width is automatic already.
        (Context::SortFilter, "w"),
        // History: none in a fresh session.
        (Context::Query, "↑ / ↓"),
        (Context::Find, "↑ / ↓"),
        // On a group's row, a struct or JSON, or a field the rows lack.
        (Context::Inspector, "Enter"),
        (Context::Inspector, "r"),
        // Long text only; another program.
        (Context::Inspector, "w"),
        (Context::Inspector, "o"),
        // The scrolling tabs: a CSV has none; Notes: none.
        (Context::Info, "PgUp / PgDn"),
        (Context::Info, "Home / End"),
        (Context::Info, "Enter"),
        // The Documentation tab: a dataset no catalog lists has none.
        (Context::Info, "y"),
        (Context::Info, "o"),
        // A sample, a followed file, a number column.
        (Context::ValueCounts, "a"),
        (Context::ValueCounts, "c"),
        (Context::ValueCounts, "t"),
        // Nothing in effect: the sidebar opens on "add sort", which has no value
        // to step, and there is nothing to remove or clear.
        (Context::SortFilter, "← / →"),
        (Context::SortFilter, "d / Del"),
        (Context::SortFilter, "C"),
        // History: none in a fresh session.
        (Context::Export, "Ctrl+P / Ctrl+N"),
        // A range: Enter in the help presses its first.
        (Context::SortFilter, "1-9"),
        (Context::Chart, "1-7"),
        // The number rows; columns picked; a followed file.
        (Context::Chart, "+ / -"),
        (Context::Chart, "x"),
        (Context::Chart, "e"),
        (Context::Chart, "t"),
        // A saved view.
        (Context::Views, "↑ / ↓ (j/k)"),
        (Context::Views, "Enter"),
        (Context::Views, "e"),
        (Context::Views, "d"),
        (Context::Views, "i"),
    ];
    // One language to switch between without SQL.
    if crate::QueryMode::available().len() < 2 {
        exempt.push((Context::Query, "Ctrl+T"));
    }
    let mut ignored = Vec::new();
    for (context, open, groups) in &screens {
        let screen = keys::screen(*context);
        for group in screen.groups.iter().filter(|g| groups.contains(&g.name)) {
            for key in group.keys {
                let Some(chord) = key.action() else {
                    continue;
                };
                if exempt.contains(&(*context, key.keys)) {
                    continue;
                }
                let (mut p, dir) = long_wide_pump();
                for k in open {
                    p.terminal_key(*k).unwrap();
                    settle(&mut p);
                    paint(&mut p);
                }
                assert_eq!(p.app.keys_context(), *context, "opened {}", screen.title);
                // The suggested name is relative: an Enter would write it into the
                // crate directory.
                if *context == Context::Export {
                    let out = dir.path().join("out.csv");
                    p.app
                        .export_modal
                        .path_input
                        .set_value(out.display().to_string());
                }
                // The entry's keys in turn, the first at least: `← / →` at the
                // first column is taken by its →.
                let mut presses = keys::chords(key.keys);
                if !presses.contains(&chord) {
                    presses.insert(0, chord);
                }
                let mut taken = false;
                for press in presses {
                    let before = frame(&mut p.app);
                    p.terminal_key(crate::help::key_event(press)).unwrap();
                    if matches!(settle(&mut p), Drained::Exit) {
                        taken = true;
                        break;
                    }
                    paint(&mut p);
                    if before != frame(&mut p.app) || p.app.keys_context() != *context {
                        taken = true;
                        break;
                    }
                }
                if !taken {
                    ignored.push(format!("{} · {} · {}", screen.title, group.name, key.keys));
                }
            }
        }
    }
    assert!(
        ignored.is_empty(),
        "keys nothing took:\n{}",
        ignored.join("\n")
    );
}

/// Enter on a help line closes the help and presses the line's key at the screen
/// under it, through the loop as a typed key goes.
#[test]
fn enter_in_the_help_runs_the_key() {
    let (mut p, _dir) = long_wide_pump();
    p.terminal_key(plain(KeyCode::Char('?'))).unwrap();
    settle(&mut p);
    assert!(p.app.help_visible());
    for c in "/value counts".chars() {
        p.terminal_key(plain(KeyCode::Char(c))).unwrap();
    }
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    settle(&mut p);
    assert!(!p.app.help_visible(), "Enter closed the help");
    assert_eq!(
        p.app.input_mode,
        InputMode::ValueCounts,
        "F ran at the table"
    );
}

/// While the app is busy, the key Enter presses waits its turn like a typed one.
#[test]
fn the_key_enter_presses_waits_while_busy() {
    let (mut p, _dir) = long_wide_pump();
    p.terminal_key(plain(KeyCode::Char('?'))).unwrap();
    for c in "/sort & filter sidebar".chars() {
        p.terminal_key(plain(KeyCode::Char(c))).unwrap();
    }
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    p.app.busy = true;
    p.drain().unwrap();
    assert!(!p.app.help_visible());
    assert_eq!(p.app.input_mode, InputMode::Normal, "held while busy");
    assert_eq!(held(&p), [KeyCode::Char('s')]);
    p.app.busy = false;
    settle(&mut p);
    assert_eq!(
        p.app.input_mode,
        InputMode::SortFilter,
        "replayed once idle"
    );
}

/// Type `text` into the open help's filter, then press Enter.
fn run_from_help(p: &mut EventPump, filter: &str) {
    p.terminal_key(plain(KeyCode::Char('/'))).unwrap();
    for c in filter.chars() {
        p.terminal_key(plain(KeyCode::Char(c))).unwrap();
    }
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
}

/// Over a text field, a help line whose key is a plain character does not run:
/// pressed, it would type. The help's own key presses F1, never `?`.
#[test]
fn enter_in_the_help_never_types_into_a_field() {
    let (mut p, _dir) = long_wide_pump();
    p.terminal_key(plain(KeyCode::Char('f'))).unwrap();
    p.terminal_key(plain(KeyCode::F(1))).unwrap();
    run_from_help(&mut p, "next or previous");
    settle(&mut p);
    assert!(
        p.app.help_visible(),
        "n / N does not run from the find prompt"
    );
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    assert!(!p.app.help_visible());
    assert_eq!(p.app.prompt.find.input.value(), "", "nothing was typed");

    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    p.terminal_key(plain(KeyCode::Char(':'))).unwrap();
    p.terminal_key(plain(KeyCode::F(1))).unwrap();
    run_from_help(&mut p, "screen's keys");
    settle(&mut p);
    assert_eq!(p.app.prompt.query_input.value(), "", "? was not typed");
    assert!(p.app.help_visible(), "F1 opened the help again");
}

/// A question that arrives under the help takes the keys, so the help goes: Enter
/// never answers it unseen. Nor does F1 open the help over one.
#[test]
fn a_question_under_the_help_closes_it() {
    let (mut p, _dir) = long_wide_pump();
    p.terminal_key(plain(KeyCode::Char('?'))).unwrap();
    assert!(p.app.help_visible());
    p.app.confirmation_modal.active = true;
    rendered(&mut p.app);
    assert!(!p.app.help_visible());
    p.terminal_key(plain(KeyCode::F(1))).unwrap();
    assert!(!p.app.help_visible(), "no help over the question");
}

/// While busy, the help still closes at once, at a load's screen too.
#[test]
fn the_help_closes_while_busy() {
    for key in [KeyCode::Esc, KeyCode::F(1), KeyCode::Char('?')] {
        let (mut p, _dir) = long_wide_pump();
        p.terminal_key(plain(KeyCode::Char('?'))).unwrap();
        p.app.busy = true;
        assert!(p.terminal_key(plain(key)).unwrap(), "{key:?} acted");
        assert!(!p.app.help_visible(), "{key:?} closed the help");
        assert!(held(&p).is_empty());
    }
}

/// The key Enter presses goes through `classify` as a typed one: a busy Enter
/// that would inspect waits as Space.
#[test]
fn the_pressed_key_is_classified_as_typed() {
    let (mut p, _dir) = long_wide_pump();
    p.terminal_key(plain(KeyCode::Char('?'))).unwrap();
    run_from_help(&mut p, "or inspect");
    p.app.busy = true;
    p.drain().unwrap();
    assert!(!p.app.help_visible());
    assert_eq!(held(&p), [KeyCode::Char(' ')]);
}

/// A screen that changes under the help on its own takes the help with it: its
/// keys were for the screen that is gone.
#[test]
fn help_closes_when_its_screen_goes() {
    let (mut p, _dir) = long_wide_pump();
    p.terminal_key(plain(KeyCode::Char('?'))).unwrap();
    assert!(p.app.help_visible());
    // As a finished query or a failed load moves the app on.
    p.app.input_mode = InputMode::Home;
    rendered(&mut p.app);
    assert!(!p.app.help_visible());
}

/// Help opened over the analysis view or the views list shows that screen's keys.
#[test]
fn help_shows_the_keys_of_the_screen_under_it() {
    let (mut p, _dir) = long_wide_pump();
    p.terminal_key(plain(KeyCode::Char('v'))).unwrap();
    settle(&mut p);
    p.terminal_key(plain(KeyCode::Char('?'))).unwrap();
    assert_eq!(p.app.help_context(), Some(datui_cli::keys::Context::Views));
}

// ----- The mouse on forms, tabs, the footer, the header and the menu -----

fn right_click(at: (u16, u16)) -> MouseEvent {
    use crossterm::event::{MouseButton, MouseEventKind};
    mouse(MouseEventKind::Down(MouseButton::Right), at)
}

fn drag(at: (u16, u16)) -> MouseEvent {
    use crossterm::event::{MouseButton, MouseEventKind};
    mouse(MouseEventKind::Drag(MouseButton::Left), at)
}

fn release(at: (u16, u16)) -> MouseEvent {
    use crossterm::event::{MouseButton, MouseEventKind};
    mouse(MouseEventKind::Up(MouseButton::Left), at)
}

/// Where the table's header drew `column`: its cells across, `[from, to)`, and the
/// header's first row.
fn header_of(p: &mut EventPump, column: &str) -> (u16, u16, u16) {
    rendered(&mut p.app);
    let state = p.app.data_table_state.as_ref().unwrap();
    let (area, columns) = state.drawn_header().expect("the table was drawn");
    let (from, to, _) = columns
        .into_iter()
        .find(|(_, _, name)| name == column)
        .expect("the column is drawn");
    (from, to, area.y)
}

fn order(p: &EventPump) -> Vec<String> {
    p.app.data_table_state.as_ref().unwrap().headers()
}

/// In the export dialog a click on a row focuses it and acts as Space: a
/// checkbox flips, a choice steps, a text field only takes the cursor. A click on
/// a format on its row chooses it.
#[test]
fn a_click_focuses_a_form_row_and_acts_on_it() {
    use crate::export_modal::{ExportFocus, ExportFormat};
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('e'))).unwrap();
    assert_eq!(p.app.input_mode, InputMode::Export);
    assert_eq!(p.app.export_modal.selected_format, ExportFormat::Csv);
    let header = p.app.export_modal.csv_include_header;

    let at = on_screen(&mut p.app, "Header:");
    assert!(p.terminal_mouse(click(at)).unwrap());
    assert_eq!(p.app.export_modal.focus, ExportFocus::CsvIncludeHeader);
    assert_eq!(p.app.export_modal.csv_include_header, !header, "toggled");

    let compression = p.app.export_modal.csv_compression;
    let at = on_screen(&mut p.app, "Compression:");
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(p.app.export_modal.focus, ExportFocus::Compression);
    assert_ne!(p.app.export_modal.csv_compression, compression, "stepped");
    // A right click steps it back.
    p.terminal_mouse(right_click(at)).unwrap();
    assert_eq!(p.app.export_modal.csv_compression, compression);

    let path = p.app.export_modal.path_input.value().to_string();
    let at = on_screen(&mut p.app, "Path:");
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(p.app.export_modal.focus, ExportFocus::PathInput);
    assert_eq!(p.app.export_modal.path_input.value(), path, "nothing typed");

    let at = on_screen(&mut p.app, "Parquet");
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(p.app.export_modal.selected_format, ExportFormat::Parquet);
    assert_eq!(p.app.input_mode, InputMode::Export, "still open");
}

/// A click on a tab switches to it: the Sort & Filter sidebar's, through its tab
/// bar field, and the Info panel's, which switch from anywhere.
#[test]
fn a_click_on_a_tab_switches_to_it() {
    use crate::sort_filter_modal::{SortFilterField, SortFilterTab};
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('s'))).unwrap();
    assert_eq!(p.app.input_mode, InputMode::SortFilter);
    let at = on_screen(&mut p.app, "Columns");
    assert!(p.terminal_mouse(click(at)).unwrap());
    assert_eq!(p.app.sort_filter_modal.active_tab, SortFilterTab::Columns);
    assert_eq!(p.app.sort_filter_modal.focus, SortFilterField::TabBar);
    // The tab, not the sidebar's title.
    let tab = format!("Sort & Filter {}", crate::glyphs::get().rule);
    let at = on_screen(&mut p.app, &tab);
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(p.app.sort_filter_modal.active_tab, SortFilterTab::InEffect);

    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    p.terminal_key(plain(KeyCode::Char('i'))).unwrap();
    assert_eq!(p.app.input_mode, InputMode::Info);
    let at = on_screen(&mut p.app, "Resources");
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(
        p.app.info_modal.active_tab,
        crate::widgets::info::InfoTab::Resources
    );
}

/// The footer's filters open the sidebar that lists them, and its `query`
/// opens the command line on the query's text.
#[test]
fn the_footer_filters_and_query_are_clickable() {
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('+'))).unwrap();
    settle(&mut p);
    let at = on_screen(&mut p.app, "name = ");
    assert!(p.terminal_mouse(click(at)).unwrap());
    assert_eq!(
        p.app.input_mode,
        InputMode::SortFilter,
        "the sidebar opened"
    );
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    settle(&mut p);

    p.terminal_key(plain(KeyCode::Char(':'))).unwrap();
    type_keys(&mut p, "select age");
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    settle(&mut p);
    assert_eq!(p.app.input_mode, InputMode::Normal);
    let at = on_screen(&mut p.app, " query ");
    assert!(p.terminal_mouse(click((at.0 + 1, at.1))).unwrap());
    assert_eq!(p.app.input_mode, InputMode::Editing);
    assert_eq!(p.app.prompt.query_input.value(), "select age");
}

/// A click outside a dialog does nothing: the table under the sidebar keeps its
/// cursor and the sidebar stays.
#[test]
fn a_click_outside_a_dialog_does_nothing() {
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    p.terminal_key(plain(KeyCode::Char('s'))).unwrap();
    rendered(&mut p.app);
    assert!(!p.terminal_mouse(click(alan)).unwrap());
    assert!(!p.terminal_mouse(right_click(alan)).unwrap());
    assert_eq!(cell(&p), (Some(0), Some("name".to_string())));
    assert_eq!(p.app.input_mode, InputMode::SortFilter);
    assert!(p.app.context_menu.is_none());

    // Over a dialog the table is covered, and so is anything else drawn under it.
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    p.terminal_key(plain(KeyCode::Char('e'))).unwrap();
    rendered(&mut p.app);
    let focus = p.app.export_modal.focus;
    let header = p.app.export_modal.csv_include_header;
    assert!(!p.terminal_mouse(click((0, 0))).unwrap());
    assert!(!p.terminal_mouse(click(alan)).unwrap());
    assert_eq!(p.app.export_modal.focus, focus);
    assert_eq!(p.app.export_modal.csv_include_header, header);
    assert_eq!(p.app.input_mode, InputMode::Export);
}

/// A header dragged over another column moves there on release, as `L` would,
/// with a rule on the header where it lands while it is carried.
#[test]
fn dragging_a_header_moves_its_column() {
    let (mut p, _dir) = loaded_pump();
    let (from, _, y) = header_of(&mut p, "name");
    let (age, age_to, _) = header_of(&mut p, "age");
    assert!(p.terminal_mouse(click((from + 1, y))).unwrap());
    assert_eq!(cell(&p).1.as_deref(), Some("name"));
    assert!(
        p.terminal_mouse(drag((age + 1, y))).unwrap(),
        "a frame is due for the drop mark"
    );
    assert!(
        !p.terminal_mouse(drag((age + 2, y))).unwrap(),
        "still over age: nothing new to draw"
    );
    let area = Rect::new(0, 0, 100, 20);
    let mut buf = Buffer::empty(area);
    p.app.render(area, &mut buf);
    assert_eq!(
        buf[(age_to, y)].symbol(),
        crate::glyphs::get().rule,
        "the drop mark, after age"
    );
    p.terminal_mouse(release((age + 1, y))).unwrap();
    settle(&mut p);
    assert_eq!(order(&p), ["age", "name"]);
    assert_eq!(cell(&p).1.as_deref(), Some("name"), "the cursor came along");

    // Let go where it was picked up: nothing moves.
    let (from, _, y) = header_of(&mut p, "name");
    p.terminal_mouse(click((from + 1, y))).unwrap();
    p.terminal_mouse(release((from + 1, y))).unwrap();
    settle(&mut p);
    assert_eq!(order(&p), ["age", "name"]);
}

/// The gap after a header dragged sideways sets its width by hand, within the
/// bounds `<` and `>` keep.
#[test]
fn dragging_a_header_edge_resizes_its_column() {
    use crate::widgets::column_widths::{MAX_WIDTH, MIN_WIDTH, WidthChoice};
    let (mut p, _dir) = loaded_pump();
    let (from, to, y) = header_of(&mut p, "name");
    let width = to - from;
    assert!(!p.terminal_mouse(click((to, y))).unwrap(), "a press only");
    assert!(p.terminal_mouse(drag((to + 5, y))).unwrap());
    let choice = |p: &EventPump| {
        p.app
            .data_table_state
            .as_ref()
            .unwrap()
            .width_choice("name")
    };
    assert_eq!(choice(&p), WidthChoice::Manual(width + 5));
    p.terminal_mouse(drag((0, y))).unwrap();
    assert_eq!(choice(&p), WidthChoice::Manual(MIN_WIDTH));
    p.terminal_mouse(drag((99, y))).unwrap();
    assert!(matches!(choice(&p), WidthChoice::Manual(w) if w <= MAX_WIDTH));
    p.terminal_mouse(release((99, y))).unwrap();
    assert_eq!(order(&p), ["name", "age"], "a resize moves no column");
    // Without a press first, a drag does nothing.
    let before = choice(&p);
    p.terminal_mouse(drag((to + 9, y))).unwrap();
    assert_eq!(choice(&p), before);
}

/// Press and let go at `at`, after a frame painted, as a click on what is drawn.
fn click_and_release(p: &mut EventPump, at: (u16, u16)) {
    rendered(&mut p.app);
    p.app.frame_painted();
    p.terminal_mouse(click(at)).unwrap();
    p.terminal_mouse(release(at)).unwrap();
    settle(p);
}

/// The sort in effect: its columns and directions.
fn sorted(p: &EventPump) -> (Vec<String>, Vec<bool>) {
    let state = p.app.data_table_state.as_ref().unwrap();
    let columns = state.view_sort_columns().to_vec();
    let descending = state.view_sort_descending().to_vec();
    (
        columns.clone(),
        descending[..columns.len().min(descending.len())].to_vec(),
    )
}

/// A double click on a header sorts by its column as `[` and `]` do: ascending,
/// descending, then off. One click only moves the column cursor.
#[test]
fn a_double_click_on_a_header_cycles_its_sort() {
    let (mut p, _dir) = loaded_pump();
    let (age, _, y) = header_of(&mut p, "age");
    let at = (age + 1, y);
    click_and_release(&mut p, at);
    assert_eq!(cell(&p).1.as_deref(), Some("age"));
    assert_eq!(sorted(&p), (vec![], vec![]), "one click only moves");

    click_and_release(&mut p, at);
    assert_eq!(sorted(&p), (vec!["age".to_string()], vec![false]));
    assert_eq!(order(&p), ["name", "age"], "a sort moves no column");

    click_and_release(&mut p, at);
    click_and_release(&mut p, at);
    assert_eq!(sorted(&p), (vec!["age".to_string()], vec![true]));

    click_and_release(&mut p, at);
    click_and_release(&mut p, at);
    assert_eq!(sorted(&p), (vec![], vec![]), "the third takes it away");
}

/// A double click on the gap after a header fits the column, as `=` does, and
/// sorts nothing.
#[test]
fn a_double_click_on_a_header_edge_fits_the_column() {
    use crate::widgets::column_widths::WidthChoice;
    let (mut p, _dir) = loaded_pump();
    let (_, to, y) = header_of(&mut p, "name");
    // The cursor elsewhere, to see it come to the column fitted.
    let (age, _, _) = header_of(&mut p, "age");
    click_and_release(&mut p, (age + 1, y));
    click_and_release(&mut p, (to, y));
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_ne!(
        state.width_choice("name"),
        WidthChoice::Fit,
        "one press only"
    );
    click_and_release(&mut p, (to, y));
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_eq!(state.width_choice("name"), WidthChoice::Fit);
    assert_eq!(sorted(&p), (vec![], vec![]), "never a sort");
    assert_eq!(cell(&p).1.as_deref(), Some("name"));
    assert_eq!(order(&p), ["name", "age"]);
}

/// A key handled marks the frame on screen out of date, so a click read after it
/// waits for the frame that shows what the key did, a replayed key included.
#[test]
fn a_key_handled_puts_the_next_click_behind_a_frame() {
    let (mut p, _dir) = loaded_pump();
    rendered(&mut p.app);
    p.app.frame_painted();
    assert!(p.app.pointer.on_screen());
    p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
    assert!(!p.app.pointer.on_screen());
}

/// The chart's panel takes clicks on its rows: a click focuses one and acts as
/// Space, so the type steps and a toggle flips.
#[test]
fn a_click_on_the_chart_panel_focuses_and_acts() {
    use crate::chart_modal::ChartFocus;
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('c'))).unwrap();
    settle(&mut p);
    assert_eq!(p.app.input_mode, InputMode::Chart);
    let mark = p.app.chart_modal.spec.mark;
    let at = on_screen(&mut p.app, "Type");
    assert!(p.terminal_mouse(click(at)).unwrap());
    settle(&mut p);
    assert_eq!(p.app.chart_modal.focus, ChartFocus::Type);
    assert_ne!(p.app.chart_modal.spec.mark, mark, "the type stepped");
    let grid = p.app.chart_modal.grid;
    let at = on_screen(&mut p.app, "Grid");
    p.terminal_mouse(click(at)).unwrap();
    settle(&mut p);
    assert_eq!(p.app.chart_modal.focus, ChartFocus::Grid);
    assert_ne!(p.app.chart_modal.grid, grid, "the grid flipped");
}

/// A header carried off the columns is over itself again: let go there, nothing
/// moves. A key while carrying puts it back too.
#[test]
fn a_header_drag_is_cancelled_off_the_columns_or_by_a_key() {
    let (mut p, _dir) = loaded_pump();
    let (from, _, y) = header_of(&mut p, "name");
    let (age, _, _) = header_of(&mut p, "age");
    p.terminal_mouse(click((from + 1, y))).unwrap();
    p.terminal_mouse(drag((age + 1, y))).unwrap();
    assert!(
        p.terminal_mouse(drag((age + 1, 19))).unwrap(),
        "the mark goes"
    );
    p.terminal_mouse(release((age + 1, 19))).unwrap();
    settle(&mut p);
    assert_eq!(order(&p), ["name", "age"]);

    p.terminal_mouse(click((from + 1, y))).unwrap();
    p.terminal_mouse(drag((age + 1, y))).unwrap();
    p.terminal_key(plain(KeyCode::Char('#'))).unwrap();
    assert!(p.app.pointer.drag().is_none());
    p.terminal_mouse(release((age + 1, y))).unwrap();
    settle(&mut p);
    assert_eq!(order(&p), ["name", "age"]);
}

/// The last column reaches the right side, so no gap follows it: its last
/// header cell is its edge.
#[test]
fn the_last_column_resizes_from_the_right_side() {
    use crate::widgets::column_widths::WidthChoice;
    let (mut p, _dir) = loaded_pump();
    // Wide enough to be cut at the right side.
    p.terminal_key(plain(KeyCode::Char('l'))).unwrap();
    for _ in 0..30 {
        p.terminal_key(plain(KeyCode::Char('>'))).unwrap();
    }
    let (from, to, y) = header_of(&mut p, "age");
    assert_eq!(to, 100, "age reaches the right side");
    let choice = |p: &EventPump| p.app.data_table_state.as_ref().unwrap().width_choice("age");
    let WidthChoice::Manual(set) = choice(&p) else {
        panic!("set by hand: {:?}", choice(&p));
    };
    assert!(set > to - from, "cut at the side");
    assert!(
        !p.terminal_mouse(click((to - 1, y))).unwrap(),
        "a press on the edge"
    );
    p.terminal_mouse(drag((to - 31, y))).unwrap();
    assert_eq!(choice(&p), WidthChoice::Manual(set - 30));
}

/// A right click on a choice steps it back; on a checkbox it only focuses.
#[test]
fn a_right_click_on_a_checkbox_only_focuses_it() {
    use crate::export_modal::ExportFocus;
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('e'))).unwrap();
    let header = p.app.export_modal.csv_include_header;
    let at = on_screen(&mut p.app, "Header:");
    p.terminal_mouse(right_click(at)).unwrap();
    assert_eq!(p.app.export_modal.focus, ExportFocus::CsvIncludeHeader);
    assert_eq!(p.app.export_modal.csv_include_header, header, "not toggled");
}

/// A row of a list (a sort, a column) takes focus on the first click and acts on
/// the second, so it can be picked out without changing it.
#[test]
fn a_list_row_focuses_first_and_acts_on_a_second_click() {
    use crate::sort_filter_modal::{SortFilterField, SortFilterTab};
    let (mut p, _dir) = loaded_pump();
    p.terminal_key(plain(KeyCode::Char('['))).unwrap();
    settle(&mut p);
    p.terminal_key(plain(KeyCode::Char('s'))).unwrap();
    assert_eq!(p.app.sort_filter_modal.active_tab, SortFilterTab::InEffect);
    p.app.sort_filter_modal.focus = SortFilterField::TabBar;
    let descending = |p: &EventPump| {
        let m = &p.app.sort_filter_modal.sort;
        m.columns[m.sort_entries()[0]].sort_descending
    };
    let before = descending(&p);
    let row = format!(" 1 {} name", crate::glyphs::get().sort_asc);
    let at = on_screen(&mut p.app, &row);
    p.terminal_mouse(click(at)).unwrap();
    assert_eq!(p.app.sort_filter_modal.focus, SortFilterField::Sort(0));
    assert_eq!(descending(&p), before, "the first click only focuses");
    p.terminal_mouse(click(at)).unwrap();
    assert_ne!(descending(&p), before, "the second flips it");
}

/// The menu opens only on the cell the cursor landed on: rows drawn before the
/// view scrolled are not the ones clicked.
#[test]
fn the_menu_opens_only_where_the_cursor_lands() {
    let (mut p, _dir) = numbered_pump(50);
    let at = on_screen(&mut p.app, "r0002");
    // The view scrolled since the frame on screen was drawn.
    p.terminal_key(plain(KeyCode::End)).unwrap();
    settle(&mut p);
    let row = cell(&p).0;
    p.terminal_mouse(right_click(at)).unwrap();
    assert_eq!(cell(&p).0, row, "the cursor stays");
    assert!(p.app.context_menu.is_none());
}

/// While a job runs, the open menu's own keys still act: they read nothing.
#[test]
fn the_menus_keys_act_while_busy() {
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    p.terminal_mouse(right_click(alan)).unwrap();
    p.app.busy = true;
    assert!(p.terminal_key(plain(KeyCode::Down)).unwrap());
    assert_eq!(p.app.context_menu.as_ref().map(|m| m.selected), Some(1));
    assert!(p.terminal_key(plain(KeyCode::Esc)).unwrap());
    assert!(p.app.context_menu.is_none());
    assert!(held(&p).is_empty());
}

/// Drags read together are one: only the last place counts.
#[test]
fn drags_waiting_together_are_one() {
    let (mut p, _dir) = loaded_pump();
    let (from, _, y) = header_of(&mut p, "name");
    let (age, _, _) = header_of(&mut p, "age");
    p.app.frame_painted();
    for event in [
        click((from + 1, y)),
        drag((age + 1, y)),
        drag((from + 1, y)),
        drag((age + 1, y)),
        release((age + 1, y)),
    ] {
        p.send(AppEvent::Terminal(Event::Mouse(event))).unwrap();
    }
    for _ in 0..6 {
        p.drain().unwrap();
        rendered(&mut p.app);
        p.app.frame_painted();
    }
    settle(&mut p);
    assert_eq!(order(&p), ["age", "name"]);
}

/// A right click on a cell puts the cursor there and opens the menu; Enter runs
/// its line by pressing the line's key, and the menu closes.
#[test]
fn a_right_click_opens_the_menu_and_enter_runs_a_line() {
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    assert!(p.terminal_mouse(right_click(alan)).unwrap());
    assert_eq!(cell(&p), (Some(2), Some("name".to_string())));
    assert!(p.app.context_menu.is_some());
    let text = rendered(&mut p.app);
    assert!(text.contains("Filter to this value"), "the menu is drawn");
    // The menu takes the arrows; the table's cursor stays.
    p.terminal_key(plain(KeyCode::Down)).unwrap();
    p.terminal_key(plain(KeyCode::Up)).unwrap();
    assert_eq!(cell(&p).0, Some(2));
    p.terminal_key(plain(KeyCode::Enter)).unwrap();
    settle(&mut p);
    assert!(p.app.context_menu.is_none());
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_eq!(state.view_filters().len(), 1, "+ filtered to the value");
    assert_eq!(state.num_rows(), 1);
}

/// A click on a line runs it; a click outside closes the menu and does nothing
/// more; Esc closes it.
#[test]
fn a_click_chooses_from_the_menu_and_outside_closes_it() {
    let (mut p, _dir) = loaded_pump();
    let grace = on_screen(&mut p.app, "grace");
    p.terminal_mouse(right_click(grace)).unwrap();
    let counts = on_screen(&mut p.app, "Value counts");
    assert!(p.terminal_mouse(click(counts)).unwrap());
    assert!(p.app.context_menu.is_none());
    assert_eq!(p.app.input_mode, InputMode::ValueCounts, "F ran");
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    settle(&mut p);

    let alan = on_screen(&mut p.app, "alan");
    p.terminal_mouse(right_click(alan)).unwrap();
    rendered(&mut p.app);
    assert!(p.terminal_mouse(click((0, 0))).unwrap());
    assert!(p.app.context_menu.is_none(), "closed");
    assert_eq!(cell(&p).0, Some(2), "and nothing else moved");
    p.terminal_mouse(right_click(alan)).unwrap();
    p.terminal_key(plain(KeyCode::Esc)).unwrap();
    assert!(p.app.context_menu.is_none());
    assert_eq!(p.app.input_mode, InputMode::Normal);
}

/// Busy, the mouse acts only where a typed key would act at once: no menu opens,
/// a dropped header moves nothing and a menu line presses nothing, while a width
/// drag acts, as `>` does at a busy table.
#[test]
fn mouse_actions_while_busy_obey_the_key_rules() {
    use crate::widgets::column_widths::WidthChoice;
    let (mut p, _dir) = loaded_pump();
    let alan = on_screen(&mut p.app, "alan");
    let (from, to, y) = header_of(&mut p, "name");
    let (age, _, _) = header_of(&mut p, "age");
    p.app.busy = true;
    assert!(!p.terminal_mouse(right_click(alan)).unwrap());
    assert!(p.app.context_menu.is_none());

    p.app.busy = false;
    p.terminal_mouse(click((from + 1, y))).unwrap();
    p.terminal_mouse(drag((age + 1, y))).unwrap();
    p.app.busy = true;
    p.terminal_mouse(release((age + 1, y))).unwrap();
    assert!(held(&p).is_empty(), "the mouse is never held");
    p.app.busy = false;
    settle(&mut p);
    assert_eq!(order(&p), ["name", "age"], "dropped");

    p.app.busy = true;
    p.terminal_mouse(click((to, y))).unwrap();
    p.terminal_mouse(drag((to + 3, y))).unwrap();
    let state = p.app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.width_choice("name"),
        WidthChoice::Manual(to - from + 3)
    );

    p.app.busy = false;
    p.terminal_mouse(right_click(alan)).unwrap();
    rendered(&mut p.app);
    let line = on_screen(&mut p.app, "Filter to this value");
    p.app.busy = true;
    p.terminal_mouse(click(line)).unwrap();
    assert!(p.app.context_menu.is_none(), "the menu closes");
    p.app.busy = false;
    settle(&mut p);
    let state = p.app.data_table_state.as_ref().unwrap();
    assert!(state.view_filters().is_empty(), "and its key was dropped");
}

/// A spinner for work nobody waits on, a count say, turns about ten times a second:
/// a third of the frames of one the user waits on, and still moving.
#[test]
fn a_spinner_nobody_waits_on_turns_slower() {
    let frames = |waited_on: bool| {
        let mut pacer = Pacer::default();
        let start = Instant::now();
        let mut drawn = 0;
        for ms in 0..=1000 {
            let now = start + Duration::from_millis(ms);
            pacer.spinning(true, waited_on, now);
            if pacer.turn_spinner(now) {
                drawn += 1;
            }
        }
        drawn
    };
    assert_eq!(frames(true), 30);
    assert_eq!(frames(false), 10);
}
