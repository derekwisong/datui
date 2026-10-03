use crate::*;
use std::sync::mpsc;

#[test]
fn a_stalled_cache_read_holds_up_neither_the_home_screen_nor_its_keys() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("recently.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let recents = dir.path().join("recents_history.txt");
    let c_path = std::ffi::CString::new(recents.as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: a valid NUL-terminated path; mkfifo reads nothing else.
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.cache = CacheManager {
        cache_dir: dir.path().to_path_buf(),
    };
    // Returns with the read still blocked, and the screen works meanwhile.
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);
    let area = Rect::new(0, 0, 100, 20);
    app.render(area, &mut Buffer::empty(area));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('r'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.filter, "r", "a key typed now is taken now");
    assert!(app.home.listing_in_flight, "the listing waits on the read");

    // The read finishes; the listing it was waiting on arrives with the recent.
    let writer = {
        let csv = csv.clone();
        std::thread::spawn(move || {
            let mut fifo = std::fs::OpenOptions::new()
                .write(true)
                .open(recents)
                .unwrap();
            std::io::Write::write_all(&mut fifo, format!("{}\n", csv.display()).as_bytes())
                .unwrap();
        })
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while app.home.listing_in_flight {
        assert!(
            std::time::Instant::now() < deadline,
            "the listing never came"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            let mut next = Some(event);
            while let Some(event) = next.take() {
                next = app.event(&event);
            }
        }
    }
    writer.join().unwrap();
    assert!(
        format!("{:?}", app.home.sections).contains("recently.csv"),
        "the recent read late is listed"
    );
}
