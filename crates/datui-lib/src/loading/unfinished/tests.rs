use super::*;

fn file_in(dir: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    tempfile::NamedTempFile::new_in(dir)
}

fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

/// A writer that stops in time cleans up after itself, and the sweep waits for it
/// rather than for the whole grace.
#[test]
fn a_sweep_waits_for_a_writer_that_stops() {
    let dir = tempfile::tempdir().unwrap();
    let unfinished = Unfinished::default();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = unfinished.writer(stop.clone());
    let (claimed, wait) = std::sync::mpsc::channel();
    let worker = {
        let dir = dir.path().to_path_buf();
        std::thread::spawn(move || {
            let (file, claim) = writer
                .create(|| file_in(&dir))
                .unwrap()
                .expect("not stopped yet");
            claimed.send(()).unwrap();
            while !writer.stopped() {
                std::thread::sleep(Duration::from_millis(5));
            }
            // As a writer does: the file goes, then the claim.
            drop(file);
            drop(claim);
        })
    };
    wait.recv().unwrap();
    stop.store(true, Ordering::Relaxed);
    let began = Instant::now();
    unfinished.sweep(began + Duration::from_secs(30));
    assert!(
        began.elapsed() < Duration::from_secs(10),
        "not the whole grace"
    );
    assert_eq!(files_in(dir.path()), 0);
    assert!(!unfinished.writing());
    worker.join().unwrap();
}

/// A file still claimed at the deadline is removed by the sweep, and its holder
/// letting go later is harmless.
#[test]
fn a_sweep_removes_what_a_writer_still_holds() {
    let dir = tempfile::tempdir().unwrap();
    let unfinished = Unfinished::default();
    let writer = unfinished.writer(Arc::default());
    let (file, claim) = writer
        .create(|| file_in(dir.path()))
        .unwrap()
        .expect("not stopped yet");
    let path = file.path().to_path_buf();
    unfinished.sweep(Instant::now() + Duration::from_millis(20));
    assert!(!path.exists());
    assert!(!unfinished.writing());
    drop(file);
    drop(claim);
}

/// A file being created when the sweep starts is waited for: the sweep does not
/// end, and the process with it, while a file it has not heard of is still to
/// appear. Here the open is stopped, so the writer removes it itself.
#[test]
fn a_sweep_waits_for_a_file_being_created() {
    let dir = tempfile::tempdir().unwrap();
    let unfinished = Unfinished::default();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = unfinished.writer(stop.clone());
    let (creating, wait) = std::sync::mpsc::channel();
    let (go, gate) = std::sync::mpsc::channel::<()>();
    let worker = {
        let dir = dir.path().to_path_buf();
        std::thread::spawn(move || {
            writer
                .create(|| {
                    creating.send(()).unwrap();
                    gate.recv().unwrap();
                    file_in(&dir)
                })
                .unwrap()
                .is_none()
        })
    };
    wait.recv().unwrap();
    stop.store(true, Ordering::Relaxed);
    let sweeper = {
        let unfinished = unfinished.clone();
        std::thread::spawn(move || unfinished.sweep(Instant::now() + Duration::from_secs(30)))
    };
    assert!(
        unfinished.a_sweep_waits_within(Duration::from_secs(30)),
        "the sweep waits for the file"
    );
    assert!(unfinished.writing());
    assert!(!sweeper.is_finished());
    go.send(()).unwrap();
    sweeper.join().unwrap();
    // Checked before joining the writer: its thread may still be exiting, but the
    // file it made must already be gone.
    assert_eq!(files_in(dir.path()), 0, "the sweep waited for the file");
    assert!(!unfinished.writing());
    assert!(worker.join().unwrap(), "a stopped open's file is refused");
}

/// A file its holder could not remove, as on Windows while a frame still maps it,
/// is not forgotten: the next claim to let go tries it again, and so does the
/// sweep, without waiting on it as on a writer.
#[test]
fn a_file_its_holder_could_not_remove_is_removed_later() {
    let dir = tempfile::tempdir().unwrap();
    let unfinished = Unfinished::default();
    let writer = unfinished.writer(Arc::default());
    // A holder whose removal failed: the claim goes, the file stays.
    let left_over = |name: &str| {
        let path = dir.path().join(name);
        let (path, claim) = writer
            .create(|| std::fs::write(&path, b"x").map(|()| path.clone()))
            .unwrap()
            .expect("not stopped yet");
        drop(claim);
        assert!(path.exists());
        path
    };
    let first = left_over("first");
    assert!(!unfinished.writing(), "nothing is writing it");
    let (file, claim) = writer
        .create(|| file_in(dir.path()))
        .unwrap()
        .expect("not stopped yet");
    drop(file);
    drop(claim);
    assert!(!first.exists(), "the next claim tried it again");

    let second = left_over("second");
    let began = Instant::now();
    unfinished.sweep(began + Duration::from_secs(30));
    assert!(began.elapsed() < Duration::from_secs(10), "not waited on");
    assert!(!second.exists(), "the sweep removed it");
    assert_eq!(files_in(dir.path()), 0);
}

/// A stopped or swept open creates nothing.
#[test]
fn a_stopped_or_swept_open_refuses_new_files() {
    let dir = tempfile::tempdir().unwrap();
    let unfinished = Unfinished::default();
    let made = |writer: &Writer| {
        writer
            .create(|| file_in(dir.path()))
            .unwrap()
            .map(|(file, _)| file)
    };
    let stopped = unfinished.writer(Arc::new(AtomicBool::new(true)));
    assert!(made(&stopped).is_none());

    let writer = unfinished.writer(Arc::default());
    unfinished.sweep(Instant::now());
    assert!(made(&writer).is_none());
    assert_eq!(files_in(dir.path()), 0);
}
