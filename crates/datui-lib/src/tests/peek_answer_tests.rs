use crate::discover::{EntryKind, Holds};

fn counted(format: &str, n: usize) -> Holds {
    Holds {
        formats: vec![(format.to_string(), n)],
        ..Default::default()
    }
}

/// A peek is worth a listing rebuild when its answer reaches the screen, and a
/// rebuild reads the dataset index on the thread drawing the frame — so the test is
/// what the answer says, not what the peek decided. The screen is the row *and* the
/// details pane beside it, which draws the whole `holds` line.
#[test]
fn a_peek_is_worth_a_rebuild_when_its_answer_says_anything() {
    let worth = crate::App::peek_tells_a_row_something;

    // The claim staked before the answers arrive: nothing counted, nothing decided.
    // The only thing there is no reason to send.
    assert!(!worth(&(EntryKind::Directory, Holds::default())));

    // Only Parquet is read in place, so a prefix of twelve CSV objects stays a
    // `Directory` — and it is still `12 csv`, which is the label the row draws.
    assert!(worth(&(EntryKind::Directory, counted("csv", 12))));
    // A prefix of sub-prefixes and a writer's own files draws no label of its own,
    // but the pane has `12 directories · 3 skipped` to say, and says it on disk.
    assert!(worth(&(
        EntryKind::Directory,
        Holds {
            directories: 12,
            skipped: 3,
            ..Default::default()
        }
    )));
    // A README and two PDFs: nothing datui reads, which is itself the answer.
    assert!(worth(&(
        EntryKind::Directory,
        Holds {
            not_read: 3,
            ..Default::default()
        }
    )));
    // A listing cut short says so. Nothing draws it on this route today — see
    // the note on the function — but `is_empty` is one definition and this is
    // what it says.
    assert!(worth(&(
        EntryKind::Directory,
        Holds {
            truncated: true,
            ..Default::default()
        }
    )));

    // And the kinds that decide something say it whether they counted or not — a
    // partitioned lake table has no data file at its root, so its `Holds` is empty
    // and the kind is the whole of the answer.
    for kind in [
        EntryKind::Hive,
        EntryKind::Delta,
        EntryKind::Iceberg,
        EntryKind::Hudi,
    ] {
        assert!(worth(&(kind, Holds::default())), "{kind:?} decides the row");
    }
    assert!(worth(&(EntryKind::MultiFile, counted("parquet", 40))));
}
