use super::*;
use polars::prelude::{ParquetWriter, df};
use std::fs;
use std::fs::File;

fn write_parquet(path: &Path, n: i64) {
    let mut df = df!("v" => (0..n).collect::<Vec<i64>>()).unwrap();
    let file = File::create(path).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();
}

/// Every Parquet file under `dir`, the footers an open would read, and those
/// footers: the open's first two steps, as it takes them.
fn footers_of(
    dir: &Path,
    progress: &Arc<FooterProgress>,
    meter: &Arc<Meter>,
) -> (Vec<DatasetFile>, Vec<usize>, Vec<Option<FileFooter>>) {
    let local = LocalFiles::new(dir);
    let (files, _skipped, _fingerprint) = list(&local, progress, meter).unwrap();
    let read = footers_to_read(files.len());
    let files = Arc::new(files);
    let footers = local.read_footers(&files, &read, progress, meter).unwrap();
    (files.to_vec(), read, footers)
}

#[test]
fn a_count_that_read_nothing_leaves_the_measurement_for_the_one_that_does() {
    // Counting gets one measurement, and a pass where no footer parsed settled
    // nothing. If such a pass took it, the pass that eventually succeeds is
    // declined and the row never reflects the count at all.
    let dir = tempfile::tempdir().unwrap();
    let part = dir.path().join("date=2024-01-01");
    std::fs::create_dir_all(&part).unwrap();
    std::fs::write(part.join("broken.parquet"), b"not parquet").unwrap();

    let meter = Meter::default();
    // As the open leaves it. A count belongs to an open this meter measured, so
    // without a listing here the count would be declined for that reason instead.
    meter.listed(std::time::Duration::from_millis(1), Some(1), false);
    assert!(
        LocalFiles::new(dir.path())
            .count_rows(&meter, &Default::default())
            .is_err(),
        "nothing under there parses"
    );
    assert_eq!(
        meter.footers(),
        None,
        "so nothing was measured, and the one measurement is still to be had"
    );

    // The file is replaced by one that does parse, as a half-written file is once
    // its writer finishes.
    let mut frame = df!("n" => &[1i64, 2]).unwrap();
    let f = std::fs::File::create(part.join("broken.parquet")).unwrap();
    ParquetWriter::new(f).finish(&mut frame).unwrap();
    assert_eq!(
        LocalFiles::new(dir.path())
            .count_rows(&meter, &Default::default())
            .unwrap(),
        2,
        "and now it counts"
    );
    assert_eq!(
        meter.footers().and_then(|c| c.files),
        Some(1),
        "and the count that worked is the one reported"
    );
}

#[test]
fn counting_a_directory_again_is_not_more_of_what_the_open_cost() {
    // Counting runs whenever the row count is invalidated, and clearing a filter does
    // it — so on a dataset somebody is exploring this function runs over and over.
    // Each run re-walks the directory and re-reads every footer, and if each one were
    // added the section headed by what opening the dataset cost would climb for as
    // long as the session lasted.
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        let d = dir.path().join(format!("date=2024-01-0{day}"));
        std::fs::create_dir_all(&d).unwrap();
        let mut frame = df!("n" => &[day as i64]).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
    }

    let meter = Meter::default();
    meter.listed(std::time::Duration::from_millis(1), Some(3), false);
    let first = LocalFiles::new(dir.path())
        .count_rows(&meter, &Default::default())
        .unwrap();
    let after_one = meter.footers().expect("the first count was measured");
    assert_eq!(
        after_one.files,
        Some(3),
        "a footer read from each of the three"
    );

    for _ in 0..3 {
        let again = LocalFiles::new(dir.path())
            .count_rows(&meter, &Default::default())
            .unwrap();
        assert_eq!(again, first, "the same count every time");
    }
    assert_eq!(
        meter.footers(),
        Some(after_one),
        "and the figures stand where the first count left them"
    );
}

#[test]
fn test_count_rows_from_parquet_dir_sums_footers() {
    let dir = tempfile::tempdir().unwrap();
    // Hive-style layout: two partitions, multiple files each.
    let p1 = dir.path().join("year=2020");
    let p2 = dir.path().join("year=2021");
    fs::create_dir_all(&p1).unwrap();
    fs::create_dir_all(&p2).unwrap();
    write_parquet(&p1.join("a.parquet"), 10);
    write_parquet(&p1.join("b.parquet"), 5);
    write_parquet(&p2.join("c.parquet"), 7);

    let n = LocalFiles::new(dir.path())
        .count_rows(&Meter::default(), &Default::default())
        .unwrap();
    assert_eq!(n, 22, "should sum footer row counts across all files");
}

#[test]
fn test_count_rows_skips_non_parquet_and_corrupt_files() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(&dir.path().join("good.parquet"), 8);
    // A non-parquet file must be ignored entirely.
    fs::write(dir.path().join("notes.txt"), b"ignore me").unwrap();
    // A corrupt .parquet must be skipped, not abort the whole count.
    fs::write(dir.path().join("bad.parquet"), b"not a parquet footer").unwrap();

    let n = LocalFiles::new(dir.path())
        .count_rows(&Meter::default(), &Default::default())
        .unwrap();
    assert_eq!(n, 8, "non-parquet and unreadable files should be skipped");
}

#[test]
fn test_count_rows_errors_when_no_readable_parquet() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("only.txt"), b"nothing here").unwrap();
    assert!(
        LocalFiles::new(dir.path())
            .count_rows(&Meter::default(), &Default::default())
            .is_err(),
        "a directory with no parquet files should error so the caller can fall back"
    );
}

/// The real footer pass counts real footers.
///
/// The unit tests above drive the counter by hand; this is the one that says the
/// pass is wired to it at all, and that the total is the footers it will read
/// rather than the files there are — the two differ once a dataset is large enough
/// to be sampled.
#[test]
fn the_footer_pass_counts_the_footers_it_reads() {
    use polars::prelude::{ParquetWriter, df};

    let dir = tempfile::tempdir().unwrap();
    for day in 1..=4 {
        let d = dir.path().join(format!("date=2024-01-0{day}"));
        std::fs::create_dir_all(&d).unwrap();
        let mut frame = df!("n" => [day as i64]).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
    }

    let progress = Arc::new(FooterProgress::default());
    let (files, read, footers) = footers_of(dir.path(), &progress, &Arc::new(Meter::default()));
    assert_eq!((files.len(), read.len(), footers.len()), (4, 4, 4));
    assert_eq!(
        progress.reading(),
        None,
        "the pass says nothing once it has landed"
    );
    assert_eq!(
        progress.last_pass().begun,
        1,
        "and it did report: the count is unobservable afterwards, so without this \
             a pass that never told anyone would look the same as one that did"
    );
    assert_eq!(
        progress.last_pass().read,
        4,
        "counting every footer it read, not just starting and stopping"
    );
}

/// A local open measures finding the files and reading their footers separately.
///
/// Separately because they are separate costs and a directory that is slow to open is
/// slow at one of them; a single figure over both would say a directory is slow
/// without saying at what. Neither claims requests or bytes: a local directory is
/// read, not requested, and a zero there would read as "nothing moved" rather than
/// "not datui's to count".
///
/// Only the counts are asserted. The times are real elapsed times on a machine
/// doing other things, so the only claim about them that holds every time is that
/// they were recorded at all — which `Some` already says.
#[test]
fn a_local_open_measures_its_listing_and_its_footers() {
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=4 {
        let d = dir.path().join(format!("date=2024-01-0{day}"));
        std::fs::create_dir_all(&d).unwrap();
        let mut frame = df!("n" => &[day as i64]).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
    }

    let meter = Arc::new(Meter::default());
    let _ = footers_of(dir.path(), &Arc::new(FooterProgress::default()), &meter);

    let listing = meter.listing().expect("the open measured its listing");
    assert_eq!(listing.files, Some(4), "the walk found four files");
    assert!(
        listing.over_the_wire.is_none(),
        "and made no requests to find them"
    );
    let footers = meter.footers().expect("and measured its footer pass");
    assert_eq!(footers.files, Some(4), "a footer was read from each");

    assert!(
        footers.over_the_wire.is_none(),
        "off a disk, not a wire: no requests to report and no bytes to claim"
    );
}

/// The denominator is the footers it will read, not the files there are.
///
/// The two are the same number until a dataset is large enough to be sampled, which
/// is why this fixture is twenty thousand and one files — below that the confusion
/// is invisible, and a test that cannot see it is not a test of it. The files need
/// not be real Parquet: a footer that will not read is still a footer counted off,
/// which is the other half of what this asserts.
#[test]
fn the_footer_count_is_over_the_footers_read_not_the_files_there_are() {
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("date=2024-01-01");
    std::fs::create_dir_all(&sub).unwrap();
    let files = crate::formats::schema_union::MAX_FOOTER_READS + 1;
    for i in 0..files {
        std::fs::write(sub.join(format!("f{i:0>6}.parquet")), b"not parquet").unwrap();
    }

    let progress = Arc::new(FooterProgress::default());
    let meter = Arc::new(Meter::default());
    let (found, read, footers) = footers_of(dir.path(), &progress, &meter);
    assert_eq!(found.len(), files, "every file is listed");
    assert_eq!(
        read.len(),
        crate::formats::schema_union::MAX_FOOTER_READS,
        "and a sample of them is read"
    );
    assert!(footers.iter().all(Option::is_none), "none of them parses");
    // The same distinction, in the measurement: the listing found every file and
    // the footer pass read a sample of them. A fixture below the sampling threshold
    // cannot tell the two numbers apart, which is why this one asserts them.
    assert_eq!(
        meter.listing().and_then(|c| c.files),
        Some(files),
        "the listing counts the files there are"
    );
    assert_eq!(
        meter.footers().and_then(|c| c.files),
        Some(crate::formats::schema_union::MAX_FOOTER_READS),
        "while the footer pass counts the footers it read, which is fewer"
    );

    assert_eq!(
        progress.last_pass().total,
        read.len(),
        "the screen's denominator is the sample, not the {files} files there are"
    );
    assert_eq!(
        progress.last_pass().read,
        read.len(),
        "and every one of them was counted off, parse or no parse"
    );
}

/// A local listing counts the files it finds for the loading screen, as a cloud
/// one does, and an abandoned open stops it before it reads a directory (#710).
#[test]
fn a_local_listing_counts_its_files_and_stops_when_abandoned() {
    let dir = tempfile::tempdir().unwrap();
    for day in 0..3 {
        let sub = dir.path().join(format!("day={day}"));
        std::fs::create_dir_all(&sub).unwrap();
        for i in 0..4 {
            std::fs::write(sub.join(format!("f{i}.parquet")), b"x").unwrap();
        }
    }
    let progress = FooterProgress::default();
    {
        let listing = progress.listing();
        let (files, _) = LocalFiles::new(dir.path()).walk(Some(&listing));
        assert_eq!(files.len(), 12);
        assert_eq!(progress.listed(), Some(12), "Listing files: 12");
    }
    assert_eq!(progress.listed(), None, "and says nothing once it is done");

    progress.cancel();
    let listing = progress.listing();
    let (files, _) = LocalFiles::new(dir.path()).walk(Some(&listing));
    assert!(files.is_empty(), "a cancelled listing reads no directory");
    assert_eq!(progress.listed(), Some(0));
}

/// A tree of `files` small Parquet files under `part=N/` directories, a hundred to a
/// directory, three rows each.
fn tree(dir: &Path, files: usize) {
    let mut bytes = Vec::new();
    ParquetWriter::new(&mut bytes)
        .finish(&mut df!("v" => [1i64, 2, 3]).unwrap())
        .unwrap();
    for i in 0..files {
        let sub = dir.join(format!("part={}", i / 100));
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join(format!("f{i:04}.parquet")), &bytes).unwrap();
    }
}

/// A count keeps each file's footer by the file's identity: counted again it reads
/// none, with a file added it reads that one, and a count stopped part way keeps
/// what it read for the next.
#[test]
fn a_count_keeps_each_file_s_footer_for_the_next() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    tree(dir.path(), 100);
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
    let reads = Arc::new(AtomicUsize::new(0));
    let stop_after = Arc::new(AtomicUsize::new(usize::MAX));
    let progress = Arc::new(std::sync::Mutex::new(Arc::new(FooterProgress::counting())));
    let _hook = {
        let (reads, stop_after, progress) = (reads.clone(), stop_after.clone(), progress.clone());
        crate::formats::schema_union::on_local_footer_read(dir.path(), move |_| {
            if reads.fetch_add(1, Ordering::SeqCst) + 1 >= stop_after.load(Ordering::SeqCst) {
                progress.lock().unwrap().cancel();
            }
        })
    };
    let count = || {
        reads.store(0, Ordering::SeqCst);
        let fresh = Arc::new(FooterProgress::counting());
        *progress.lock().unwrap() = fresh.clone();
        let local: Arc<dyn DatasetFiles> = Arc::new(LocalFiles::new(dir.path()));
        let (files, skipped, fingerprint) =
            list(&*local, &FooterProgress::default(), &Meter::default()).unwrap();
        let listed = Arc::new(
            Listed::new(
                local,
                files,
                skipped,
                fingerprint,
                Arc::new(Meter::default()),
                Some(cache.clone()),
            )
            .unwrap(),
        );
        let n = listed.files.len();
        let counter = listed.counter(crate::formats::schema_union::FooterCount::new(
            n,
            (0..n).collect(),
            std::iter::empty(),
        ));
        let rows = counter(&fresh).map(|groups| groups.iter().flatten().sum::<usize>());
        (rows, reads.load(Ordering::SeqCst))
    };

    // Stopped after ten reads: nothing counted, what was read kept.
    stop_after.store(10, Ordering::SeqCst);
    let (rows, read) = count();
    assert!(rows.is_err(), "a stopped count is no count");
    assert!(read >= 10, "{read}");
    stop_after.store(usize::MAX, Ordering::SeqCst);
    let (rows, again) = count();
    assert_eq!(rows, Ok(300));
    assert_eq!(
        again,
        100 - read,
        "the footers the stopped count read are not read again"
    );

    let (rows, read) = count();
    assert_eq!((rows, read), (Ok(300), 0), "counted again, nothing is read");

    let mut bytes = Vec::new();
    ParquetWriter::new(&mut bytes)
        .finish(&mut df!("v" => [1i64, 2, 3, 4]).unwrap())
        .unwrap();
    fs::write(dir.path().join("part=0").join("f9999.parquet"), &bytes).unwrap();
    let (rows, read) = count();
    assert_eq!((rows, read), (Ok(304), 1), "a file added is the one read");
}

/// A random sample is the same for a seed, ascending, and every index when there
/// are no more than it asks; an estimate is the sample's mean times the files.
#[test]
fn a_seeded_sample_estimates_the_rows() {
    let a = crate::formats::schema_union::random_sample(842_225, 2_000, 7);
    assert_eq!(
        a,
        crate::formats::schema_union::random_sample(842_225, 2_000, 7)
    );
    assert_ne!(
        a,
        crate::formats::schema_union::random_sample(842_225, 2_000, 8)
    );
    assert_eq!(a.len(), 2_000);
    assert!(a.windows(2).all(|w| w[0] < w[1]) && *a.last().unwrap() < 842_225);
    assert_eq!(
        crate::formats::schema_union::random_sample(5, 2_000, 7),
        [0, 1, 2, 3, 4]
    );
    let footer = |rows: usize| {
        Some(FileFooter {
            schema: Arc::new(polars::prelude::Schema::default()),
            row_group_rows: vec![rows],
            row_group_bytes: Vec::new(),
            file_bytes: 0,
            column_bytes: Vec::new(),
        })
    };
    let estimate =
        crate::formats::schema_union::RowEstimate::of(1_000, &[footer(10), None, footer(30)])
            .unwrap();
    assert_eq!(
        (estimate.rows, estimate.sampled, estimate.files),
        (20_000, 2, 1_000)
    );
    assert!(crate::formats::schema_union::RowEstimate::of(10, &[None]).is_none());
}

/// Open `dir` as the app does, and wait for the pass behind the open.
fn open_whole(dir: &Path, cache: &crate::cache::CacheManager) -> Arc<FooterProgress> {
    let progress = Arc::new(FooterProgress::default());
    let report = OpenReport {
        progress: progress.clone(),
        meter: Arc::new(Meter::default()),
        remembered: Some(cache.clone()),
        writes: Default::default(),
    };
    let options = crate::OpenOptions {
        hive: true,
        ..crate::OpenOptions::default()
    };
    let (_, facts) = open(Arc::new(LocalFiles::new(dir)), &options, &report).unwrap();
    if let Some(join) = facts.footers_pending {
        join(&progress).expect("the pass reads the rest");
    }
    report.writes.settle();
    progress
}

/// A local open records what the home screen shows, as a cloud one does, under the
/// directory however it was named.
#[test]
fn a_local_open_records_what_the_home_screen_will_show() {
    let dir = tempfile::tempdir().unwrap();
    tree(dir.path(), 3);
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
    let _ = open_whole(dir.path(), &cache);

    let key = crate::canonical::canonicalize(dir.path()).unwrap();
    let facts = cache.dataset_facts(&key).expect("recorded");
    assert_eq!(facts.rows, Some(9));
    assert_eq!(facts.kind, Some(crate::home::discover::EntryKind::Hive));
    assert_eq!(facts.columns, ["part", "v"]);
    assert!(!facts.cols_sampled);
    assert!(
        facts.size > 0,
        "the size, from the files the footers came from"
    );
    assert_eq!(
        facts.mtime,
        LocalFiles::new(dir.path()).modified(&[]),
        "dated by the directory, which is what the home screen holds it to"
    );
}

/// The home screen measures a dataset past its footer budget from the shape an
/// open kept, reading no footer; one that changed since is sampled again.
#[test]
fn the_home_screen_measures_a_large_dataset_from_the_shape_an_open_kept() {
    use crate::home::discover::{Entry, EntryKind};
    let dir = tempfile::tempdir().unwrap();
    tree(dir.path(), 150);
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = crate::cache::CacheManager::with_dir(cache_dir.path().to_path_buf());
    let measure = |cache: Option<&crate::cache::CacheManager>| {
        let mut entry = Entry::directory(dir.path());
        entry.kind = EntryKind::Hive;
        crate::home::discover::enrich_with(
            &mut entry,
            &crate::formats::schema_union::ReadAs::default(),
            cache,
        );
        entry
    };
    assert_eq!(
        measure(Some(&cache)).rows,
        None,
        "past the budget, unopened"
    );

    let progress = open_whole(dir.path(), &cache);
    let estimate = progress
        .estimate()
        .expect("the pass estimates from its sample");
    assert_eq!(
        (estimate.rows, estimate.files),
        (450, 150),
        "every file, under the sample size"
    );
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = reads.clone();
    let _hook = crate::formats::schema_union::on_local_footer_read(dir.path(), move |_| {
        counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    let entry = measure(Some(&cache));
    assert_eq!(
        entry.rows,
        Some(450),
        "every file's rows, from the open's footers"
    );
    assert_eq!(
        entry.cols,
        Some(2),
        "the partition column and the file's own"
    );
    assert!(!entry.cols_sampled);
    assert_eq!(entry.cost.row_groups, Some(150));
    assert_eq!(
        reads.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "and no footer was read for it"
    );
    assert_eq!(
        measure(None).rows,
        None,
        "without the shape, a sample as before"
    );

    // A file added since: the listing no longer matches, so the shape is not used.
    tree(dir.path(), 151);
    assert_eq!(measure(Some(&cache)).rows, None);
}
