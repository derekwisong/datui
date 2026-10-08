use super::*;
use crate::home::Section;
use crate::home::discover::Entry;

/// [`preview_head_keyed`] with nothing below it to line up with.
fn preview_head(
    entry: &Entry,
    place_kind: Option<&'static str>,
    looking: Option<crate::home::CloudLook>,
    width: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    preview_head_keyed(entry, place_kind, looking, None, width, 0, ctx).0
}

/// Every value the pane offers as guidance.
fn guidance_notes() -> [&'static str; 8] {
    [
        FOOTER_UNREADABLE,
        INSIDE_AND_THE_DOOR,
        INSIDE_A_LAKE_TABLE,
        THE_DOOR,
        DOOR_OF_A_HIVE_TABLE,
        DOOR_OF_A_LAKE_TABLE,
        DOOR_OF_FILES_THAT_DIFFER,
        DOOR_OF_A_MIX,
    ]
}

/// The list as an entry row is drawn in, at `name_width`.
fn rows(ctx: &RenderContext, name_width: usize, show_meta: bool) -> ListDraw<'_> {
    ListDraw {
        ctx,
        width: name_width,
        name_width,
        show_meta,
        frame: 0,
    }
}

/// `[[cloud.connections]]` of these names.
fn connections(names: &[&str]) -> Vec<crate::config::CloudConnectionConfig> {
    names
        .iter()
        .map(|name| crate::config::CloudConnectionConfig {
            name: name.to_string(),
            ..Default::default()
        })
        .collect()
}

/// What a row is marked by for `filter`: the matched characters of `column` when the
/// row is listed by one, else of `name`.
fn marks_for(filter: &str, name: &str, column: Option<&str>) -> Vec<usize> {
    match column {
        Some(column) => crate::home::substring_positions(filter, column),
        None => crate::home::fuzzy_positions(filter, name),
    }
}

/// The list as a section header is drawn in, `width` wide.
fn header_row(ctx: &RenderContext, width: usize) -> ListDraw<'_> {
    rows(ctx, width, true)
}

/// The pane's guidance is a value, not a sentence: short, lower case, unpunctuated,
/// with no gap left by a wrapped literal (`cargo fmt` once joined a `\` continuation
/// back up and kept the indentation with it).
#[test]
fn the_pane_s_guidance_is_a_value_not_a_sentence() {
    for note in guidance_notes() {
        assert!(
            !note.contains("  "),
            "a run of spaces in the middle of {note:?}"
        );
        assert!(
            !note.ends_with('.') && !note.contains('\n') && !note.contains(';'),
            "a value, not a sentence: {note:?}"
        );
        assert!(
            note.chars().count() <= 40,
            "short enough for the pane: {note:?}"
        );
    }
}

fn row(path: &str, kind: crate::home::discover::EntryKind) -> Entry {
    Entry {
        path: std::path::PathBuf::from(path),
        kind,
        name: path.rsplit('/').next().unwrap_or(path).to_string(),
        size: None,
        modified: None,
        rows: None,
        cols: None,
        cols_sampled: false,
        columns: Vec::new(),
        cost: Default::default(),
        holds: Default::default(),
        opens_whole_directory: false,
        measured: false,
        format_spec: None,
        table: None,
    }
}

/// A row with nothing in the meta columns gives them to its name, and an empty
/// shape cell gives its columns too, size and age staying aligned; a relative path
/// keeps its leaf (#648).
#[test]
fn blank_meta_columns_go_to_the_name() {
    let ctx = RenderContext::for_test();
    let text = |entry: &Entry| -> String {
        entry_line(entry, false, EntryNotes::default(), &rows(&ctx, 26, true))
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect()
    };
    let mut public = row("/data/food_nutrition.csv", EntryKind::File);
    public.name = "Food nutrition (fast food)".to_string();
    assert!(text(&public).contains("Food nutrition (fast food)"));

    let mut sized = row("/data/chart_export_overwrite_test.csv", EntryKind::File);
    sized.size = Some(25);
    let mut shaped = sized.clone();
    shaped.rows = Some(3);
    shaped.cols = Some(2);
    let (sized, shaped) = (text(&sized), text(&shaped));
    assert!(
        sized.contains("chart_export_overwrite_test.csv"),
        "{sized:?}"
    );
    assert!(
        !shaped.contains("chart_export_overwrite_test.csv"),
        "{shaped:?}"
    );
    assert_eq!(
        sized.chars().count(),
        shaped.chars().count(),
        "the same width either way"
    );
    assert_eq!(
        sized.find("25 B").map(|at| sized[..at].chars().count()),
        shaped.find("25 B").map(|at| shaped[..at].chars().count()),
        "size stays in its column"
    );

    let mut hit = row(
        "/data/tests/sample-data/sales_by_region.csv",
        EntryKind::File,
    );
    hit.name = "tests/sample-data/sales_by_region.csv".to_string();
    hit.size = Some(25);
    hit.rows = Some(3);
    hit.cols = Some(2);
    assert!(text(&hit).contains("region.csv"), "{:?}", text(&hit));
}

/// A directory in a bucket reads like a local one: a spinner while it is looked
/// into, `…` before, and then what it holds, or `dir`. Never `prefix`, which said
/// nothing a user could act on. A bucket keeps its word.
#[test]
fn a_cloud_directory_is_labelled_by_what_it_holds() {
    let ctx = RenderContext::for_test();
    let drawn = |entry: &Entry, look: Option<crate::home::CloudLook>| -> String {
        entry_line(
            entry,
            false,
            EntryNotes {
                look,
                ..EntryNotes::default()
            },
            &rows(&ctx, 40, false),
        )
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect::<Vec<_>>()
        .join("")
    };
    let g = glyphs::get();
    let spinner = g.spinner[0];

    let mut directory = row("s3://bucket/exports", EntryKind::Directory);
    let text = drawn(&directory, Some(crate::home::CloudLook::Looking));
    assert!(text.contains(spinner), "{text}");
    assert!(!text.contains("dir") && !text.contains("prefix"), "{text}");
    let text = drawn(&directory, Some(crate::home::CloudLook::Waiting));
    assert!(text.contains(g.ellipsis), "{text}");

    // Looked into, and nothing counted: a directory of directories.
    let text = drawn(&directory, None);
    assert!(text.contains(" dir"), "{text}");
    assert!(!text.contains("prefix"), "{text}");

    directory.holds = crate::home::discover::Holds {
        formats: vec![("csv".to_string(), 12)],
        ..Default::default()
    };
    let text = drawn(&directory, None);
    assert!(text.contains("12 csv"), "{text}");

    let bucket = row("s3://bucket", EntryKind::Directory);
    assert!(drawn(&bucket, Some(crate::home::CloudLook::Waiting)).contains("bucket"));
    let container = row(
        "abfss://data@acct.dfs.core.windows.net/",
        EntryKind::Directory,
    );
    assert!(drawn(&container, Some(crate::home::CloudLook::Waiting)).contains("container"));
    let inside = row(
        "abfss://data@acct.dfs.core.windows.net/jolpica",
        EntryKind::Directory,
    );
    assert!(!drawn(&inside, None).contains("prefix"));
}

/// One grammar on every row: the name, a slash when it is a place to go into, two
/// cells, and what it is. Local, catalog and bucket rows alike, a directory of
/// directories counting them (#547 M7).
#[test]
fn every_row_reads_name_slash_two_spaces_label() {
    let ctx = RenderContext::for_test();
    let drawn = |entry: &Entry, place_kind: Option<&'static str>| -> String {
        entry_line(
            entry,
            false,
            EntryNotes {
                place_kind,
                ..EntryNotes::default()
            },
            &rows(&ctx, 60, true),
        )
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect::<Vec<_>>()
        .join("")
    };
    let mut local = row("/data/project/data", EntryKind::Directory);
    local.holds.directories = 3;
    let mut hive = row("/data/project/events", EntryKind::Hive);
    hive.holds.partitions = 2;
    let mut parquet = row("/data/project/processed", EntryKind::MultiFile);
    parquet.holds.formats = vec![("parquet".to_string(), 2)];
    let mut prefix = row("s3://bucket/exports", EntryKind::Directory);
    prefix.holds.formats = vec![("csv".to_string(), 12)];
    let bucket = row("s3://noaa-ghcn-pds", EntryKind::Directory);
    let mut noaa = row("s3://noaa-ghcn-pds/parquet/", EntryKind::Directory);
    noaa.name = "NOAA daily weather".to_string();
    let mut penguins = row("https://example.com/csv/penguins.csv", EntryKind::File);
    penguins.name = "Palmer penguins".to_string();
    for (entry, place_kind, expect) in [
        (&local, None, "data/  3 dirs"),
        (&hive, None, "events/  hive"),
        (&parquet, None, "processed/  2 parquet"),
        (&prefix, None, "exports/  12 csv"),
        (&bucket, None, "noaa-ghcn-pds/  bucket"),
        (&noaa, Some("dataset"), "NOAA daily weather/  dataset"),
        (&penguins, None, "Palmer penguins  csv"),
    ] {
        let text = drawn(entry, place_kind);
        assert!(text.contains(expect), "{expect:?} in {text:?}");
    }
}

/// The row that opens the directory being browsed has no label, and in a bucket a
/// directory of Parquet files is a kind that wears its label as a chip. The chip is
/// drawn by taking the cell apart again, so a chip made of nothing indexed past the
/// end of an empty string and brought the renderer down — on a real GCS prefix,
/// where CI found it and no test here had put the two together.
#[test]
fn the_row_that_opens_a_directory_draws_without_a_label() {
    let ctx = RenderContext::for_test();
    let mut drawn: Vec<(EntryKind, String)> = Vec::new();
    for kind in [EntryKind::MultiFile, EntryKind::Hive, EntryKind::Directory] {
        let mut door = row("gs://cloud-samples-data/bigquery/us-states", kind);
        door.name = "us-states (all files)".to_string();
        door.opens_whole_directory = true;
        door.holds = crate::home::discover::Holds {
            formats: vec![("parquet".to_string(), 9)],
            ..Default::default()
        };
        assert_eq!(door.label(), "", "{kind:?}");

        // Every width, because the crash was in taking the cell apart and the
        // widths are where the cell is rewritten. Nothing here asserts a shape:
        // drawing at all is the thing that was not happening.
        for width in 1..=60usize {
            let line = entry_line(
                &door,
                false,
                EntryNotes::default(),
                &rows(&ctx, width, true),
            );
            let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
            assert!(!text.is_empty(), "at {width} cells, {kind:?}");
        }
        // And with room to spare it reads as itself, with no label beside it.
        let line = entry_line(&door, false, EntryNotes::default(), &rows(&ctx, 40, true));
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(text.contains("us-states (all files)"), "{kind:?}: {text:?}");
        assert!(!text.contains("parquet"), "{kind:?}: {text:?}");
        drawn.push((kind, text));
    }

    // Nor does a curated place word or a source id stand in for the label it does
    // not have. Both are about the directory; this row is the door into it, and
    // `bigquery (all files)  dataset` says the door is the dataset.
    let mut door = row("s3://lab@bucket/exports", EntryKind::Directory);
    door.name = "exports (all files)".to_string();
    door.opens_whole_directory = true;
    let known = connections(&["lab"]);
    let text: String = entry_line(
        &door,
        false,
        EntryNotes {
            known_sources: Some(&known),
            place_kind: Some("dataset"),
            ..EntryNotes::default()
        },
        &rows(&ctx, 60, true),
    )
    .spans
    .iter()
    .map(|s| s.content.as_ref())
    .collect();
    assert!(text.contains("exports (all files)"), "{text:?}");
    assert!(
        !text.contains("dataset"),
        "curated word on a door: {text:?}"
    );
    assert!(!text.contains("lab"), "source id on a door: {text:?}");

    // And the pane beside it says the same nothing. It takes the place word through
    // a second match of its own, which is how the row and the pane come to disagree
    // about one directory.
    let mut door = row("s3://bucket/warehouse", EntryKind::Directory);
    door.name = "warehouse (all files)".to_string();
    door.opens_whole_directory = true;
    let pane = preview_text(&door, 60);
    assert!(pane.contains("warehouse (all files)"), "{pane}");
    assert!(
        !pane.lines().any(|l| l.trim_start().starts_with("kind")),
        "a kind stood in for the label it does not have: {pane}"
    );

    // And all three draw the same row. A hive or multi-file kind wears its label as
    // a chip, which is a space of the row's own background and then the label — so
    // with no label, a kind that would have worn one leaves the space behind and
    // the row sits one cell right of every other door.
    let (_, first) = &drawn[0];
    for (kind, text) in &drawn[1..] {
        assert_eq!(first, text, "{kind:?} drew a different row");
    }
}

/// A row count that is out of reach says `?`. A directory that is not one table has
/// none to be out of reach, and must not read like a dataset too big to count.
#[test]
fn a_directory_of_separate_tables_shows_its_width_without_a_question_mark() {
    let mut directory = row("/data/consolidated", EntryKind::Directory);
    directory.cols = Some(72);
    let shape = meta_columns(&directory, false, None, None);
    assert!(shape.contains("72 cols"), "{shape}");
    assert!(!shape.contains('?'), "{shape}");
    assert!(
        !shape.contains(glyphs::get().times),
        "an operator with nothing on its left: {shape}"
    );

    let mut big = row("/data/events", EntryKind::Hive);
    big.cols = Some(72);
    assert!(
        meta_columns(&big, false, None, None).contains('?'),
        "a hive dataset still says ?"
    );
}

/// A schema line wraps too, so the budget counts the rows they take rather than
/// one per column. Taking a column per row draws the tail past the bottom of the
/// pane and sends the `… N more` line over the edge with it — the loss the count
/// exists to report.
#[test]
fn a_wrapping_schema_line_is_paid_for_at_its_real_height() {
    use polars::prelude::DataType;
    let ctx = RenderContext::for_test();
    // Forty cells is what the pane has; this type alone is past it.
    let long = (0..6).fold(DataType::Int64, |inner, _| DataType::List(Box::new(inner)));
    assert!(
        format!("{long}").chars().count() > 18,
        "a type past the pane's forty cells beside a name: {long}"
    );
    let schema: Vec<(String, DataType)> = (0..10)
        .map(|i| (format!("created_at_{i}"), long.clone()))
        .collect();

    let (shown, lines) = schema_lines(&schema, 22, 40, 6, &ctx);
    let rows: usize = lines.iter().map(|l| wrapped_rows(l, 40)).sum();
    assert!(rows <= 6, "{rows} rows drawn into six: {shown} columns");
    assert!(shown < 6, "each column takes more than one row: {shown}");
    assert!(shown > 0, "and at least one still fits");
}

/// The pane word-wraps, so a row count taken by dividing the width into the length
/// is a floor: a word that will not fit goes whole to the next row. The schema list
/// is drawn in what is left, and under-counting drew its tail past the bottom while
/// `… N more` reported nothing lost.
#[test]
fn a_wrapped_line_is_counted_by_the_rows_it_takes() {
    let line = |text: &str| Line::from(vec![Span::raw(text.to_string())]);
    assert_eq!(wrapped_rows(&line("short"), 40), 1);
    // Five four-letter words at a width of six. Dividing twenty-four characters
    // into six says four rows; each row can hold one word and the two cells left
    // beside it are cells no word can use, so it takes five.
    assert_eq!(
        wrapped_rows(&line("aaaa aaaa aaaa aaaa aaaa"), 6),
        5,
        "the room left at the end of a row is room a word cannot use"
    );
    // A single word longer than the pane wraps inside itself.
    assert_eq!(wrapped_rows(&line(&"x".repeat(25)), 10), 3);
    // Cells, not characters. Three glyphs of two cells each do not fit in five,
    // and counting characters would say they do — a skipped file can be named in
    // a script where that is true of every letter.
    assert_eq!(
        wrapped_rows(&line("漢字漢"), 5),
        2,
        "three characters, six cells"
    );
}

/// A label describes and a name identifies, so on a screen too narrow for both the
/// label goes. Before this a `5000+ parquet` chip left the name nothing to be
/// truncated into and shoved the size and modified columns out of alignment.
#[test]
fn a_label_gives_way_to_the_name_on_a_narrow_screen() {
    let ctx = RenderContext::for_test();
    let mut entry = row("/data/exports", crate::home::discover::EntryKind::MultiFile);
    // Real metadata, so the meta columns are a string the offset can be found by.
    entry.size = Some(4096);
    entry.rows = Some(12);
    // A shape too: a row with an empty shape cell gives it to the name (#648).
    entry.cols = Some(3);
    entry.holds = crate::home::discover::Holds {
        formats: vec![("parquet".to_string(), 5000)],
        truncated: true,
        ..Default::default()
    };
    assert_eq!(entry.label(), "5000+ parquet");

    // Where the meta columns begin: everything drawn before them. It must not
    // depend on how long a row's label is, or the columns stop lining up.
    let offset_of_meta = |line: Line<'_>, entry: &Entry| -> usize {
        let meta = meta_columns(entry, false, None, None);
        let at = line
            .spans
            .iter()
            .position(|s| s.content == meta)
            .expect("the meta columns are drawn");
        line.spans[..at]
            .iter()
            .map(|s| s.content.chars().count())
            .sum()
    };
    let meta_starts_at = |entry: &Entry, width: usize| -> usize {
        let line = entry_line(
            entry,
            false,
            EntryNotes::default(),
            &rows(&ctx, width, true),
        );
        offset_of_meta(line, entry)
    };

    // The same kind, so only the label's length differs: a `Directory` row carries
    // a trailing slash and would be a character wider for a reason of its own.
    let mut short = row("/data/exports", crate::home::discover::EntryKind::MultiFile);
    short.size = Some(4096);
    short.rows = Some(12);
    short.cols = Some(3);
    short.holds = crate::home::discover::Holds {
        formats: vec![("csv".to_string(), 2)],
        ..Default::default()
    };
    assert_eq!(short.label(), "2 csv");

    for width in [22usize, 24, 30, 48, 100] {
        assert_eq!(
            meta_starts_at(&entry, width),
            meta_starts_at(&short, width),
            "the meta columns must start in the same place at {width}"
        );
    }

    // The curated word is not a label to give up. `dataset` and `project` are what
    // a source calls a place it names, and the only thing marking a curated row.
    let mut named = row("s3://bucket/occurrence", EntryKind::Directory);
    named.size = Some(4096);
    named.holds = crate::home::discover::Holds {
        formats: vec![("parquet".to_string(), 5000)],
        truncated: true,
        ..Default::default()
    };
    let curated = |width: usize| -> String {
        entry_line(
            &named,
            false,
            EntryNotes {
                place_kind: Some("dataset"),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        )
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect::<Vec<_>>()
        .join("")
    };
    // Narrow enough that the guard fires: the word is eight cells with its space,
    // so anything at or under fourteen leaves the name nothing to be cut into.
    for width in [10usize, 12, 14, 22, 30] {
        assert!(
            curated(width).contains("dataset"),
            "at {width}: {}",
            curated(width)
        );
    }

    // And a row whose *path* is curated but whose kind never reaches for the word
    // is carrying a label, which gives way like any other. Only a `Directory` row
    // consults `place_kind`; a `multi` prefix in a catalog does not.
    let mut curated_multi = row("s3://bucket/occurrence", EntryKind::MultiFile);
    curated_multi.size = Some(4096);
    curated_multi.rows = Some(12);
    curated_multi.cols = Some(3);
    curated_multi.holds = crate::home::discover::Holds {
        formats: vec![("parquet".to_string(), 5000)],
        truncated: true,
        ..Default::default()
    };
    let with_curated_path = |width: usize| -> usize {
        let line = entry_line(
            &curated_multi,
            false,
            EntryNotes {
                place_kind: Some("dataset"),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        );
        offset_of_meta(line, &curated_multi)
    };
    for width in [20usize, 22, 24, 30] {
        assert_eq!(
            with_curated_path(width),
            meta_starts_at(&short, width),
            "a count under a curated path still gives way at {width}"
        );
    }

    // A cell the guard may not drop is cut instead. A `source not found:` is as
    // long as somebody's configuration, and a name with nothing left to be cut
    // into is the misalignment all of this is for.
    let mut gone = row("s3://averylongsourcename@bucket/exports", EntryKind::File);
    gone.size = Some(4096);
    gone.rows = Some(12);
    gone.cols = Some(3);
    let known = connections(&["other"]);
    let missing = |width: usize| -> usize {
        let line = entry_line(
            &gone,
            false,
            EntryNotes {
                known_sources: Some(&known),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        );
        offset_of_meta(line, &gone)
    };
    for width in [22usize, 24, 30, 48] {
        assert_eq!(
            missing(width),
            meta_starts_at(&short, width),
            "a warning too long for the row is cut, not left to push the columns \
             out, at {width}"
        );
    }

    // An uncut note keeps the mark on its last letter. The subtraction that makes
    // room for the ellipsis applies only where there is one.
    let marks = |width: usize| -> Vec<String> {
        entry_line(
            &short,
            false,
            EntryNotes {
                matched_column: Some("amount"),
                marks: &marks_for("amount", "", Some("amount")),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        )
        .spans
        .iter()
        .filter(|s| s.style.add_modifier.contains(Modifier::UNDERLINED))
        .map(|s| s.content.to_string())
        .collect()
    };
    assert_eq!(
        marks(200).join(""),
        "amount",
        "every letter of the match is marked when the note is whole"
    );

    // A label under a named source is still a label. The path carrying a source id
    // is a different question from the cell showing one, and a peeked prefix under
    // `s3://prod@bucket` has both.
    let mut named = row("s3://prod@bucket/exports", EntryKind::MultiFile);
    named.size = Some(4096);
    named.rows = Some(12);
    named.cols = Some(3);
    named.holds = crate::home::discover::Holds {
        formats: vec![("parquet".to_string(), 12000)],
        ..Default::default()
    };
    let sources = connections(&["prod"]);
    let offset = |width: usize| -> usize {
        let line = entry_line(
            &named,
            false,
            EntryNotes {
                known_sources: Some(&sources),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        );
        offset_of_meta(line, &named)
    };
    for width in [22usize, 24, 30] {
        assert_eq!(
            offset(width),
            meta_starts_at(&short, width),
            "a label under a named source gives way like any other, at {width}"
        );
    }

    // A column note is not a label: it says why the row is in the list, and the
    // draw site writes it from `matched_column` rather than from the cell, so
    // blanking the cell would hand the name a budget the note then overruns.
    let noted = |entry: &Entry, width: usize| -> usize {
        let line = entry_line(
            entry,
            false,
            EntryNotes {
                matched_column: Some("transaction_amount"),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        );
        offset_of_meta(line, entry)
    };
    for width in [20usize, 24, 30, 48] {
        assert_eq!(
            noted(&entry, width),
            noted(&short, width),
            "a matched row is drawn by its note, whatever its label would say, \
             at {width}"
        );
        // And the note itself is cut to fit, not left to push the columns out.
        assert_eq!(
            noted(&short, width),
            meta_starts_at(&short, width),
            "a column note too long for the row is cut, at {width}"
        );
    }
}

/// A dot in a directory's name does not make it a file. A local row nothing has
/// looked into came from a listing that saw a directory, so the name is not the
/// evidence.
/// Launched where there is nothing to open, the directory's heading says so and
/// points at `~`, rather than a bare `0` above the public rows (#547 D9).
#[test]
fn an_empty_current_directory_says_so_and_points_at_the_path_prompt() {
    let ctx = RenderContext::for_test();
    let section = Section {
        origin: Some(crate::home::RootOrigin::Cwd.note()),
        ..Section::titled("/home/me/empty", Vec::new())
    };
    let text = |matches: usize, section: &Section| -> String {
        section_header(section, matches, false, false, &header_row(&ctx, 80))
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect()
    };
    let empty = text(0, &section);
    assert!(empty.contains("nothing to open here"), "{empty:?}");
    assert!(empty.contains("~ types a path"), "{empty:?}");
    assert!(!text(3, &section).contains("nothing to open"));
    let configured = Section {
        origin: Some("configured"),
        ..section
    };
    assert!(!text(0, &configured).contains("nothing to open"));
}

#[test]
fn the_origin_chip_sits_by_the_count_and_the_state_by_the_rule() {
    let ctx = RenderContext::for_test();
    let section = Section {
        subtitle: Some("nfs4".to_string()),
        origin: Some("configured"),
        ..Section::titled("/mnt/data", Vec::new())
    };
    let text = |width: usize| -> String {
        section_header(&section, 12, false, false, &header_row(&ctx, width))
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect()
    };
    let wide = text(80);
    let count = wide.find(" 12 ").expect("count chip");
    let origin = wide.find(" configured ").expect("origin chip");
    let state = wide.find("nfs4").expect("state note");
    assert!(count < origin && origin < state, "{wide:?}");
    assert_eq!(wide.chars().count(), 80);

    // At forty columns both chips and the note still fit beside the title.
    let narrow = text(40);
    assert_eq!(narrow.chars().count(), 40, "{narrow:?}");
    assert!(
        narrow.contains(" configured ") && narrow.contains("nfs4"),
        "{narrow:?}"
    );
    assert!(narrow.contains("/mnt/data"), "{narrow:?}");

    // Squeezed further: the title keeps at least three cells while the note and
    // then the chip give way, and once both are gone the whole title is back.
    for width in [30usize, 24, 16] {
        let tight = text(width);
        assert_eq!(tight.chars().count(), width, "{tight:?}");
        assert!(tight.contains("ta"), "{width}: {tight:?}");
    }
    let bare = text(20);
    assert!(bare.contains("/mnt/data"), "{bare:?}");
    assert!(
        !bare.contains("configured") && !bare.contains("nfs4"),
        "{bare:?}"
    );
}

/// A path longer than the line still leaves the rule a cell, so the cursor on the
/// heading shows (#575).
#[test]
fn a_long_path_leaves_the_focused_heading_its_rule() {
    let ctx = RenderContext::for_test();
    let g = glyphs::get();
    let section = Section {
        origin: Some("configured"),
        ..Section::titled(format!("/var/folders/{}", "x".repeat(120)), Vec::new())
    };
    for width in [40usize, 80, 120] {
        let text: String = section_header(&section, 60, false, true, &header_row(&ctx, width))
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect();
        assert_eq!(text.chars().count(), width, "{text:?}");
        assert!(text.contains(g.rule_h_focused), "{width}: {text:?}");
    }
}

#[test]
fn an_unmeasured_recent_from_a_store_shows_an_ellipsis_for_its_shape() {
    let ctx = RenderContext::for_test();
    let g = glyphs::get();
    let mut entry = row("s3://bucket/sales/", EntryKind::MultiFile);
    entry.cost.source = Some("s3".to_string());
    let text = |entry: &Entry, indent: usize| -> String {
        entry_line(
            entry,
            false,
            EntryNotes {
                indent,
                ..EntryNotes::default()
            },
            &rows(&ctx, 60, true),
        )
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect()
    };
    // Under a place, with nothing measured: the shape cell is an admission.
    let meta = meta_columns(&entry, true, None, None);
    assert!(text(&entry, NEST_INDENT).contains(&meta));
    assert!(meta.contains(g.ellipsis), "{meta:?}");
    // The same row in a directory listing, where the probe measures it, and a
    // local row, which is measured in place: blank, as before.
    assert!(!text(&entry, 0).contains(g.ellipsis));
    let mut local = row("/data/sales.csv", EntryKind::File);
    local.cost.source = Some("ext4".to_string());
    local.size = Some(10);
    assert!(!text(&local, NEST_INDENT).contains(g.ellipsis));
    // Once measured, the shape.
    entry.rows = Some(20);
    entry.cols = Some(3);
    assert!(!text(&entry, NEST_INDENT).contains(g.ellipsis));
}

#[test]
fn a_place_row_carries_the_label_the_index_remembers_and_drops_it_before_the_path() {
    let ctx = RenderContext::for_test();
    let text = |label: Option<&str>, name_width: usize| -> String {
        place_line(
            std::path::Path::new("/mnt/data/sets/bitcoin"),
            label,
            Some("nfs4"),
            false,
            name_width,
            false,
            &ctx,
        )
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect()
    };
    let wide = text(Some("2 parquet"), 60);
    assert!(wide.contains("bitcoin/  2 parquet"), "{wide:?}");
    assert!(wide.trim_end().ends_with("nfs4"), "{wide:?}");
    assert_eq!(wide.chars().count(), row_width(60, false));
    // No room for both: the path stays, the label goes.
    let tight = text(Some("2 parquet"), 22);
    assert!(!tight.contains("parquet"), "{tight:?}");
    assert!(tight.contains("bitcoin/"), "{tight:?}");
    assert_eq!(tight.chars().count(), row_width(22, false));
}

#[test]
fn a_nested_row_keeps_the_meta_columns_where_the_place_row_ends() {
    // A row two cells in under a place must not push its meta columns two cells
    // to the right, or the column of shapes and sizes zigzags down RECENT. The
    // indent comes off the name's side, and the narrow-screen rule that gives a
    // label up for the name fires that much sooner.
    let ctx = RenderContext::for_test();
    let mut entry = row("/data/exports", EntryKind::MultiFile);
    entry.size = Some(4096);
    entry.rows = Some(12);
    // A shape too: a row with an empty shape cell gives it to the name (#648).
    entry.cols = Some(3);
    entry.holds = crate::home::discover::Holds {
        formats: vec![("parquet".to_string(), 5000)],
        truncated: true,
        ..Default::default()
    };
    let meta = meta_columns(&entry, false, None, None);
    let meta_at = |indent: usize, width: usize| -> usize {
        let line = entry_line(
            &entry,
            false,
            EntryNotes {
                indent,
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        );
        let at = line
            .spans
            .iter()
            .position(|s| s.content == meta)
            .expect("the meta columns are drawn");
        line.spans[..at]
            .iter()
            .map(|s| s.content.chars().count())
            .sum()
    };
    for width in [24usize, 30, 48, 100] {
        assert_eq!(
            meta_at(NEST_INDENT, width),
            meta_at(0, width),
            "at {width} the indented row's meta columns start where a plain row's do"
        );
    }
    // And the place row above it ends where the entry row ends, so its right-hand
    // cell sits on the edge the age column sits on. `row_width` names the entry
    // row's width rather than measuring it, and this is what keeps the two honest.
    assert_eq!(meta.chars().count(), META_COLUMNS_WIDTH);
    let drawn =
        |line: Line<'_>| -> usize { line.spans.iter().map(|s| s.content.chars().count()).sum() };
    // With the meta columns shown, which is when there is an edge to share. Without
    // them an entry row stops after its label, and the place row fills its width
    // alone so the selection tint reaches the edge.
    for name_width in [60usize, 24] {
        let entry_width = drawn(entry_line(
            &entry,
            false,
            EntryNotes {
                indent: NEST_INDENT,
                ..EntryNotes::default()
            },
            &rows(&ctx, name_width, true),
        ));
        assert_eq!(entry_width, row_width(name_width, true));
        let place = place_line(
            std::path::Path::new("/data"),
            None,
            Some("nfs4"),
            false,
            name_width,
            true,
            &ctx,
        );
        assert_eq!(drawn(place), entry_width, "{name_width}");
        let more = more_line(3, 2, false, false, name_width, true, &ctx);
        assert_eq!(drawn(more), entry_width, "{name_width}");
    }
}

#[test]
fn a_place_row_names_only_a_filesystem_worth_naming() {
    // A local disk is the overwhelming majority of places, and `ext4` beside
    // every one would be texture. A share or a store is worth a word, in the
    // color the rows' own glyph uses for it.
    let ctx = RenderContext::for_test();
    let text = |source: Option<&str>| -> String {
        place_line(
            std::path::Path::new("/data/x"),
            None,
            source,
            false,
            40,
            false,
            &ctx,
        )
        .spans
        .iter()
        .map(|s| s.content.to_string())
        .collect::<String>()
    };
    assert!(
        text(Some("ext4")).trim_end().ends_with("/data/x/"),
        "{:?}",
        text(Some("ext4"))
    );
    assert!(
        text(Some("nfs4")).trim_end().ends_with("nfs4"),
        "{:?}",
        text(Some("nfs4"))
    );
    assert!(
        text(Some("s3")).trim_end().ends_with("s3"),
        "{:?}",
        text(Some("s3"))
    );
    assert!(
        text(None).trim_end().ends_with("/data/x/"),
        "{:?}",
        text(None)
    );
    // A long path keeps its tail, which is the part that says where it is.
    let long = place_line(
        std::path::Path::new("/very/deeply/nested/place/on/a/share/somewhere/far"),
        None,
        Some("nfs4"),
        false,
        27,
        false,
        &ctx,
    );
    let drawn: String = long.spans.iter().map(|s| s.content.to_string()).collect();
    assert_eq!(drawn.chars().count(), row_width(27, false), "{drawn:?}");
    assert!(drawn.contains("far/"), "{drawn:?}");
}

#[test]
fn the_more_row_counts_rows_and_places() {
    let ctx = RenderContext::for_test();
    let text = |hidden: usize, places: usize, measuring: bool| -> String {
        more_line(hidden, places, measuring, false, 40, false, &ctx)
            .spans
            .iter()
            .map(|s| s.content.to_string())
            .collect::<String>()
            .trim_end()
            .to_string()
    };
    let g = glyphs::get();
    assert_eq!(
        text(13, 5, false),
        format!("{}{} 13 more in 5 places", g.selector_blank, g.ellipsis)
    );
    assert_eq!(
        text(1, 1, false),
        format!("{}{} 1 more in 1 place", g.selector_blank, g.ellipsis)
    );
    // A directory's: files and directories both, so no noun.
    assert_eq!(
        text(4958, 0, false),
        format!("{}{} 4,958 more", g.selector_blank, g.ellipsis)
    );
    assert_eq!(
        text(4958, 0, true),
        format!(
            "{}{} 4,958 more {} measuring",
            g.selector_blank, g.ellipsis, g.middot
        )
    );
}

#[test]
fn a_local_directory_keeps_its_slash_however_its_name_is_spelled() {
    for name in ["project.old", "v1.2", "site.com", "datui.git", "plain"] {
        let entry = row(&format!("/home/derek/{name}"), EntryKind::Unknown);
        assert!(
            shows_as_a_place(&entry),
            "{name} is a directory the listing saw"
        );
    }
}

/// Remote, the name is all there is. One with an extension is a file even when it
/// is one datui does not read, and one without is a prefix.
#[test]
fn a_remote_row_is_read_as_a_prefix_only_when_its_name_is_not_a_file() {
    // A path on a network mount takes the same branch, but whether one *is* a
    // network mount is a question about this machine's mount table, so the two
    // URL schemes are what a test can say.
    for prefix in ["s3://bucket", "https://host"] {
        for name in ["export.txt", "part-00000.parquet", "notes.md"] {
            assert!(
                !shows_as_a_place(&row(&format!("{prefix}/{name}"), EntryKind::Unknown)),
                "{prefix}/{name} is named like a file"
            );
        }
    }
    for prefix in ["s3://bucket", "https://host"] {
        for name in ["exports", "2024", "raw"] {
            assert!(
                shows_as_a_place(&row(&format!("{prefix}/{name}"), EntryKind::Unknown)),
                "{prefix}/{name} is a prefix"
            );
        }
    }
}

fn header_width(section: &Section, width: usize) -> usize {
    let ctx = RenderContext::for_test();
    let line = section_header(section, 3, false, false, &header_row(&ctx, width));
    line.spans.iter().map(|s| s.content.chars().count()).sum()
}

#[test]
fn a_long_note_never_pushes_the_header_past_the_screen() {
    // A search heading carries the path it searched, which is easily longer than
    // the terminal. The note is context; the title is what the section is.
    let section = Section {
        subtitle: Some(
            "/very/deeply/nested/path/that/goes/on/and/on/for/quite/a/while · 99999 searched"
                .to_string(),
        ),
        ..Section::titled("Found", Vec::new())
    };

    for width in [20usize, 40, 80, 120] {
        let rendered = header_width(&section, width);
        assert!(
            rendered <= width,
            "a {width}-wide screen produced a {rendered}-character header"
        );
    }
}

#[test]
fn a_recent_from_a_source_names_it_or_says_it_is_gone() {
    let ctx = RenderContext::for_test();
    let path = std::path::Path::new("s3://lab@data/sales.parquet");
    let mut entry = Entry::for_test(path, "sales.parquet");
    entry.kind = EntryKind::Unknown;
    let text = |known: Option<&[crate::config::CloudConnectionConfig]>| -> String {
        entry_line(
            &entry,
            false,
            EntryNotes {
                known_sources: known,
                ..EntryNotes::default()
            },
            &rows(&ctx, 80, false),
        )
        .spans
        .iter()
        .map(|s| s.content.to_string())
        .collect()
    };
    let lab = connections(&["lab"]);
    assert!(text(Some(&lab)).contains("sales.parquet  lab"));
    assert!(text(Some(&[])).contains("source not found: lab"));
    // Inside a source the trail already says which, so nothing is added.
    assert!(!text(None).contains("lab"));
}

/// Where the meta columns begin, in cells. Everything left of them is the name
/// half of the row, and it is one width for every row on screen or the columns are
/// not columns.
fn meta_offset(line: &Line) -> usize {
    // The meta text is second from the end; the last span carries the tint to the
    // edge.
    let spans = &line.spans[..line.spans.len().saturating_sub(2)];
    spans
        .iter()
        .map(|s| unicode_width::UnicodeWidthStr::width(s.content.as_ref()))
        .sum()
}

#[test]
fn the_pane_does_not_say_what_a_directory_holds_twice() {
    let mut entry = Entry::for_test(std::path::Path::new("/data/consolidated"), "consolidated");
    entry.kind = EntryKind::Directory;
    // A directory of one format and nothing else: the label and the line are the same
    // words, and `kind  12 parquet` above `holds  12 parquet` says it twice.
    entry.holds = crate::home::discover::Holds {
        formats: vec![("parquet".to_string(), 12)],
        ..Default::default()
    };
    let text = preview_text(&entry, 60);
    assert!(text.contains("12 parquet"), "{text}");
    assert_eq!(
        text.matches("12 parquet").count(),
        1,
        "the label and the line are the same words: {text}"
    );

    // With anything else beside them the line carries that too.
    entry.holds.directories = 3;
    let text = preview_text(&entry, 60);
    assert!(text.contains("contains"), "{text}");
    assert!(text.contains("3 directories"), "{text}");

    // Files datui cannot open are not counted here: inside, a row says so.
    entry.holds = crate::home::discover::Holds {
        not_read: 10,
        ..Default::default()
    };
    let text = preview_text(&entry, 60);
    assert!(!text.contains("10"), "{text}");
    assert!(text.contains("no data files"), "{text}");
}

#[test]
fn no_mark_ever_lands_on_a_cut_notes_ellipsis() {
    // The note says why a row is in the list, and the marks say which letters
    // matched. The ellipsis standing for the rest of the note matched nothing, so a
    // mark on it is the row claiming a hit it does not have. `kept` counts the
    // note's own characters and the marks stop there — this is what says so, since
    // the comparison between the two rows below cannot see a change that moves
    // both of them.
    let ctx = RenderContext::for_test();
    let entry = Entry::for_test(std::path::Path::new("/tmp/x"), "x");
    let g = glyphs::get();
    let marks = marks_for("usd", "", Some("transaction_amount_usd"));
    for width in 1..=40usize {
        let line = entry_line(
            &entry,
            false,
            EntryNotes {
                matched_column: Some("transaction_amount_usd"),
                marks: &marks,
                known_sources: Some(&[]),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, false),
        );
        let marked: String = line
            .spans
            .iter()
            .filter(|s| s.style.add_modifier.contains(Modifier::UNDERLINED))
            .map(|s| s.content.to_string())
            .collect();
        assert!(
            !marked.contains(g.ellipsis),
            "at {width} cells a mark landed on the cut note's ellipsis: {marked:?}"
        );
        // And a note cut down to nothing but the ellipsis is not a note. Where
        // there is no room for a letter beside it the whole note stays and the name
        // gives way instead, the same reasoning that keeps `dataset` from becoming
        // `d…t`.
        let sep = format!(" {}", g.middot);
        let note: String = line
            .spans
            .iter()
            .skip_while(|s| s.content != sep)
            .skip(1)
            .map(|s| s.content.to_string())
            .collect();
        assert_ne!(
            note.trim_end(),
            g.ellipsis,
            "at {width} cells the note was cut down to an ellipsis and said nothing"
        );
        // A note that fits is not cut. The cut one is always shorter than the
        // whole, so a drawn note as long as the column can only be the column
        // itself — which is what catches a boundary set one cell the wrong way,
        // where the ellipsis replaces the last letter and changes nothing else.
        let note = note.trim_end();
        if note.chars().count() >= "transaction_amount_usd".chars().count() {
            assert_eq!(
                note, "transaction_amount_usd",
                "at {width} cells a note that fitted was cut anyway"
            );
        }
    }
}

#[test]
fn a_column_note_on_a_row_from_a_source_leaves_the_meta_columns_alone() {
    let ctx = RenderContext::for_test();
    let known = connections(&["prod"]);
    // The same row in every way but the source id, so the only thing that can
    // move the meta columns is the cell the id goes in.
    let mut plain = Entry::for_test(
        std::path::Path::new("s3://bucket/sales.parquet"),
        "sales.parquet",
    );
    plain.kind = EntryKind::Unknown;
    let mut sourced = Entry::for_test(
        std::path::Path::new("s3://prod@bucket/sales.parquet"),
        "sales.parquet",
    );
    sourced.kind = EntryKind::Unknown;

    // Two cells hold this row's note: the source id, which is as long as somebody's
    // configuration and so is cut when it stops fitting, and the column that put
    // the row in the list. Only one of them is drawn. Cutting the other moved the
    // meta columns of this row and no other — the columns coming unstuck on the one
    // row a search was about.
    let marks = marks_for("cust", "", Some("customer_identifier"));
    for width in 6..=30usize {
        let with_note = entry_line(
            &sourced,
            false,
            EntryNotes {
                matched_column: Some("customer_identifier"),
                marks: &marks,
                known_sources: Some(&known),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        );
        let without = entry_line(
            &plain,
            false,
            EntryNotes {
                matched_column: Some("customer_identifier"),
                marks: &marks,
                known_sources: Some(&[]),
                ..EntryNotes::default()
            },
            &rows(&ctx, width, true),
        );
        assert_eq!(
            meta_offset(&with_note),
            meta_offset(&without),
            "at {width} cells the source id moved the meta columns"
        );
    }
}

/// The row's spans, as (text, is_highlighted) pairs.
fn row_spans(name: &str, filter: &str, column: Option<&str>) -> Vec<(String, bool)> {
    let ctx = RenderContext::for_test();
    let entry = Entry::for_test(std::path::Path::new("/tmp/x"), name);
    let marks = marks_for(filter, name, column);
    let line = entry_line(
        &entry,
        false,
        EntryNotes {
            matched_column: column,
            marks: &marks,
            known_sources: Some(&[]),
            ..EntryNotes::default()
        },
        &rows(&ctx, 60, false),
    );
    line.spans
        .iter()
        .skip(1) // the selection marker
        .map(|s| {
            (
                s.content.to_string(),
                s.style.add_modifier.contains(Modifier::UNDERLINED),
            )
        })
        .collect()
}

fn highlighted_text(spans: &[(String, bool)]) -> String {
    spans
        .iter()
        .filter(|(_, hit)| *hit)
        .map(|(t, _)| t.as_str())
        .collect()
}

fn preview_text(entry: &Entry, width: usize) -> String {
    let ctx = RenderContext::for_test();
    preview_head(entry, None, None, width, &ctx)
        .iter()
        .map(|l| {
            l.spans
                .iter()
                .map(|s| s.content.as_ref())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn costed(name: &str, cost: crate::home::discover::Cost, size: Option<u64>) -> Entry {
    let mut e = Entry::for_test(std::path::Path::new("/tmp/x"), name);
    e.size = size;
    e.cost = cost;
    e
}

/// A file of tables says how many it contains; a spec's file does not, whose
/// variants are record types its `records` line counts.
#[test]
fn only_a_file_of_real_tables_contains_tables() {
    let db = costed(
        "shop.db",
        crate::home::discover::Cost {
            tables: Some(3),
            ..Default::default()
        },
        None,
    );
    let shown = preview_text(&db, 60);
    assert!(
        shown
            .lines()
            .any(|l| l.starts_with("contains") && l.ends_with(" 3 tables")),
        "{shown}"
    );
    let mut spec_file = costed(
        "day.ord",
        crate::home::discover::Cost {
            tables: Some(5),
            ..Default::default()
        },
        None,
    );
    spec_file.format_spec = Some("acme.orders".into());
    let shown = preview_text(&spec_file, 60);
    assert!(!shown.contains("tables"), "{shown}");
    assert!(shown.contains("acme.orders file"), "{shown}");
}

#[test]
fn a_network_source_is_named_rather_than_described() {
    // The filesystem's own name and nothing else. A sentence explaining that a
    // network is a network is a sentence to skip on every row.
    let e = costed(
        "prices.parquet",
        crate::home::discover::Cost {
            source: Some("nfs4".into()),
            ..Default::default()
        },
        None,
    );
    let text = preview_text(&e, 44);
    assert!(text.contains("nfs4"), "{text}");
}

#[test]
fn local_disk_gets_no_warning() {
    let e = costed(
        "prices.parquet",
        crate::home::discover::Cost {
            source: Some("ext4".into()),
            ..Default::default()
        },
        None,
    );
    let text = preview_text(&e, 44);
    assert!(text.contains("ext4"), "{text}");
    assert!(
        !text.contains("network"),
        "an ordinary disk should say nothing alarming: {text}"
    );
}

#[test]
fn what_a_file_weighs_open_is_stated_with_its_ratio() {
    // The number nothing else on screen implies.
    let e = costed(
        "prices.parquet",
        crate::home::discover::Cost {
            source: Some("ext4".into()),
            uncompressed: Some(2_000_000_000),
            codec: Some("zstd".into()),
            row_groups: Some(12),
            ..Default::default()
        },
        Some(200_000_000),
    );
    let text = preview_text(&e, 44);
    assert!(text.contains("in memory"), "{text}");
    assert!(text.contains("zstd"), "{text}");
    assert!(text.contains("10.0"), "the ratio should be stated: {text}");
    assert!(text.contains("row groups"), "{text}");
}

#[test]
fn a_ratio_too_small_to_matter_is_left_out() {
    // Below about 1.2x the number is noise dressed as insight.
    let e = costed(
        "prices.parquet",
        crate::home::discover::Cost {
            source: Some("ext4".into()),
            uncompressed: Some(1_050_000),
            codec: Some("uncompressed".into()),
            ..Default::default()
        },
        Some(1_000_000),
    );
    let text = preview_text(&e, 44);
    assert!(!text.contains("1.0×") && !text.contains("1.1×"), "{text}");
}

#[test]
fn a_partition_layout_names_its_keys_and_its_range() {
    let e = costed(
        "events",
        crate::home::discover::Cost {
            source: Some("nfs4".into()),
            partitions: Some(crate::home::discover::Partitions {
                keys: vec!["year".into(), "region".into()],
                first_key_values: vec!["2023".into(), "2024".into(), "2025".into()],
                count: 3,
                more: false,
            }),
            ..Default::default()
        },
        None,
    );
    let text = preview_text(&e, 44);
    assert!(text.contains("3 by year, region"), "{text}");
    assert!(text.contains("year 2023 to 2025"), "{text}");
}

#[test]
fn a_bounded_partition_count_says_it_is_a_floor() {
    let e = costed(
        "daily",
        crate::home::discover::Cost {
            partitions: Some(crate::home::discover::Partitions {
                keys: vec!["day".into()],
                first_key_values: vec!["0001".into()],
                count: 512,
                more: true,
            }),
            ..Default::default()
        },
        None,
    );
    assert!(preview_text(&e, 44).contains("512+"));
}

#[test]
fn the_preview_never_draws_past_its_pane() {
    let e = costed(
        "a_dataset_with_a_very_long_name_indeed.parquet",
        crate::home::discover::Cost {
            source: Some("fuse.sshfs".into()),
            uncompressed: Some(9_000_000_000),
            codec: Some("zstd".into()),
            row_groups: Some(1024),
            partitions: Some(crate::home::discover::Partitions {
                keys: vec!["year".into(), "month".into(), "day".into()],
                first_key_values: vec!["2001".into(), "2025".into()],
                count: 9999,
                more: true,
            }),
            tables: None,
            opens_one: false,
            ipc_stream: false,
        },
        Some(400_000_000),
    );
    for width in [24usize, 40, 80] {
        let ctx = RenderContext::for_test();
        for line in preview_head(&e, None, None, width, &ctx) {
            // The pane wraps rather than clips, so a long value is allowed to run
            // on; what must not happen is a *heading* bar overrunning its width.
            let text: String = l_text(&line);
            if text.trim_start().starts_with("OPENING") {
                assert!(
                    text.chars().count() <= width,
                    "a {width}-wide pane drew a {}-character heading",
                    text.chars().count()
                );
            }
        }
    }
}

fn l_text(line: &Line<'_>) -> String {
    line.spans.iter().map(|s| s.content.as_ref()).collect()
}

#[test]
fn the_matched_characters_are_the_highlighted_ones() {
    let spans = row_spans("sales_2024.parquet", "sales", None);
    assert_eq!(highlighted_text(&spans), "sales");
    // Reassembling the spans must give back exactly the name; highlighting is a
    // change of style, never of text. The locality marker sits between the
    // selection marker and the name, so the name is what follows it rather than
    // what the row starts with.
    let text: String = spans.iter().map(|(t, _)| t.as_str()).collect();
    assert!(text.contains("sales_2024.parquet"), "got {text:?}");
}

#[test]
fn a_scattered_match_highlights_each_run_separately() {
    let spans = row_spans("sales_by_region.parquet", "sreg", None);
    assert_eq!(highlighted_text(&spans), "sreg");
    let runs = spans.iter().filter(|(_, hit)| *hit).count();
    assert!(
        runs >= 2,
        "a match spread across the name should be several runs, got {runs}"
    );
}

#[test]
fn consecutive_matches_become_one_span_not_a_stutter() {
    let spans = row_spans("sales.parquet", "sales", None);
    let runs = spans.iter().filter(|(_, hit)| *hit).count();
    assert_eq!(runs, 1, "five adjacent characters are one run, got {runs}");
}

#[test]
fn nothing_is_highlighted_without_a_filter() {
    let spans = row_spans("sales.parquet", "", None);
    assert_eq!(highlighted_text(&spans), "");
}

#[test]
fn a_row_matched_by_a_column_highlights_the_column_not_the_name() {
    // Marks scattered over a name that had nothing to do with the match read as
    // the filter having gone wrong.
    let spans = row_spans("orders.parquet", "cust", Some("customer_id"));
    assert_eq!(highlighted_text(&spans), "cust");
    let text: String = spans.iter().map(|(t, _)| t.as_str()).collect();
    let note = format!("{}customer_id", glyphs::get().middot);
    assert!(
        text.contains(&note),
        "the column note should still read whole, got {text:?}"
    );
}

#[test]
fn the_title_survives_a_note_that_wants_the_whole_line() {
    // Trimming the note first is the point: a header that says only where it
    // looked, and not what it is, has lost the more useful half.
    let section = Section {
        subtitle: Some("x".repeat(200)),
        ..Section::titled("Found", Vec::new())
    };
    let ctx = RenderContext::for_test();
    let line = section_header(&section, 3, false, false, &header_row(&ctx, 40));
    let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
    assert!(
        text.contains("FOUND"),
        "the title should still be readable, got {text:?}"
    );
}

/// At 80 columns the open-all row keeps what it opens and cuts the directory's
/// name, which the section title above it already carries; at 200 it is whole.
#[test]
fn the_door_keeps_what_it_opens_when_its_name_is_cut() {
    let ctx = RenderContext::for_test();
    let mut door = row("/data/same_schema/", EntryKind::MultiFile);
    door.name = "same_schema (3 Parquet files, one schema)".to_string();
    door.opens_whole_directory = true;
    door.rows = Some(6);
    door.cols = Some(2);
    let drawn = |width: usize| -> String {
        entry_line(
            &door,
            true,
            EntryNotes::default(),
            &rows(&ctx, width.saturating_sub(META_WIDTH as usize), true),
        )
        .spans
        .iter()
        .map(|s| s.content.as_ref())
        .collect()
    };
    // An 80-column terminal leaves the list 78.
    let narrow = drawn(78);
    assert!(
        narrow.contains("(3 Parquet files, one schema)"),
        "{narrow:?}"
    );
    assert!(!narrow.contains("same_schema ("), "{narrow:?}");
    assert!(
        narrow.contains("6 × 2") || narrow.contains("6 x 2"),
        "{narrow:?}"
    );
    assert_eq!(
        narrow.chars().count(),
        row_width(78 - META_WIDTH as usize, true),
        "the columns stay where they are: {narrow:?}"
    );
    // At 60 the directory goes and what Enter opens keeps its head.
    let narrowest = drawn(58);
    assert!(narrowest.contains("(3 Parquet files"), "{narrowest:?}");
    assert!(!narrowest.contains("same_"), "{narrowest:?}");
    assert_eq!(
        narrowest.chars().count(),
        row_width(58 - META_WIDTH as usize, true),
        "{narrowest:?}"
    );
    let wide = drawn(200);
    assert!(
        wide.contains("same_schema (3 Parquet files, one schema)"),
        "{wide:?}"
    );
}

fn texts(lines: &[Line]) -> Vec<String> {
    lines
        .iter()
        .map(|l| l.spans.iter().map(|s| s.content.as_ref()).collect())
        .collect()
}

/// A path too long for the pane loses its middle, between components, and keeps its
/// file name; one that fits is left alone.
#[test]
fn a_long_path_is_cut_in_the_middle_not_wrapped() {
    let e = glyphs::get().ellipsis;
    let path = "~/.config/datui/formats/demo-mktdata.toml";
    assert_eq!(elide_path(path, 60), path);
    assert_eq!(
        elide_path(path, 30),
        format!("~/{e}/formats/demo-mktdata.toml")
    );
    assert!(elide_path(path, 30).chars().count() <= 30);
    assert_eq!(elide_path(path, 22), format!("~/{e}/demo-mktdata.toml"));
    assert_eq!(
        elide_path("/a/b/c/long-name.toml", 17),
        format!("/{e}/long-name.toml")
    );
    // Only the name's end fits.
    let cut = elide_path(path, 10);
    assert!(cut.ends_with("ta.toml"), "{cut}");
    assert!(glyphs::display_width(&cut) <= 10, "{cut}");
}

/// Windows' `\` separates components too, and is kept where the path has it.
#[cfg(windows)]
#[test]
fn a_windows_path_is_cut_at_its_backslashes() {
    let e = glyphs::get().ellipsis;
    let path = r"~\.config\datui\formats\demo-mktdata.toml";
    assert_eq!(
        elide_path(path, 30),
        format!(r"~\{e}\formats\demo-mktdata.toml")
    );
}

fn spec_chips() -> Vec<MatchChip> {
    let spec = crate::formats::Spec::parse(
        r#"name = "acme.chips"
match = { glob = ["*.bin", "*.dat"], magic = "MKTD", where = { "header.version" = 1, "header.kind" = "A" } }
[header]
fields = [
  { name = "magic", type = "str", size = 4 },
  { name = "version", type = "u2" },
  { name = "kind", type = "str", size = 1 },
]
[records]
fields = [{ name = "x", type = "u1" }]
"#,
        None,
    )
    .unwrap();
    spec.match_chips()
}

/// The match line wraps between chips, never inside one, at any width; on the
/// chrome tier where the tint shows, bracketed with text quoted where it does not.
#[test]
fn match_chips_wrap_between_whole_chips() {
    let ctx = RenderContext::for_test();
    let chips = spec_chips();
    for filled in [true, false] {
        for width in [24, 30, 40, 80] {
            let lines = chip_fact_lines("match", &chips, 5, width, filled, &ctx);
            let shown = texts(&lines);
            for line in &shown {
                assert!(
                    glyphs::display_width(line) <= width,
                    "{line:?} wider than {width}"
                );
            }
            let kind = if filled { "kind A" } else { "kind \"A\"" };
            let whole: &[&str] = if width < 30 {
                &[]
            } else {
                &["magic MKTD", "version 1", kind, "glob *.bin *.dat"]
            };
            for chip in whole {
                assert!(
                    shown.iter().any(|l| l.contains(chip)),
                    "{chip:?} whole on one line at {width}: {shown:#?}"
                );
            }
            assert!(shown[0].starts_with("match  "), "{shown:?}");
            assert!(
                shown[1..].iter().all(|l| l.starts_with("       ")),
                "wrapped under the value: {shown:#?}"
            );
        }
    }
    let wide = texts(&chip_fact_lines("match", &chips, 5, 80, false, &ctx));
    assert_eq!(
        wide,
        ["match  [magic MKTD] [kind \"A\"] [version 1] [glob *.bin *.dat]"]
    );
    let wide = chip_fact_lines("match", &chips, 5, 80, true, &ctx);
    let magic = &wide[0].spans[1..6];
    assert!(
        magic
            .iter()
            .all(|s| s.style.bg == Some(ctx.table_header_bg)),
        "{magic:?}"
    );
    assert_eq!(magic[1].style.fg, Some(ctx.dimmed));
    assert_eq!(magic[3].style.fg, Some(ctx.str_col));
    let one = wide[0]
        .spans
        .iter()
        .find(|s| s.content == "1")
        .expect("the version");
    assert_eq!(one.style.fg, Some(ctx.int_col), "{wide:?}");
}

#[test]
fn a_spec_without_a_match_says_so() {
    let ctx = RenderContext::for_test();
    let lines = chip_fact_lines("match", &[], 5, 40, true, &ctx);
    assert_eq!(texts(&lines), ["match  no match"]);
}

/// A field name too long for the line is cut, not the value, and the chip stays on
/// its line whole: never past the pane, never broken in two.
#[test]
fn a_long_field_name_is_cut_before_its_value() {
    use crate::formats::ChipKind;
    let ctx = RenderContext::for_test();
    let e = glyphs::get().ellipsis;
    let chips = [
        MatchChip {
            name: "magic".to_string(),
            value: "MKTD".to_string(),
            kind: ChipKind::Magic,
            offset: None,
        },
        MatchChip {
            name: "exchange_feed_sequence_version".to_string(),
            value: "3".to_string(),
            kind: ChipKind::Int,
            offset: None,
        },
    ];
    for filled in [true, false] {
        let shown = texts(&chip_fact_lines("match", &chips, 8, 40, filled, &ctx));
        assert_eq!(shown.len(), 2, "{shown:#?}");
        for line in &shown {
            assert!(glyphs::display_width(line) <= 40, "{line:?}");
        }
        let long = &shown[1];
        assert!(long.starts_with("          "), "{shown:#?}");
        let chip = long.trim();
        let (open, close) = if filled { ("", "") } else { ("[", "]") };
        assert!(
            chip.starts_with(&format!("{open}exchange_feed")),
            "{chip:?}"
        );
        assert!(chip.ends_with(&format!("{e} 3{close}")), "{chip:?}");
    }
    // A long value with a short name still cuts the value.
    let glob = MatchChip {
        name: "glob".to_string(),
        value: "*.alpha *.bravo *.charlie *.delta *.echo".to_string(),
        kind: ChipKind::Glob,
        offset: None,
    };
    let shown = texts(&chip_fact_lines("match", &[glob], 5, 30, false, &ctx));
    assert_eq!(shown.len(), 1, "{shown:#?}");
    assert!(shown[0].starts_with("match  [glob *.alpha"), "{shown:#?}");
    assert!(shown[0].ends_with(&format!("{e}]")), "{shown:#?}");
    assert_eq!(glyphs::display_width(&shown[0]), 30, "{shown:#?}");
}

/// Variants pack under the value column, `name count` each, a line breaking only
/// between them, counted when the rows run out.
#[test]
fn variants_pack_densely_and_break_between_items() {
    let ctx = RenderContext::for_test();
    let table = |name: &str, n: usize| {
        crate::formats::members::Table::plain(name, "record type", (0..n).map(|i| format!("c{i}")))
    };
    let variants = [
        table("Status", 6),
        table("OrderAdd", 9),
        table("OrderCancel", 6),
        table("Trade", 9),
        table("Fill", 9),
    ];
    let m = glyphs::get().middot;
    let wide = texts(&variant_lines(&variants, 8, 80, 10, &ctx));
    assert_eq!(
        wide,
        [format!(
            "        Status 6 {m} OrderAdd 9 {m} OrderCancel 6 {m} Trade 9 {m} Fill 9"
        )]
    );
    let narrow = texts(&variant_lines(&variants, 8, 40, 10, &ctx));
    assert!(narrow.len() > 1, "{narrow:#?}");
    for line in &narrow {
        assert!(glyphs::display_width(line) <= 40, "{line:?}");
        assert!(!line.trim_end().ends_with(m), "{line:?}");
    }
    for item in [
        "Status 6",
        "OrderAdd 9",
        "OrderCancel 6",
        "Trade 9",
        "Fill 9",
    ] {
        assert!(
            narrow.iter().any(|l| l.contains(item)),
            "{item}: {narrow:#?}"
        );
    }
    let cut = texts(&variant_lines(&variants, 8, 30, 2, &ctx));
    assert_eq!(cut.len(), 2, "{cut:#?}");
    assert!(cut[1].trim_start().ends_with("more"), "{cut:#?}");
}

/// Every pane heading reads the same way: a noun on the rule, and any count
/// in a flat chip after it, as the list's sections carry theirs.
#[test]
fn pane_headings_put_counts_in_a_chip() {
    let ctx = RenderContext::for_test();
    let text = |line: &Line| {
        line.spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect::<String>()
    };
    let rows = rows_heading(5, 20, 40, &ctx);
    assert!(
        text(&rows).starts_with("ROWS  5 of 20 columns  "),
        "{:?}",
        text(&rows)
    );
    let chip = rows
        .spans
        .iter()
        .find(|s| s.content.contains("5 of 20"))
        .unwrap();
    assert_eq!(chip.style.bg, Some(ctx.controls_bg));
    assert!(text(&rows_heading(5, 5, 40, &ctx)).starts_with("ROWS "));
    let columns = pane_heading_counted("COLUMNS", Some("1,200"), 40, &ctx);
    assert!(
        text(&columns).starts_with("COLUMNS  1,200  "),
        "{:?}",
        text(&columns)
    );
    assert_eq!(
        text(&columns).chars().count(),
        39,
        "as wide as a heading without one"
    );
}

/// The pane's column notes sit under COLUMNS, a line each, with no key hint among
/// them (the footer has `^E Docs`); cut to the pane, the rest are counted.
#[test]
fn column_notes_are_listed_under_columns_and_counted_when_cut() {
    let ctx = RenderContext::for_test();
    let columns: Vec<(String, crate::home::catalog::ColumnNote)> = (0..6)
        .map(|i| {
            let note = crate::home::catalog::ColumnNote {
                description: format!("Note {i}"),
                unit: if i == 0 { "USD".into() } else { String::new() },
                values: Vec::new(),
                ty: String::new(),
            };
            (format!("col{i}"), note)
        })
        .collect();
    let whole = texts(&column_notes_block(&columns, 50, 20, &ctx));
    assert!(whole[1].starts_with("COLUMNS "), "{whole:#?}");
    assert_eq!(whole.len(), 2 + 6, "{whole:#?}");
    assert!(whole[2].contains("col0") && whole[2].contains("Note 0 (USD)"));
    assert!(!whole.iter().any(|l| l.contains("^E")), "{whole:#?}");

    let cut = texts(&column_notes_block(&columns, 50, 6, &ctx));
    assert_eq!(cut.len(), 6, "{cut:#?}");
    let more = format!("{} 3 more", glyphs::get().ellipsis);
    assert_eq!(cut.last(), Some(&more), "{cut:#?}");
    assert!(!cut.iter().any(|l| l.contains("^E")), "{cut:#?}");
}
