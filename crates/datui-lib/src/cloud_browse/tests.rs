use super::*;

/// The filter is asked for after the part the names share, in their case.
#[test]
fn a_filter_narrows_by_the_prefix_the_names_share() {
    let stations = [
        "STATION=ACW00011604",
        "STATION=AE000041196",
        "STATION=AFM00040938",
    ];
    assert_eq!(
        narrowing_prefix("usw", &stations).as_deref(),
        Some("STATION=USW")
    );
    // Typed with the shared part, in any case.
    assert_eq!(
        narrowing_prefix("station=usw", &stations).as_deref(),
        Some("STATION=USW")
    );
    let years = ["year=2019", "year=2020", "year=2021"];
    assert_eq!(narrowing_prefix("202", &years).as_deref(), Some("year=202"));
    // Mixed case is taken as typed; no separator shares nothing.
    let files = ["Sales.csv", "returns.csv"];
    assert_eq!(narrowing_prefix("Sal", &files).as_deref(), Some("Sal"));
    assert_eq!(narrowing_prefix("", &files), None);
    assert_eq!(narrowing_prefix("a/b", &files), None);
    assert_eq!(narrowing_prefix("x", &[]), None);
}

fn resolved(url: &str, kind: ProviderKind) -> crate::cloud_sources::Resolved {
    crate::cloud_sources::Resolved {
        url: url.to_string(),
        kind,
        source_id: String::new(),
        s3: S3Settings::default(),
        azure: Default::default(),
        signing: Signing::Try,
        place: crate::cloud_sources::access_key(url).unwrap(),
        gcloud: None,
        google_credentials: None,
        login_error: None,
    }
}

#[test]
fn the_unsigned_look_is_one_small_request() {
    let mut s3 = resolved("s3://aws-public-blockchain/v1.0/btc/", ProviderKind::S3);
    s3.s3.region = Some("us-east-2".to_string());
    assert_eq!(
        probe_url(&s3).unwrap(),
        "https://aws-public-blockchain.s3.us-east-2.amazonaws.com/?list-type=2&max-keys=1&prefix=v1.0%2Fbtc%2F"
    );
    let object = resolved("s3://my.dotted.bucket/a b/100%.parquet", ProviderKind::S3);
    assert_eq!(
        probe_url(&object).unwrap(),
        "https://s3.us-east-1.amazonaws.com/my.dotted.bucket/a%20b/100%25.parquet"
    );
    let glob = resolved("gs://bucket/year=*/part-*.parquet", ProviderKind::Gcs);
    assert_eq!(
        probe_url(&glob).unwrap(),
        "https://storage.googleapis.com/storage/v1/b/bucket/o?maxResults=1&prefix="
    );
    let azure = resolved(
        "abfss://release@overturemapswestus2.dfs.core.windows.net/2026-08-19.0/",
        ProviderKind::Azure,
    );
    assert_eq!(
        probe_url(&azure).unwrap(),
        "https://overturemapswestus2.blob.core.windows.net/release?restype=container&comp=list&maxresults=1&prefix=2026-08-19.0%2F"
    );
    let mut minio = resolved("s3://data/x.parquet", ProviderKind::S3);
    minio.s3.endpoint = Some("http://127.0.0.1:9000".to_string());
    assert_eq!(probe_url(&minio), None, "a custom endpoint is not probed");
}

/// The listing helpers every test below shares: a prefix's sub-prefixes and its
/// objects, in the shapes `look_at_listing` takes.
fn directories(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

fn objects(names: &[(&str, u64)]) -> Vec<(String, u64)> {
    names.iter().map(|(n, s)| (n.to_string(), *s)).collect()
}

#[test]
fn the_prefix_being_listed_is_not_something_it_holds() {
    // A console makes a folder by writing a zero-byte object at its key, and
    // listing that directory hands the key straight back. It stands for the prefix
    // being listed rather than for anything in it, and each of the three counts
    // missed it in its own way before it was dropped once, up front.
    let holds = look_at_listing(
        "out/sub/",
        &directories(&[]),
        &objects(&[("out/sub/", 0), ("out/sub/a.parquet", 5)]),
    )
    .1;
    assert_eq!(holds.formats, vec![("parquet".to_string(), 1)]);
    assert_eq!(holds.not_read, 0, "the prefix is not a file it cannot read");
    assert_eq!(holds.skipped, 0);
    assert_eq!(holds.directories, 0);

    // Nor when the marker has bytes in it, as `cloud-samples-data`'s do.
    let holds = look_at_listing(
        "bigquery/census/",
        &directories(&["bigquery/census/data/"]),
        &objects(&[("bigquery/census", 11)]),
    )
    .1;
    assert_eq!(holds.not_read, 0);
    assert_eq!(holds.label(), "1 dir");
}

/// A Hugging Face cache's JSON is its writer's, as on disk: one shard beside
/// `dataset_info.json` and `state.json` is a prefix of Arrow, not of JSON.
#[test]
fn a_hugging_face_prefix_is_arrow() {
    let holds = look_at_listing(
        "hf/",
        &directories(&[]),
        &objects(&[
            ("hf/data-00000-of-00001.arrow", 5),
            ("hf/dataset_info.json", 5),
            ("hf/state.json", 5),
        ]),
    )
    .1;
    assert_eq!(holds.formats, vec![("arrow".to_string(), 1)]);
    assert_eq!(holds.skipped_names, ["dataset_info.json", "state.json"]);
    let holds = look_at_listing("j/", &directories(&[]), &objects(&[("j/state.json", 5)])).1;
    assert_eq!(holds.formats, vec![("json".to_string(), 1)], "no Arrow");
}

/// A saved DatasetDict is `dataset_dict.json` beside a prefix per split: its JSON is
/// its writer's, and the prefix is read as Arrow. Without a prefix beside it, the
/// same name is a JSON file like any other.
#[test]
fn a_saved_dataset_dict_prefix_is_arrow() {
    let (kind, holds) = look_at_listing(
        "dd/",
        &directories(&["dd/test/", "dd/train/"]),
        &objects(&[("dd/dataset_dict.json", 30)]),
    );
    assert_eq!(kind, crate::discover::EntryKind::Directory);
    assert!(holds.dataset_dict);
    assert!(holds.formats.is_empty(), "{:?}", holds.formats);
    assert_eq!(holds.skipped_names, ["dataset_dict.json"]);
    assert_eq!(holds.directories, 2);

    let holds = look_at_listing(
        "j/",
        &directories(&[]),
        &objects(&[("j/dataset_dict.json", 30)]),
    )
    .1;
    assert!(!holds.dataset_dict);
    assert_eq!(holds.formats, vec![("json".to_string(), 1)]);
}

#[test]
fn the_prefix_being_listed_is_not_a_row_in_it() {
    // `object_store` strips the slash from `bigquery/census/`, so the row named an
    // object that does not exist and opening it was a 404.
    let prefixes = directories(&["bigquery/census/data/"]);
    assert!(!is_listed_object(
        "bigquery/census",
        11,
        "bigquery/census",
        &prefixes
    ));
    assert!(!is_listed_object(
        "bigquery/census",
        0,
        "bigquery/census",
        &prefixes
    ));
    assert!(is_listed_object(
        "bigquery/census/test.csv",
        240,
        "bigquery/census",
        &prefixes
    ));
    // An object named like the directory, one level up, is a real object.
    assert!(is_listed_object("bigquery/census", 11, "bigquery", &[]));
}

#[test]
fn sub_prefixes_count_against_a_prefix_being_one_table() {
    // Two Parquet objects under ten sub-prefixes is a place to look inside, which
    // is what the local route calls it. Leaving the prefixes out of `seen` made the
    // majority a formality and the same directory answered `dir` on disk and `multi`
    // in a bucket.
    let subs: Vec<String> = (0..10).map(|i| format!("out/sub{i}/")).collect();
    let (kind, holds) = look_at_listing(
        "out/",
        &subs,
        &objects(&[("out/a.parquet", 5), ("out/b.parquet", 5)]),
    );
    assert_eq!(kind, crate::discover::EntryKind::Directory);
    assert_eq!(holds.label(), "2 parquet");
    assert_eq!(holds.directories, 10);
}

#[test]
fn a_consoles_folder_placeholder_is_not_also_a_file_it_cannot_read() {
    // `out/sub/` made by a console is a zero-byte object at `out/sub` and a prefix
    // `out/sub/`. The listing reports both; they are one directory.
    let holds = look_at_listing(
        "out/",
        &directories(&["out/sub/"]),
        &objects(&[("out/sub", 0)]),
    )
    .1;
    assert_eq!(holds.directories, 1);
    assert_eq!(holds.not_read, 0, "the placeholder is the directory itself");
    assert_eq!(holds.label(), "1 dir");
}

#[test]
fn an_emr_folder_marker_beside_a_partition_is_a_writers_own_file() {
    // Legacy s3n and EMR write `<name>_$folder$` beside every prefix, so a hive
    // table's markers are named `year=2024_$folder$`. A partition test that only
    // looks for an `=` calls those data, and the pane then reports one file datui
    // cannot read per partition.
    let holds = look_at_listing(
        "out/",
        &directories(&["out/year=2024/", "out/year=2025/"]),
        &objects(&[("out/year=2024_$folder$", 0), ("out/year=2025_$folder$", 0)]),
    )
    .1;
    assert_eq!(holds.not_read, 0);
    assert_eq!(holds.partitions, 2);
    assert_eq!(holds.skipped, 2);
}

#[test]
fn partitions_carry_a_prefix_only_while_they_are_the_most_of_it() {
    // The boundary the rule turns on, which neither route had a test for: as many
    // partitions as data files is a hive root with a few files beside it, one more
    // data file than partitions is a directory that happens to hold a `key=value`.
    let parts = directories(&["out/year=2024/", "out/year=2025/"]);
    let two = objects(&[("out/a.parquet", 5), ("out/b.parquet", 5)]);
    assert_eq!(
        look_at_listing("out/", &parts, &two).0,
        crate::discover::EntryKind::Hive,
        "two partitions against two files"
    );
    let three = objects(&[
        ("out/a.parquet", 5),
        ("out/b.parquet", 5),
        ("out/c.parquet", 5),
    ]);
    assert_ne!(
        look_at_listing("out/", &parts, &three).0,
        crate::discover::EntryKind::Hive,
        "one more file than partitions"
    );
}

#[test]
fn a_stray_it_cannot_read_counts_against_a_prefix_the_way_it_does_on_disk() {
    // A zero-byte object with no dot and no prefix of its name beside it: not a
    // console's placeholder, because there is nothing it could stand for. The local
    // route puts such a stray in `not_read` and still counts it among what it saw,
    // which is what the majority is measured against. Leaving these out of `seen`
    // made two Parquet files among five of them one table in a bucket and a place
    // to look inside on disk.
    let (kind, holds) = look_at_listing(
        "yellow/",
        &directories(&[]),
        &objects(&[
            ("yellow/a.parquet", 5),
            ("yellow/b.parquet", 5),
            ("yellow/year=2028", 0),
            ("yellow/year=2029", 0),
            ("yellow/year=2030", 0),
            ("yellow/year=2031", 0),
            ("yellow/year=2032", 0),
        ]),
    );
    assert_eq!(holds.not_read, 5);
    assert_eq!(
        kind,
        crate::discover::EntryKind::Directory,
        "five it cannot read outvote two it can"
    );
}

#[test]
fn a_prefix_with_another_page_behind_it_says_so() {
    let keys: Vec<(String, u64)> = (0..100)
        .map(|i| (format!("out/part-{i:05}.parquet"), 100u64))
        .collect();
    // The page is all datui asked for, so the count is a floor and the label says
    // it. Without the `+` a hundred is an exact hundred nobody counted.
    let holds = look_at_page("out/", &directories(&[]), &keys, Some("next-page-token")).1;
    assert!(holds.truncated);
    assert_eq!(holds.label(), "100+ parquet");
    // And the last page, which the store answers with no token at all, counts what
    // is there.
    let holds = look_at_page("out/", &directories(&[]), &keys, None).1;
    assert!(!holds.truncated);
    assert_eq!(holds.label(), "100 parquet");
}

#[test]
fn a_pane_never_lists_more_skipped_names_than_it_promised() {
    // The names are there so the convention is recognisable, not so the pane holds
    // a paragraph of them. Thirty `.crc` objects is an ordinary Spark output.
    let keys: Vec<(String, u64)> = (0..30)
        .map(|i| (format!("out/.part-{i:05}.parquet.crc"), 8u64))
        .collect();
    let holds = look_at_listing("out/", &directories(&[]), &keys).1;
    assert_eq!(holds.skipped, 30, "all of them are counted");
    assert_eq!(
        holds.skipped_names.len(),
        crate::discover::SKIPPED_NAMES_SHOWN,
        "but only a few are named"
    );

    // And they are the first few *by name*, not the first few the store listed.
    // The objects and the prefixes arrive as two runs, so without a sort the pane
    // names a different four here than the local route names for the same directory.
    let holds = look_at_listing(
        "out/",
        &directories(&["out/_temporary/"]),
        &objects(&[("out/_SUCCESS", 0), ("out/.part.crc", 8)]),
    )
    .1;
    assert_eq!(
        holds.skipped_names,
        vec![
            ".part.crc".to_string(),
            "_SUCCESS".to_string(),
            "_temporary".to_string()
        ]
    );
}

#[test]
fn two_formats_that_tie_are_named_in_the_same_order_every_time() {
    // Commonest first, and by name where two tie — otherwise the line reads in
    // whatever order the store listed the keys, and the same prefix says `2 json ·
    // 2 csv` on one visit and `2 csv · 2 json` on the next.
    let holds = look_at_listing(
        "out/",
        &directories(&[]),
        &objects(&[
            ("out/a.json", 5),
            ("out/b.json", 5),
            ("out/y.csv", 5),
            ("out/z.csv", 5),
        ]),
    )
    .1;
    assert_eq!(holds.line(false).unwrap(), "2 csv · 2 json");
    assert_eq!(holds.label(), "mixed");
}

#[test]
fn a_directory_is_classified_by_one_page_of_its_listing() {
    use crate::discover::EntryKind;
    // Each listing under the prefix it is a listing of. The prefix is only read to
    // drop the prefix's own key, which `the_prefix_being_listed_is_not_something_it_holds`
    // is about — but a prefix that is not the parent of the keys beside it is a
    // fixture describing a listing no store would return.
    assert_eq!(
        look_at_listing(
            "v1.0/btc/blocks/",
            &directories(&[
                "v1.0/btc/blocks/date=2009-01-03/",
                "v1.0/btc/blocks/date=2009-01-09/"
            ]),
            &objects(&[("v1.0/btc/blocks/_SUCCESS", 0)]),
        )
        .0,
        EntryKind::Hive
    );
    assert_eq!(
        look_at_listing(
            "gbif/occurrence.parquet/",
            &directories(&[]),
            &objects(&[
                ("gbif/occurrence.parquet/000001", 10),
                ("gbif/occurrence.parquet/000002", 10)
            ]),
        )
        .0,
        EntryKind::MultiFile,
        "part files with no extension"
    );
    assert_eq!(
        look_at_listing(
            "a/",
            &directories(&[]),
            &objects(&[("a/x.csv", 5), ("a/y.csv", 5)])
        )
        .0,
        EntryKind::Directory,
        "CSV cannot be read in place as one table"
    );
    assert_eq!(
        look_at_listing(
            "a/",
            &directories(&["a/by_year/", "a/by_station/"]),
            &objects(&[])
        )
        .0,
        EntryKind::Directory
    );
    assert_eq!(
        look_at_listing(
            "a/",
            &directories(&["a/b/"]),
            &objects(&[("a/one.parquet", 5)])
        )
        .0,
        EntryKind::Directory,
        "one file is a file to open, not a dataset"
    );
}

/// A lake table's data files agree on a schema, so the one-table rule says `multi`
/// and is right about the schema and wrong about the rows.
#[test]
fn a_lake_table_is_not_a_directory_of_parquet_files() {
    use crate::discover::EntryKind;
    let parts = objects(&[
        ("t/part-00000.parquet", 10),
        ("t/part-00001.parquet", 10),
        ("t/part-00002.parquet", 10),
    ]);

    assert_eq!(
        look_at_listing("t/", &directories(&["t/_delta_log/"]), &parts).0,
        EntryKind::Delta
    );
    assert_eq!(
        look_at_listing("t/", &directories(&["t/.hoodie/"]), &parts).0,
        EntryKind::Hudi
    );
    assert_eq!(
        look_at_listing(
            "t/",
            &directories(&["t/metadata/", "t/data/"]),
            &objects(&[])
        )
        .0,
        EntryKind::Iceberg
    );

    // The plain name alone is not the marker.
    assert_eq!(
        look_at_listing("t/", &directories(&["t/metadata/"]), &parts).0,
        EntryKind::MultiFile,
        "a directory called metadata beside part files is not an Iceberg table"
    );
    assert_eq!(
        look_at_listing("t/", &directories(&["t/metadata/", "t/data/"]), &parts).0,
        EntryKind::MultiFile,
        "an Iceberg root holds its data under data/, not beside it"
    );
    assert_eq!(
        look_at_listing(
            "t/",
            &directories(&["t/metadata/", "t/data/"]),
            &objects(&[("t/README.md", 20)])
        )
        .0,
        EntryKind::Iceberg,
        "but something else beside them does not disqualify it"
    );
    assert_eq!(
        look_at_listing("t/", &directories(&[]), &parts).0,
        EntryKind::MultiFile,
        "and a directory of part files with no log is still one table"
    );
}

#[test]
fn refusals_and_job_files() {
    assert!(is_refusal(
        "Client error with status 403 Forbidden: <Code>AccessDenied</Code>"
    ));
    assert!(is_refusal(
        "Server returned 401 NoAuthenticationInformation"
    ));
    assert!(!is_refusal("error sending request: connection refused"));
    for name in [
        "_SUCCESS",
        "_committed_123",
        "_started_123",
        "yellow_$folder$",
        "_metadata.json",
        ".crc",
    ] {
        assert!(
            crate::discover::is_bookkeeping(name),
            "{name} is a writer's own file"
        );
    }
    assert!(!crate::discover::is_bookkeeping("part-0000.parquet"));

    // One thing named twice — the object and the prefix — is one skipped entry,
    // and an empty object standing for no folder is a file nothing can read, which
    // is what the local route calls it.
    let directories = ["out/_temporary/".to_string()];
    let objects: Vec<(String, u64)> = [
        ("out/_temporary", 12u64),
        ("out/NOTES", 0),
        ("out/a.parquet", 100),
        ("out/b.parquet", 100),
    ]
    .iter()
    .map(|(k, s)| ((*k).to_string(), *s))
    .collect();
    let holds = look_at_listing("out", &directories, &objects).1;
    assert_eq!(holds.line(true).as_deref(), Some("2 parquet"));

    // The folder's own marker stands for the prefix being listed, not for
    // anything in it: a console makes a folder by writing a zero-byte object at
    // its key, and listing that directory hands it straight back.
    let own_marker: Vec<(String, u64)> = [("out", 0u64), ("out/a.parquet", 100)]
        .iter()
        .map(|(k, s)| ((*k).to_string(), *s))
        .collect();
    let holds = look_at_listing("out/", &[], &own_marker).1;
    assert_eq!(holds.line(true).as_deref(), Some("1 parquet"));

    // However it is spelled. A dot in the prefix name carries its key past the
    // empty-marker test, and a prefix named like a writer's own file would
    // otherwise report itself under `skipped`.
    for (prefix, key) in [("v1.0/", "v1.0"), ("out/_temporary/", "out/_temporary")] {
        let objects: Vec<(String, u64)> =
            [(key.to_string(), 0u64), (format!("{key}/a.parquet"), 100)]
                .into_iter()
                .collect();
        let holds = look_at_listing(prefix, &[], &objects).1;
        assert_eq!(
            holds.line(true).as_deref(),
            Some("1 parquet"),
            "{prefix} counted its own key"
        );
    }
    // Whether a key counts as data and whether it is worth a row are two questions.
    // `_manifest.parquet` is a writer's own file and still something to open, and
    // the local listing has always shown its equivalent.
    assert!(crate::discover::is_bookkeeping("_manifest.parquet"));
    assert!(!is_marker("_manifest.parquet"));
    assert!(!is_marker("_2024_sales.csv"));
    for name in ["_SUCCESS", "_committed_1", "_started_1", "yellow_$folder$"] {
        assert!(is_marker(name), "{name} stands for no data at all");
    }
    assert!(is_empty_marker("year=2032", 0));
    assert!(!is_empty_marker("year=2032", 10));
    assert!(!is_empty_marker("empty.csv", 0));
}

#[test]
fn a_key_is_taken_as_the_service_stores_it() {
    assert_eq!(object_path("edge/100%.csv.gz").as_ref(), "edge/100%.csv.gz");
    assert_eq!(
        object_path("edge/a+b=c&d#e.parquet").as_ref(),
        "edge/a+b=c&d#e.parquet"
    );
    assert_eq!(
        object_path("edge/name with spaces").as_ref(),
        "edge/name with spaces"
    );
    // Not a path as it stands: made into one the way object_store does.
    assert_eq!(
        object_path("a/../b").as_ref(),
        object_store::path::Path::from("a/../b").as_ref()
    );
}
use std::collections::HashMap;

/// An environment built from literals, so a test says exactly what the machine
/// looks like and nothing leaks in from the machine running it.
fn env_of(
    vars: &[(&str, &str)],
    files: &[&str],
    home: Option<&str>,
) -> (HashMap<String, String>, Vec<PathBuf>, Option<PathBuf>) {
    (
        vars.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        files.iter().map(PathBuf::from).collect(),
        home.map(PathBuf::from),
    )
}

macro_rules! environment {
    ($vars:expr_2021, $files:expr_2021, $home:expr_2021) => {
        Environment {
            var: &|key| $vars.get(key).cloned(),
            exists: &|path| $files.iter().any(|f: &PathBuf| f == path),
            read: &|_| None,
            home: $home.clone(),
            windows: false,
            run: &|_, _| {
                Err(crate::cloud_command::CommandError::Missing(
                    "test".to_string(),
                ))
            },
            all_vars: &|| Vec::new(),
            list: &|_| Vec::new(),
        }
    };
    ($vars:expr_2021, $files:expr_2021, $home:expr_2021, $contents:expr_2021) => {
        Environment {
            var: &|key| $vars.get(key).cloned(),
            exists: &|path| $files.iter().any(|f: &PathBuf| f == path),
            read: &|_| Some($contents.to_string()),
            home: $home.clone(),
            windows: false,
            run: &|_, _| {
                Err(crate::cloud_command::CommandError::Missing(
                    "test".to_string(),
                ))
            },
            all_vars: &|| Vec::new(),
            list: &|_| Vec::new(),
        }
    };
}

#[test]
fn nothing_configured_finds_nothing() {
    let (vars, files, home) = env_of(&[], &[], Some("/home/u"));
    let env = environment!(vars, files, home);
    assert!(detect(&CloudConfig::default(), &env).is_empty());
}

#[test]
fn gcloud_default_credentials_are_enough_to_list_gcs() {
    let (vars, files, home) = env_of(
        &[("GOOGLE_CLOUD_PROJECT", "example-project")],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].kind, ProviderKind::Gcs);
    assert_eq!(found[0].project.as_deref(), Some("example-project"));
    assert!(found[0].can_list_buckets());
    assert_eq!(found[0].note, "gcloud");
    assert_eq!(
        found[0].detail().as_deref(),
        Some("project: example-project")
    );
}

#[test]
fn an_aws_profile_is_the_detail_for_s3() {
    let (vars, files, home) = env_of(&[("AWS_PROFILE", "research")], &[], Some("/home/u"));
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found[0].detail().as_deref(), Some("profile: research"));
}

#[test]
fn gcs_without_a_project_is_shown_but_cannot_enumerate() {
    // Worth keeping visible: the credentials work, so a URL the user types still
    // opens. Only the listing is impossible, and the UI can say so.
    let (vars, files, home) = env_of(
        &[],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found.len(), 1);
    assert!(found[0].project.is_none());
    assert!(!found[0].can_list_buckets());
}

#[test]
fn a_service_account_outranks_the_developer_login_in_the_note() {
    let (vars, files, home) = env_of(
        &[("GOOGLE_SERVICE_ACCOUNT", "/keys/sa.json")],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found[0].note, "service account");
}

#[test]
fn aws_keys_in_the_environment_are_amazon_until_an_endpoint_says_otherwise() {
    let (vars, files, home) = env_of(&[("AWS_ACCESS_KEY_ID", "AKIA")], &[], Some("/home/u"));
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].label, "Amazon S3");
    assert_eq!(found[0].note, "AWS_ACCESS_KEY_ID");
}

#[test]
fn a_custom_endpoint_is_named_by_its_host_and_not_guessed_at() {
    let config = CloudConfig {
        s3_endpoint_url: Some("http://localhost:9000".to_string()),
        s3_access_key_id: Some("minioadmin".to_string()),
        ..CloudConfig::default()
    };
    let (vars, files, home) = env_of(&[], &[], Some("/home/u"));
    let env = environment!(vars, files, home);
    let found = detect(&config, &env);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].label, "S3-compatible (localhost:9000)");
    assert_eq!(found[0].note, "datui config");
    assert_eq!(found[0].endpoint.as_deref(), Some("http://localhost:9000"));
}

/// The config as `run()` hands it to discovery: the file's settings with the
/// environment folded in.
fn effective(config: &CloudConfig, env: &Environment<'_>) -> CloudConfig {
    let mut merged = config.clone();
    merged.overlay(CloudConfig::from_env(env.var));
    merged
}

#[test]
fn an_endpoint_from_the_environment_counts_too() {
    let (vars, files, home) = env_of(
        &[
            ("AWS_ACCESS_KEY_ID", "minioadmin"),
            ("AWS_ENDPOINT_URL", "https://minio.internal:9000/"),
        ],
        &[],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home);
    let config = effective(&CloudConfig::default(), &env);
    let found = detect(&config, &env);
    assert_eq!(found[0].label, "S3-compatible (minio.internal:9000)");
    // The bug this guards against: the title named the environment's host while
    // the listing, reading the config alone, went to AWS.
    assert_eq!(
        s3_list_buckets_url(&S3Settings::from_config(&config)),
        "https://minio.internal:9000/"
    );
}

#[test]
fn the_service_specific_endpoint_variable_outranks_the_general_one() {
    let (vars, files, home) = env_of(
        &[
            ("AWS_ACCESS_KEY_ID", "k"),
            ("AWS_ENDPOINT", "http://third:1"),
            ("AWS_ENDPOINT_URL", "http://second:2"),
            ("AWS_ENDPOINT_URL_S3", "http://first:3"),
        ],
        &[],
        None,
    );
    let env = environment!(vars, files, home);
    let config = effective(&CloudConfig::default(), &env);
    assert_eq!(
        s3_list_buckets_url(&S3Settings::from_config(&config)),
        "http://first:3/"
    );
    assert_eq!(detect(&config, &env)[0].label, "S3-compatible (first:3)");
}

#[test]
fn a_blank_endpoint_variable_does_not_erase_the_configured_one() {
    let (vars, files, home) = env_of(
        &[("AWS_ACCESS_KEY_ID", "k"), ("AWS_ENDPOINT_URL", "  ")],
        &[],
        None,
    );
    let env = environment!(vars, files, home);
    let file = CloudConfig {
        s3_endpoint_url: Some("http://localhost:9000".to_string()),
        ..CloudConfig::default()
    };
    let config = effective(&file, &env);
    assert_eq!(
        s3_list_buckets_url(&S3Settings::from_config(&config)),
        "http://localhost:9000/"
    );
    assert_eq!(
        detect(&config, &env)[0].label,
        "S3-compatible (localhost:9000)"
    );
}

#[test]
fn a_key_without_a_secret_still_reaches_the_builder() {
    // The generated config recommends the key in the file and the secret from
    // AWS_SECRET_ACCESS_KEY. Applying them only as a pair dropped the key.
    use object_store::aws::AmazonS3ConfigKey;
    let config = CloudConfig {
        s3_access_key_id: Some("from-config".to_string()),
        ..CloudConfig::default()
    };
    let builder = s3_builder("bucket", &S3Settings::from_config(&config));
    assert_eq!(
        builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
        Some("from-config".to_string())
    );
    let config = CloudConfig {
        s3_secret_access_key: Some("from-env".to_string()),
        ..CloudConfig::default()
    };
    let builder = s3_builder("bucket", &S3Settings::from_config(&config));
    assert_eq!(
        builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey),
        Some("from-env".to_string())
    );
}

#[test]
fn a_shared_credentials_file_is_enough() {
    let (vars, files, home) = env_of(&[], &["/home/u/.aws/credentials"], Some("/home/u"));
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].note, "~/.aws");
}

#[test]
fn both_providers_appear_when_both_are_usable() {
    let (vars, files, home) = env_of(
        &[("AWS_ACCESS_KEY_ID", "AKIA"), ("GOOGLE_CLOUD_PROJECT", "p")],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].kind, ProviderKind::Gcs);
    assert_eq!(found[1].kind, ProviderKind::S3);
}

#[test]
fn a_blank_project_variable_is_not_a_project() {
    let (vars, files, home) = env_of(
        &[("GOOGLE_CLOUD_PROJECT", "   ")],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home);
    let found = detect(&CloudConfig::default(), &env);
    assert!(found[0].project.is_none());
}

#[test]
fn the_project_comes_from_the_credentials_file_when_nothing_else_says() {
    // The shape gcloud writes: an authorized_user with the project the developer
    // was working in. Without this fallback, a machine that has only ever run
    // `gcloud auth application-default login` can open a bucket but not find one.
    let adc = r#"{
          "type": "authorized_user",
          "client_id": "x.apps.googleusercontent.com",
          "refresh_token": "secret-and-not-read-here",
          "quota_project_id": "example-project"
        }"#;
    let (vars, files, home) = env_of(
        &[],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home, adc);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found[0].project.as_deref(), Some("example-project"));
    assert!(found[0].can_list_buckets());
}

#[test]
fn an_environment_variable_outranks_the_credentials_file() {
    let adc = r#"{"quota_project_id": "from-the-file"}"#;
    let (vars, files, home) = env_of(
        &[("GOOGLE_CLOUD_PROJECT", "from-the-shell")],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home, adc);
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found[0].project.as_deref(), Some("from-the-shell"));
}

#[test]
fn an_unparseable_credentials_file_costs_the_project_and_nothing_else() {
    let (vars, files, home) = env_of(
        &[],
        &["/home/u/.config/gcloud/application_default_credentials.json"],
        Some("/home/u"),
    );
    let env = environment!(vars, files, home, "{ not json");
    let found = detect(&CloudConfig::default(), &env);
    assert_eq!(found.len(), 1, "the provider is still usable");
    assert!(found[0].project.is_none());
}

#[test]
fn gcs_buckets_come_out_of_a_real_shaped_response() {
    let body = r#"{
          "kind": "storage#buckets",
          "items": [
            {"kind": "storage#bucket", "name": "example-data", "location": "US-CENTRAL1"},
            {"kind": "storage#bucket", "name": "example-backups"}
          ]
        }"#;
    assert_eq!(
        parse_gcs_buckets(body).unwrap(),
        vec!["example-data", "example-backups"]
    );
}

#[test]
fn a_project_with_no_buckets_is_an_answer_not_an_error() {
    assert_eq!(
        parse_gcs_buckets(r#"{"kind": "storage#buckets"}"#).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn a_gcs_error_body_is_reported_rather_than_read_as_emptiness() {
    let body =
        r#"{"error": {"code": 403, "message": "does not have storage.buckets.list access"}}"#;
    let err = parse_gcs_buckets(body).unwrap_err();
    assert!(err.contains("storage.buckets.list"), "{err}");
}

#[test]
fn a_malformed_gcs_entry_does_not_discard_the_page() {
    let body = r#"{"items": [{"name": ""}, {"nome": "typo"}, {"name": "good"}]}"#;
    assert_eq!(parse_gcs_buckets(body).unwrap(), vec!["good"]);
}

#[test]
fn gcs_pagination_token_is_found_when_present() {
    assert_eq!(
        gcs_next_page_token(r#"{"nextPageToken": "abc", "items": []}"#).as_deref(),
        Some("abc")
    );
    assert!(gcs_next_page_token(r#"{"items": []}"#).is_none());
}

#[test]
fn the_listing_goes_to_amazon_when_no_endpoint_is_set() {
    assert_eq!(
        s3_list_buckets_url(&S3Settings::from_config(&CloudConfig::default())),
        "https://s3.amazonaws.com/"
    );
}

#[test]
fn the_listing_honours_the_endpoint_override() {
    // The bug this guards against: the section title named the override's host
    // while `ListBuckets` went to AWS. Listing must see the same merged endpoint
    // the open path uses, with the environment's beating the config.
    let config = CloudConfig {
        s3_endpoint_url: Some("http://localhost:9000/".to_string()),
        ..CloudConfig::default()
    };
    let mut effective = config.clone();
    effective.overlay(CloudConfig::from_env(&|name| {
        (name == "AWS_ENDPOINT_URL").then(|| "http://127.0.0.1:9101".to_string())
    }));
    assert_eq!(
        s3_list_buckets_url(&S3Settings::from_config(&effective)),
        "http://127.0.0.1:9101/"
    );
    // The title names the same host the listing goes to.
    let (vars, files, home) = env_of(&[("AWS_ACCESS_KEY_ID", "testing")], &[], None);
    let env = environment!(vars, files, home);
    let found = detect(&effective, &env);
    assert_eq!(found[0].label, "S3-compatible (127.0.0.1:9101)");

    // Without an override the config file's endpoint stands.
    let mut effective = config.clone();
    effective.overlay(CloudConfig::from_env(&|_| None));
    assert_eq!(
        s3_list_buckets_url(&S3Settings::from_config(&effective)),
        "http://localhost:9000/"
    );
}

#[test]
fn the_override_carries_keys_and_region_too() {
    let config = CloudConfig {
        s3_access_key_id: Some("from-config".to_string()),
        s3_region: Some("eu-west-1".to_string()),
        ..CloudConfig::default()
    };
    let mut effective = config.clone();
    effective.overlay(CloudConfig::from_env(&|name| match name {
        "AWS_ACCESS_KEY_ID" => Some("from-env".to_string()),
        "AWS_SECRET_ACCESS_KEY" => Some("secret".to_string()),
        _ => None,
    }));
    assert_eq!(effective.s3_access_key_id.as_deref(), Some("from-env"));
    assert_eq!(effective.s3_secret_access_key.as_deref(), Some("secret"));
    assert_eq!(effective.s3_region.as_deref(), Some("eu-west-1"));
}

#[test]
fn s3_buckets_come_out_of_a_real_shaped_response() {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner><ID>abc</ID><DisplayName>owner</DisplayName></Owner>
          <Buckets>
            <Bucket><Name>first-bucket</Name><CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>
            <Bucket><Name>second-bucket</Name><CreationDate>2024-02-01T00:00:00.000Z</CreationDate></Bucket>
          </Buckets>
        </ListAllMyBucketsResult>"#;
    assert_eq!(
        parse_s3_buckets(body).unwrap(),
        vec!["first-bucket", "second-bucket"]
    );
}

#[test]
fn the_owner_display_name_is_not_mistaken_for_a_bucket() {
    // Owner/DisplayName and Bucket/Name are both leaves called something plausible.
    // Matching on the leaf alone picked up the owner; matching on the parent too is
    // what makes this right.
    let body = r#"<ListAllMyBucketsResult>
          <Owner><ID>x</ID><DisplayName>Name</DisplayName></Owner>
          <Buckets><Bucket><Name>only-bucket</Name></Bucket></Buckets>
        </ListAllMyBucketsResult>"#;
    assert_eq!(parse_s3_buckets(body).unwrap(), vec!["only-bucket"]);
}

#[test]
fn an_s3_error_document_is_reported() {
    let body =
        r#"<Error><Code>InvalidAccessKeyId</Code><Message>The key is not valid</Message></Error>"#;
    let err = parse_s3_buckets(body).unwrap_err();
    assert!(err.contains("not valid"), "{err}");
}

#[test]
fn no_buckets_is_not_an_error() {
    let body = r#"<ListAllMyBucketsResult><Buckets></Buckets></ListAllMyBucketsResult>"#;
    assert_eq!(parse_s3_buckets(body).unwrap(), Vec::<String>::new());
}

#[test]
fn implausibly_deep_xml_is_refused_rather_than_followed() {
    let body = "<a>".repeat(MAX_XML_DEPTH + 2);
    assert!(parse_s3_buckets(&body).is_err());
}

#[test]
fn malformed_xml_is_an_error_and_not_a_panic() {
    assert!(parse_s3_buckets("<Buckets><Bucket><Name>x").is_err());
}

#[test]
fn an_endpoint_host_is_extracted_or_declined() {
    assert_eq!(
        endpoint_host("http://localhost:9000"),
        Some("localhost:9000".into())
    );
    assert_eq!(endpoint_host("https://a.b/c/d"), Some("a.b".into()));
    assert_eq!(endpoint_host("minio:9000"), Some("minio:9000".into()));
    assert_eq!(endpoint_host(""), None);
    assert_eq!(endpoint_host("http://"), None);
}
