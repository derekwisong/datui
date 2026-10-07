use super::*;

#[test]
fn an_existing_name_is_never_a_glob() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("d[1].csv");
    std::fs::write(&file, "a\n1\n").unwrap();
    assert!(!expands_as_glob(&file));
    assert!(expands_as_glob(&dir.path().join("d[2].csv")));
    assert!(expands_as_glob(&dir.path().join("*.csv")));
    assert!(!expands_as_glob(&dir.path().join("plain.csv")));
    assert!(!expands_as_glob(dir.path()));

    let escaped = polars_literal_path(&file).unwrap();
    assert!(escaped.as_str().ends_with("d[[]1[]].csv"), "{escaped:?}");
    let pattern = dir.path().join("d[2].csv");
    let kept = polars_literal_path(&pattern).unwrap();
    // Polars writes a Windows path with forward slashes.
    assert_eq!(
        kept.as_str().replace('\\', "/"),
        pattern.to_str().unwrap().replace('\\', "/")
    );
}

/// The escaped name reads that one file through the NDJSON scan, which always
/// expands, whatever else the name holds: `]` alone, braces (no glob meaning to
/// Polars), a backslash (a plain character on Unix), and a directory with `[`.
#[test]
fn an_escaped_name_reads_that_file_alone() {
    use polars::prelude::{LazyFileListReader, LazyJsonLineReader};
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("set[1]");
    std::fs::create_dir(&nested).unwrap();
    std::fs::create_dir(dir.path().join("set1")).unwrap();
    let mut names = vec!["d[1]", "h]", "i{j,k}", "[!x]", "set[1]/p[a]"];
    if cfg!(unix) {
        names.extend(["a*b", "x?", "e\\f", "g[x]*?"]);
    }
    // What the unescaped patterns would also match.
    for decoy in ["d1", "ha", "set1/pa", "ab", "xy", "gx", "y"] {
        std::fs::write(dir.path().join(format!("{decoy}.jsonl")), "{\"v\": 0}\n").unwrap();
    }
    for name in names {
        let file = dir.path().join(format!("{name}.jsonl"));
        std::fs::write(
            &file,
            format!("{{\"v\": \"{}\"}}\n", name.replace('\\', "/")),
        )
        .unwrap();
        let lf = LazyJsonLineReader::new(polars_literal_path(&file).unwrap())
            .finish()
            .unwrap();
        let df = lf.collect().unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(df.height(), 1, "{name}");
        let v = df
            .column("v")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .map(str::to_string);
        assert_eq!(
            v.as_deref(),
            Some(name.replace('\\', "/").as_str()),
            "{name}"
        );
    }
}

#[test]
fn every_url_datui_reads_is_remote() {
    for url in [
        "s3://bucket/key.parquet",
        "s3a://bucket/key.parquet",
        "gs://bucket/dir/",
        "gcs://bucket/dir/",
        "abfss://release@overturemapswestus2.dfs.core.windows.net/2026-09-23.1/",
        "abfs://container@account.dfs.core.windows.net/x.parquet",
        "https://account.blob.core.windows.net/container/x.csv",
        "az://container/x.csv",
        "adl://container/x.csv",
        "azure://container/x.csv",
        "http://example.com/data.csv",
        "https://example.com/data.csv",
    ] {
        assert!(is_remote_url(Path::new(url)), "{url}");
    }
    for path in ["/tmp/file.parquet", "relative.csv", ".", "data/2024.csv"] {
        assert!(!is_remote_url(Path::new(path)), "{path}");
    }
}

#[test]
fn input_source_local_path() {
    let p = PathBuf::from("/tmp/file.parquet");
    assert!(matches!(input_source(&p), InputSource::Local(_)));
    let p = PathBuf::from("relative.csv");
    assert!(matches!(input_source(&p), InputSource::Local(_)));
    let p = PathBuf::from(".");
    assert!(matches!(input_source(&p), InputSource::Local(_)));
}

#[test]
fn input_source_s3() {
    let p = PathBuf::from("s3://bucket/key.parquet");
    match input_source(&p) {
        InputSource::S3(rest) => assert_eq!(rest, "bucket/key.parquet"),
        _ => panic!("expected S3"),
    }
    let p = PathBuf::from("S3://my-bucket/path/to/file.csv");
    match input_source(&p) {
        InputSource::S3(rest) => assert_eq!(rest, "my-bucket/path/to/file.csv"),
        _ => panic!("expected S3"),
    }
}

#[test]
fn a_source_is_split_off_s3_urls_only() {
    assert_eq!(
        split_source_id("s3://onprem@sales/2024/q3.parquet"),
        (Some("onprem"), "s3://sales/2024/q3.parquet".into())
    );
    assert_eq!(
        split_source_id("s3://onprem@sales"),
        (Some("onprem"), "s3://sales".into())
    );
    assert_eq!(
        split_source_id("s3://sales/a@b.parquet"),
        (None, "s3://sales/a@b.parquet".into())
    );
    assert_eq!(
        split_source_id("gs://bucket/key"),
        (None, "gs://bucket/key".into())
    );
    assert_eq!(
        split_source_id("https://user@host/file.csv"),
        (None, "https://user@host/file.csv".into())
    );
    assert_eq!(split_source_id("s3://@sales"), (None, "s3://@sales".into()));
}

#[test]
fn azure_urls_in_every_form_that_names_the_account_become_one() {
    let canonical = "abfss://datui-test@datalake001.dfs.core.windows.net/demo/fred/";
    for url in [
        canonical,
        "abfs://datui-test@datalake001.dfs.core.windows.net/demo/fred/",
        "https://datalake001.blob.core.windows.net/datui-test/demo/fred/",
        "https://DataLake001.dfs.core.windows.net/datui-test/demo/fred/",
    ] {
        assert_eq!(
            input_source(Path::new(url)),
            InputSource::Azure(canonical.to_string()),
            "{url}"
        );
    }
    assert_eq!(
        azure_parts("abfss://c@acct.dfs.core.windows.net"),
        Some(("acct".to_string(), "c".to_string(), String::new()))
    );
    // No account, or not Azure at all.
    assert_eq!(azure_parts("az://container/path"), None);
    assert!(matches!(
        input_source(Path::new("https://example.com/c/x.csv")),
        InputSource::Http(_)
    ));
    assert!(scans_in_place(Path::new(
        "abfss://c@acct.dfs.core.windows.net/x.parquet"
    )));
}

#[test]
fn input_source_http() {
    let p = PathBuf::from("https://example.com/data.parquet");
    match input_source(&p) {
        InputSource::Http(u) => assert_eq!(u, "https://example.com/data.parquet"),
        _ => panic!("expected Http"),
    }
    let p = PathBuf::from("http://host/path/file.csv");
    match input_source(&p) {
        InputSource::Http(u) => assert_eq!(u, "http://host/path/file.csv"),
        _ => panic!("expected Http"),
    }
}

#[test]
fn input_source_gcs() {
    let p = PathBuf::from("gs://my-bucket/path/file.parquet");
    match input_source(&p) {
        InputSource::Gcs(rest) => assert_eq!(rest, "my-bucket/path/file.parquet"),
        _ => panic!("expected Gcs"),
    }
    let p = PathBuf::from("gcs://bucket/key.parquet");
    match input_source(&p) {
        InputSource::Gcs(rest) => assert_eq!(rest, "bucket/key.parquet"),
        _ => panic!("expected Gcs"),
    }
}

#[test]
fn input_source_unknown_scheme_stays_local() {
    let p = PathBuf::from("file:///tmp/foo.parquet");
    assert!(matches!(input_source(&p), InputSource::Local(_)));
}

#[test]
fn url_path_extension_s3() {
    let (path, ext) = url_path_extension("s3://bucket/key.parquet");
    assert_eq!(path, "bucket/key.parquet");
    assert_eq!(ext.as_deref(), Some("parquet"));
    let (path, ext) = url_path_extension("s3://b/path/to/file.csv");
    assert_eq!(path, "b/path/to/file.csv");
    assert_eq!(ext.as_deref(), Some("csv"));
}

#[cfg(any(feature = "http", feature = "cloud"))]
#[test]
fn a_download_keeps_what_the_compressed_file_holds() {
    assert_eq!(
        download_suffix("s3://b/edge/100%.csv.gz").as_deref(),
        Some("csv.gz")
    );
    assert_eq!(
        download_suffix("https://x.com/a/log.json.zst").as_deref(),
        Some("json.zst")
    );
    assert_eq!(download_suffix("gs://b/data.csv").as_deref(), Some("csv"));
    assert_eq!(download_suffix("s3://b/archive.gz").as_deref(), Some("gz"));
    assert_eq!(download_suffix("s3://b/no-extension"), None);
}

#[test]
fn url_path_extension_https() {
    let (path, ext) = url_path_extension("https://example.com/dir/file.parquet");
    assert_eq!(path, "dir/file.parquet");
    assert_eq!(ext.as_deref(), Some("parquet"));
    let (_, ext) = url_path_extension("https://x.com/file.csv.gz");
    assert_eq!(ext.as_deref(), Some("gz"));
}

#[test]
fn only_parquet_prefixes_and_globs_are_scanned_in_place() {
    assert!(scans_in_place(Path::new("s3://bucket/obj.parquet")));
    assert!(scans_in_place(Path::new("gs://bucket/prefix/")));
    assert!(scans_in_place(Path::new("s3://bucket/year=*/*.parquet")));
    // Downloaded first, then opened as a local file: not a remote scan.
    assert!(!scans_in_place(Path::new("s3://bucket/data.csv")));
    assert!(!scans_in_place(Path::new("s3://bucket/data.csv.gz")));
    assert!(!scans_in_place(Path::new(
        "https://example.com/data.parquet"
    )));
    assert!(!scans_in_place(Path::new("/data/local.parquet")));
}

#[test]
fn cloud_path_should_download() {
    assert!(super::cloud_path_should_download(Some("csv"), false));
    assert!(super::cloud_path_should_download(Some("gz"), false));
    assert!(super::cloud_path_should_download(Some("csv.gz"), false));
    assert!(!super::cloud_path_should_download(Some("parquet"), false));
    assert!(!super::cloud_path_should_download(None, false));
    assert!(!super::cloud_path_should_download(Some("csv"), true));
    assert!(!super::cloud_path_should_download(Some("parquet"), true));
}
