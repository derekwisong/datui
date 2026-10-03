use crate::*;

/// A spec asked for on an object store path scanned in place is refused, saying
/// why, rather than dropped; a local path and a plain remote open are not.
#[test]
fn a_spec_on_a_remote_path_is_refused() {
    let asked = [
        OpenOptions {
            spec_name: Some("acme.l2feed".into()),
            ..OpenOptions::default()
        },
        OpenOptions {
            spec_file: Some(PathBuf::from("l2feed.toml")),
            ..OpenOptions::default()
        },
    ];
    for url in ["s3://bucket/day.l2", "gs://bucket/day.l2", "az://c/day.l2"] {
        for options in &asked {
            let e = App::build_lazyframe_from_paths_with(
                &crate::config::CloudConfig::default(),
                &[PathBuf::from(url)],
                options,
                &mut ReadReport::default(),
                &crate::formats::Registry::default(),
            )
            .err()
            .unwrap_or_else(|| panic!("{url} opened"));
            assert!(
                e.to_string().contains("Format specs read local files"),
                "{url}: {e}"
            );
        }
    }
    assert!(App::refuse_spec_in_place(Path::new("/data/day.l2"), &asked[0]).is_ok());
    assert!(
        App::refuse_spec_in_place(Path::new("s3://bucket/day.l2"), &OpenOptions::default()).is_ok()
    );
}

/// Refusals made before a reader is chosen name the file, in the one shape: `--table`
/// on a file of one table, and a format spec that does not parse, at its line and
/// column.
#[test]
fn refusals_name_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("a.csv");
    std::fs::write(&data, "a\n1\n").unwrap();
    let spec = dir.path().join("bad.toml");
    std::fs::write(&spec, "name = \"acme.l2\"\n[records]\nfields = 3\n").unwrap();
    let open = |options: &OpenOptions| {
        let e = App::build_lazyframe_from_paths_with(
            &crate::config::CloudConfig::default(),
            std::slice::from_ref(&data),
            options,
            &mut ReadReport::default(),
            &crate::formats::Registry::default(),
        )
        .err()
        .expect("refused");
        crate::error_display::user_message_from_report(&e, Some(&data))
    };
    let table = open(&OpenOptions {
        table: Some("x".into()),
        ..OpenOptions::default()
    });
    eprintln!("{table}");
    crate::readers::bad_input::assert_shape(&table, &data);
    assert!(table.contains("--table picks one from"), "{table}");

    let spec_said = open(&OpenOptions {
        spec_file: Some(spec.clone()),
        ..OpenOptions::default()
    });
    eprintln!("{spec_said}");
    crate::readers::bad_input::assert_shape(&spec_said, &spec);
    assert!(spec_said.contains(":3:"), "{spec_said}");
}
