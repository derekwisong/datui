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
