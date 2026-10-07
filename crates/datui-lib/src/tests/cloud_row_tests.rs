/// A source that cannot list says the problem once and what to do about it, rather
/// than `not signed in  not signed in` (#547 D5).
#[test]
fn a_source_not_signed_in_says_what_to_run() {
    let source = crate::cloud_sources::Source {
        id: "az".to_string(),
        label: "Azure".to_string(),
        kind: crate::cloud_browse::ProviderKind::Azure,
        tier: crate::cloud_sources::Tier::Tools,
        origin: "not signed in".to_string(),
        s3: Default::default(),
        azure: Default::default(),
        project: None,
        profile: None,
        buckets: Vec::new(),
        problem: Some("not signed in: run az login".to_string()),
        gcloud: None,
        secret_command: None,
        google_credentials: None,
    };
    let row = crate::home_app::home_cloud_source(&source, None, false);
    assert_eq!(row.count_text(), "not signed in");
    assert_eq!(row.note, "run az login");

    // A problem with nothing after the short word keeps its whole text.
    let source = crate::cloud_sources::Source {
        problem: Some("no credentials in AWS_PROFILE".to_string()),
        origin: "env".to_string(),
        ..source
    };
    let row = crate::home_app::home_cloud_source(&source, None, false);
    assert_eq!(row.count_text(), "not configured");
    assert_eq!(row.note, "no credentials in AWS_PROFILE");
}
