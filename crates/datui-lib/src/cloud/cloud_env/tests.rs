use super::*;

#[test]
fn only_cloud_variables_are_read() {
    let text = "# a project's .env\n\
                    DATABASE_URL=postgres://secret\n\
                    export AWS_ACCESS_KEY_ID=AKIAFILE\n\
                    AWS_SECRET_ACCESS_KEY=\"with spaces\" # quoted\n\
                    AZURE_STORAGE_ACCOUNT_NAME=acct # trailing comment\n\
                    MC_HOST_lab=http://k:s@127.0.0.1:9000\n\
                    ONPREM_KEY='single'\n";
    let allowed = |k: &str| KNOWN.contains(&k) || k.starts_with("MC_HOST_") || k == "ONPREM_KEY";
    let parsed: HashMap<String, String> = parse(text, &allowed).into_iter().collect();
    assert_eq!(parsed.get("DATABASE_URL"), None);
    assert_eq!(parsed["AWS_ACCESS_KEY_ID"], "AKIAFILE");
    assert_eq!(parsed["AWS_SECRET_ACCESS_KEY"], "with spaces");
    assert_eq!(parsed["AZURE_STORAGE_ACCOUNT_NAME"], "acct");
    assert_eq!(parsed["MC_HOST_lab"], "http://k:s@127.0.0.1:9000");
    assert_eq!(parsed["ONPREM_KEY"], "single");
}

#[test]
fn files_are_found_relative_to_the_working_directory() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(
        dir.path().join(".env"),
        "DATUI_ENV_FILE_TEST_ONLY=x\nONPREM_SECRET_FOR_ENV_FILE_TEST=s3cr3t\n",
    )
    .unwrap();
    let cloud = CloudConfig {
        env_files: vec![".env".to_string(), "missing.env".to_string()],
        connections: vec![crate::config::CloudConnectionConfig {
            name: "onprem".to_string(),
            secret_access_key_env: Some("ONPREM_SECRET_FOR_ENV_FILE_TEST".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let notes = load(&cloud, dir.path());
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert!(notes[0].contains("missing.env"));
    assert_eq!(
        var("ONPREM_SECRET_FOR_ENV_FILE_TEST").as_deref(),
        Some("s3cr3t")
    );
    assert_eq!(
        var("DATUI_ENV_FILE_TEST_ONLY"),
        None,
        "not a cloud variable"
    );
    assert!(
        std::env::var("ONPREM_SECRET_FOR_ENV_FILE_TEST").is_err(),
        "never exported"
    );
    load(&CloudConfig::default(), dir.path());
}
