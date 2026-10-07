use super::*;
use std::collections::HashMap;

fn detect_with(vars: &[(&str, &str)]) -> Vec<Provider> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let env = Environment {
        var: &|key| vars.get(key).cloned(),
        exists: &|_| false,
        read: &|_| None,
        home: Some(PathBuf::from("/home/u")),
        windows: false,
        run: &|_, _| {
            Err(crate::cloud_command::CommandError::Missing(
                "test".to_string(),
            ))
        },
        all_vars: &|| Vec::new(),
        list: &|_| Vec::new(),
    };
    detect(&CloudConfig::default(), &env)
}

#[test]
fn a_gcloud_login_on_windows_is_found_under_appdata() {
    let vars: HashMap<String, String> = [("APPDATA", r"C:\Users\u\AppData\Roaming")]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let adc = PathBuf::from(r"C:\Users\u\AppData\Roaming")
        .join("gcloud")
        .join("application_default_credentials.json");
    let under_home = PathBuf::from(r"C:\Users\u")
        .join(".config")
        .join("gcloud")
        .join("application_default_credentials.json");
    let windows = |exists: PathBuf, windows: bool| {
        let env = Environment {
            var: &|key| vars.get(key).cloned(),
            exists: &|path| path == exists,
            read: &|_| None,
            home: Some(PathBuf::from(r"C:\Users\u")),
            windows,
            run: &|_, _| {
                Err(crate::cloud_command::CommandError::Missing(
                    "test".to_string(),
                ))
            },
            all_vars: &|| Vec::new(),
            list: &|_| Vec::new(),
        };
        detect(&CloudConfig::default(), &env)
            .iter()
            .any(|p| p.kind == ProviderKind::Gcs)
    };
    assert!(windows(adc.clone(), true));
    // The Unix location means nothing on Windows, and the reverse.
    assert!(!windows(under_home, true));
    assert!(!windows(adc, false));
}

#[test]
fn an_ecs_task_role_is_enough() {
    // Fargate's usual shape: no key anywhere, credentials fetched from a loopback
    // endpoint. A check for keys and ~/.aws finds nothing on exactly the machines
    // most likely to be reading from S3.
    for key in [
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    ] {
        let found = detect_with(&[(key, "/v2/credentials/abc")]);
        assert_eq!(found.len(), 1, "{key} should be enough");
        assert_eq!(found[0].kind, ProviderKind::S3);
        assert_eq!(found[0].label, "Amazon S3");
        assert_eq!(found[0].note, "container role");
    }
}

#[test]
fn an_eks_web_identity_is_enough() {
    let found = detect_with(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/token"),
        ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
    ]);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].note, "web identity");
}

#[test]
fn a_region_alone_is_not_a_credential() {
    // Worth pinning down. A region is configuration, not authorisation, and a
    // provider listed on the strength of one would fail on every Enter.
    assert!(detect_with(&[("AWS_REGION", "us-east-1")]).is_empty());
    assert!(detect_with(&[("AWS_DEFAULT_REGION", "us-east-1")]).is_empty());
}

#[test]
fn a_key_still_outranks_a_role_in_the_note() {
    let found = detect_with(&[
        ("AWS_ACCESS_KEY_ID", "AKIA"),
        ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/creds"),
    ]);
    assert_eq!(found[0].note, "AWS_ACCESS_KEY_ID");
}
