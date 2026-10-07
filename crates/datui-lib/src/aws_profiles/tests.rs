use super::*;
use std::path::Path;

const CONFIG: &str = r#"
[default]
region = us-east-1

[profile work]
region = eu-west-1

# An S3-compatible server reached through a profile
[profile lab]
services = lab-s3
region = us-east-1

[services lab-s3]
s3 =
  endpoint_url = http://localhost:9000
  addressing_style = path

[profile onprem]
endpoint_url = https://minio.corp.example:9000

[profile sso]
sso_session = corp
sso_account_id = 123456789012
sso_role_name = ReadOnly

[sso-session corp]
sso_start_url = https://corp.awsapps.com/start

[profile vault]
credential_process = aws-vault export --format=json vault-inner

[profile regional-only]
region = ap-south-1
"#;

const CREDENTIALS: &str = "
[default]
aws_access_key_id = AKIADEFAULT
aws_secret_access_key = default-secret

[work]
aws_access_key_id = AKIAWORK
aws_secret_access_key = work-secret
aws_session_token = work-token

[lab]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadmin
";

fn by_name<'a>(profiles: &'a [Profile], name: &str) -> &'a Profile {
    profiles
        .iter()
        .find(|p| p.name == name)
        .unwrap_or_else(|| panic!("{name} in {profiles:?}"))
}

fn no_vars(_: &str) -> Option<String> {
    None
}

#[test]
fn profiles_merge_both_files() {
    let profiles = parse(CONFIG, CREDENTIALS);
    assert_eq!(profiles[0].name, "default");
    let work = by_name(&profiles, "work");
    assert_eq!(work.region.as_deref(), Some("eu-west-1"));
    assert_eq!(work.access_key_id.as_deref(), Some("AKIAWORK"));
    assert_eq!(work.session_token.as_deref(), Some("work-token"));
    assert!(work.has_credentials() && !work.needs_cli);

    assert!(by_name(&profiles, "sso").needs_cli);
    assert_eq!(
        by_name(&profiles, "vault").credential_process.as_deref(),
        Some("aws-vault export --format=json vault-inner")
    );
    assert!(!by_name(&profiles, "regional-only").has_credentials());
}

#[test]
fn endpoints_follow_the_sdk_precedence() {
    let profiles = parse(CONFIG, CREDENTIALS);
    let lab = by_name(&profiles, "lab");
    let onprem = by_name(&profiles, "onprem");
    assert_eq!(
        lab.s3_endpoint(&no_vars).as_deref(),
        Some("http://localhost:9000")
    );
    assert_eq!(
        onprem.s3_endpoint(&no_vars).as_deref(),
        Some("https://minio.corp.example:9000")
    );
    assert_eq!(by_name(&profiles, "work").s3_endpoint(&no_vars), None);

    let general = |key: &str| (key == "AWS_ENDPOINT_URL").then(|| "http://general".to_string());
    assert_eq!(lab.s3_endpoint(&general).as_deref(), Some("http://general"));
    let both = |key: &str| match key {
        "AWS_ENDPOINT_URL_S3" => Some("http://s3-only".to_string()),
        "AWS_ENDPOINT_URL" => Some("http://general".to_string()),
        _ => None,
    };
    assert_eq!(lab.s3_endpoint(&both).as_deref(), Some("http://s3-only"));
    let ignored =
        |key: &str| (key == "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS").then(|| "true".to_string());
    assert_eq!(lab.s3_endpoint(&ignored), None);
}

fn environment<'a>(
    var: &'a dyn Fn(&str) -> Option<String>,
    run: &'a crate::cloud_command::Runner<'a>,
) -> Environment<'a> {
    Environment {
        var,
        exists: &|_: &Path| false,
        read: &|_: &Path| None,
        home: Some(PathBuf::from("/home/u")),
        windows: false,
        run,
        all_vars: &|| Vec::new(),
        list: &|_| Vec::new(),
    }
}

#[test]
fn the_files_can_be_moved_and_the_active_profile_named() {
    let var = |key: &str| match key {
        "AWS_CONFIG_FILE" => Some("/etc/aws/config".to_string()),
        "AWS_PROFILE" => Some("work".to_string()),
        _ => None,
    };
    let run = |_: &str, _: &[&str]| Err(CommandError::Missing("aws".to_string()));
    let env = environment(&var, &run);
    assert_eq!(config_path(&env), Some(PathBuf::from("/etc/aws/config")));
    assert_eq!(
        credentials_path(&env),
        Some(PathBuf::from("/home/u/.aws/credentials"))
    );
    assert_eq!(active_profile(&env), "work");
}

#[test]
fn keys_come_from_the_file_a_process_or_the_cli() {
    let profiles = parse(CONFIG, CREDENTIALS);
    let calls = std::sync::Mutex::new(Vec::<String>::new());
    let run = |program: &str, args: &[&str]| {
        calls
            .lock()
            .unwrap()
            .push(format!("{program} {}", args.join(" ")));
        Ok(
            r#"{"Version": 1, "AccessKeyId": "ASIATEMP", "SecretAccessKey": "temp-secret",
                   "SessionToken": "temp-token", "Expiration": "2999-01-01T00:00:00Z"}"#
                .to_string(),
        )
    };
    let env = environment(&no_vars, &run);

    let work = credentials(by_name(&profiles, "work"), &env).unwrap();
    assert_eq!(work.access_key_id, "AKIAWORK");

    let sso = credentials(by_name(&profiles, "sso"), &env).unwrap();
    assert_eq!(sso.access_key_id, "ASIATEMP");
    assert_eq!(sso.session_token.as_deref(), Some("temp-token"));
    // Cached until close to expiry: a second open runs nothing.
    credentials(by_name(&profiles, "sso"), &env).unwrap();

    let vault = credentials(by_name(&profiles, "vault"), &env).unwrap();
    assert_eq!(vault.secret_access_key, "temp-secret");

    assert_eq!(
        *calls.lock().unwrap(),
        [
            "aws configure export-credentials --profile sso --format process",
            "aws-vault export --format=json vault-inner",
        ]
    );
}

#[test]
fn a_missing_cli_or_a_failed_login_says_so() {
    let profiles = parse(CONFIG, CREDENTIALS);
    let missing = |_: &str, _: &[&str]| Err(CommandError::Missing("aws".to_string()));
    let env = environment(&no_vars, &missing);
    let mut sso = by_name(&profiles, "sso").clone();
    sso.name = "sso-missing-cli".to_string();
    assert_eq!(
        credentials(&sso, &env),
        Err("needs the AWS CLI".to_string())
    );

    let expired = |_: &str, _: &[&str]| {
        Err(CommandError::Failed(
            "Error loading SSO Token: Token for corp does not exist".to_string(),
        ))
    };
    let env = environment(&no_vars, &expired);
    sso.name = "sso-expired".to_string();
    assert!(
        credentials(&sso, &env)
            .unwrap_err()
            .contains("Token for corp does not exist")
    );
    assert!(credentials(by_name(&profiles, "regional-only"), &env).is_err());
}

#[test]
fn process_output_needs_both_keys() {
    assert!(parse_process_output(r#"{"Version": 1, "AccessKeyId": "A"}"#).is_none());
    let parsed =
        parse_process_output(r#"{"Version":1,"AccessKeyId":"A","SecretAccessKey":"S"}"#).unwrap();
    assert_eq!(parsed.session_token, None);
    assert_eq!(parsed.expires, None);
}
