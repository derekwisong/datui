use super::*;
use crate::cloud::cloud_command::CommandError;
use std::path::{Path, PathBuf};

fn minio(name: &str, endpoint: &str) -> CloudConnectionConfig {
    CloudConnectionConfig {
        name: name.to_string(),
        kind: Some("s3".to_string()),
        endpoint_url: Some(endpoint.to_string()),
        access_key_id_env: Some(format!("{}_KEY", name.to_uppercase())),
        secret_access_key_env: Some(format!("{}_SECRET", name.to_uppercase())),
        ..Default::default()
    }
}

/// A machine described by literals: environment variables, the text of files by
/// path, and a runner that fails as if nothing were installed.
struct Machine {
    vars: HashMap<String, String>,
    files: HashMap<PathBuf, String>,
}

impl Machine {
    fn new(vars: &[(&str, &str)], files: &[(&str, &str)]) -> Self {
        Machine {
            vars: vars
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            files: files
                .iter()
                .map(|(p, t)| (PathBuf::from(p), t.to_string()))
                .collect(),
        }
    }
}

fn with_machine<T>(machine: &Machine, body: impl FnOnce(&Environment<'_>) -> T) -> T {
    let var = |key: &str| machine.vars.get(key).cloned();
    let exists = |path: &Path| machine.files.contains_key(path);
    let read = |path: &Path| machine.files.get(path).cloned();
    let run = |program: &str, _: &[&str]| Err(CommandError::Missing(program.to_string()));
    let all_vars = || {
        machine
            .vars
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let list = |dir: &Path| {
        machine
            .files
            .keys()
            .filter(|path| path.parent() == Some(dir))
            .cloned()
            .collect()
    };
    let env = Environment {
        var: &var,
        exists: &exists,
        read: &read,
        home: Some(PathBuf::from("/home/u")),
        windows: false,
        run: &run,
        all_vars: &all_vars,
        list: &list,
    };
    body(&env)
}

#[test]
fn two_servers_with_the_same_bucket_resolve_to_their_own_endpoints() {
    let config = CloudConfig {
        connections: vec![
            minio("lab", "http://127.0.0.1:9000"),
            minio("onprem", "https://minio.corp.example:9000"),
        ],
        ..Default::default()
    };
    let machine = Machine::new(
        &[
            ("LAB_KEY", "lab-key"),
            ("LAB_SECRET", "lab-secret"),
            ("ONPREM_KEY", "corp-key"),
            ("ONPREM_SECRET", "corp-secret"),
        ],
        &[],
    );
    with_machine(&machine, |env| {
        let lab = resolve_with("s3://lab@data/sales.parquet", &config, env).unwrap();
        let corp = resolve_with("s3://onprem@data/sales.parquet", &config, env).unwrap();
        assert_eq!(lab.url, "s3://data/sales.parquet");
        assert_eq!(corp.url, "s3://data/sales.parquet");
        assert_eq!(lab.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(
            corp.s3.endpoint.as_deref(),
            Some("https://minio.corp.example:9000")
        );
        assert_eq!(lab.s3.access_key_id.as_deref(), Some("lab-key"));
        assert_eq!(corp.s3.secret_access_key.as_deref(), Some("corp-secret"));
        assert!(!lab.s3.from_env && !lab.s3.virtual_hosted_style());
    });
}

#[test]
fn a_plain_url_is_the_default_source_as_before() {
    let config = CloudConfig {
        s3_endpoint_url: Some("http://localhost:9000".to_string()),
        s3_access_key_id: Some("key".to_string()),
        connections: vec![minio("lab", "http://127.0.0.1:9000")],
        ..Default::default()
    };
    with_machine(&Machine::new(&[], &[]), |env| {
        let resolved = resolve_with("s3://data/key.parquet", &config, env).unwrap();
        assert_eq!(resolved.source_id, DEFAULT_S3);
        assert_eq!(resolved.url, "s3://data/key.parquet");
        assert_eq!(
            resolved.s3.endpoint.as_deref(),
            Some("http://localhost:9000")
        );
        assert!(resolved.s3.from_env);
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("key"));
        assert_eq!(resolved.signing, Signing::Try);
    });
}

#[test]
fn with_no_login_for_a_provider_its_urls_are_read_unsigned() {
    let config = CloudConfig {
        s3_endpoint_url: Some("http://localhost:9000".to_string()),
        ..Default::default()
    };
    with_machine(&Machine::new(&[], &[]), |env| {
        let s3 = resolve_with("s3://nologin-data/key.parquet", &config, env).unwrap();
        assert_eq!(s3.signing, Signing::Unsigned);
        assert!(s3.s3.skip_signature && !s3.s3.from_env);
        assert_eq!(
            s3.s3.endpoint.as_deref(),
            Some("http://localhost:9000"),
            "still sent to the configured server"
        );
        let gcs = resolve_with("gs://nologin-bucket/key", &config, env).unwrap();
        assert_eq!(gcs.source_id, DEFAULT_GCS);
        assert_eq!(gcs.signing, Signing::Unsigned);
        let azure = resolve_with(
            "abfss://c@nologinacct.dfs.core.windows.net/k.parquet",
            &config,
            env,
        )
        .unwrap();
        assert_eq!(azure.signing, Signing::Unsigned);
        assert_eq!(azure.azure.auth, crate::cloud::azure::AzureAuth::None);
    });
}

#[test]
fn a_login_that_may_not_own_the_place_tries_then_remembers() {
    let machine = Machine::new(
        &[
            ("AWS_ACCESS_KEY_ID", "AKIA"),
            ("AWS_SECRET_ACCESS_KEY", "s"),
        ],
        &[],
    );
    with_machine(&machine, |env| {
        let config = CloudConfig::from_env(env.var);
        let first = resolve_with("s3://tries-bucket/a.parquet", &config, env).unwrap();
        assert_eq!(first.signing, Signing::Try);
        assert_eq!(first.place, "s3://tries-bucket");
        assert_eq!(first.s3.access_key_id.as_deref(), Some("AKIA"));

        let unsigned = first.unsigned();
        assert!(unsigned.s3.skip_signature);
        assert_eq!(unsigned.s3.access_key_id, None, "no key goes with it");

        remember_access("s3://tries-bucket", true);
        let again = resolve_with("s3://tries-bucket/b/c.parquet", &config, env).unwrap();
        assert_eq!(again.signing, Signing::Unsigned);

        remember_access("s3://signed-bucket", false);
        let signed = resolve_with("s3://signed-bucket/x", &config, env).unwrap();
        assert_eq!(signed.signing, Signing::Signed);
    });
}

/// The cloud settings of a config built from `toml`, with `catalog` as a listed
/// catalog named `id`, and the machine's environment.
fn with_catalog(toml: &str, id: &str, catalog: &str, env: &Environment<'_>) -> CloudConfig {
    let mut app: crate::config::AppConfig = toml::from_str(toml).unwrap();
    if !catalog.is_empty() {
        app.read_catalogs =
            vec![crate::catalog::parse(catalog, id, crate::catalog::Origin::Listed, None).unwrap()];
    }
    app.sync_dataset_access();
    app.validate().unwrap();
    let mut cloud = app.cloud.clone();
    cloud.overlay(CloudConfig::from_env(env.var));
    cloud
}

#[test]
fn the_builtin_catalog_is_read_anonymously_whoever_is_logged_in() {
    let machine = Machine::new(
        &[
            ("AWS_ACCESS_KEY_ID", "AKIA"),
            ("AWS_SECRET_ACCESS_KEY", "s"),
        ],
        &[],
    );
    with_machine(&machine, |env| {
        let config = with_catalog("", "", "", env);
        let catalog = crate::catalog::bundled();
        assert!(catalog.datasets.len() >= 6);
        // Web files are fetched, not resolved against a store.
        for dataset in &catalog.datasets {
            let url = dataset.url.as_deref().unwrap();
            if !crate::config::is_object_store_dataset(url) {
                continue;
            }
            let resolved = resolve_with(url, &config, env).unwrap();
            assert_eq!(resolved.signing, Signing::Unsigned, "{url}");
            assert_eq!(resolved.source_id, crate::catalog::EXAMPLES);
            assert_eq!(resolved.s3.access_key_id, None);
        }
        let inside = resolve_with(
            "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2020/",
            &config,
            env,
        )
        .unwrap();
        assert_eq!(inside.signing, Signing::Unsigned);
        // The rest of the bucket is not a dataset.
        let beside = resolve_with("s3://noaa-ghcn-pds/csv/", &config, env).unwrap();
        assert_eq!(beside.signing, Signing::Try);

        // An examples.toml listed replaces the bundled catalog.
        let off = with_catalog(
            "",
            "examples",
            "[w]\nname = \"W\"\nurl = \"s3://other-bucket/w/\"\n",
            env,
        );
        let resolved = resolve_with("s3://noaa-ghcn-pds/parquet/", &off, env).unwrap();
        assert_eq!(resolved.signing, Signing::Try, "no catalog, no claim on it");
        // No home-screen row stands for a catalog: they are sections.
        assert!(discover(&config, env).iter().all(|s| s.id != "examples"));
    });
}

#[test]
fn anonymous_datasets_on_any_provider_are_read_unsigned() {
    with_machine(&Machine::new(&[], &[]), |env| {
        let config = with_catalog(
            "",
            "open",
            r#"
[gbif]
name = "GBIF"
url = "s3://gbif-open-data-us-east-1/occurrence/"
auth = "anonymous"
[taxis]
name = "Taxis"
url = "https://azureopendatastorage.blob.core.windows.net/nyctlc/"
auth = "anonymous"
[samples]
name = "Samples"
url = "gs://cloud-samples-data/bigquery/"
"#,
            env,
        );
        let azure = resolve_with(
            "abfss://nyctlc@azureopendatastorage.dfs.core.windows.net/yellow/",
            &config,
            env,
        )
        .unwrap();
        assert_eq!(azure.source_id, "open");
        assert_eq!(azure.signing, Signing::Unsigned);
        let s3 = resolve_with("s3://gbif-open-data-us-east-1/occurrence/x", &config, env).unwrap();
        assert_eq!(
            (s3.source_id.as_str(), s3.signing),
            ("open", Signing::Unsigned)
        );
        // `auth` unset reads as any URL typed at the prompt would be.
        let auto = resolve_with("gs://cloud-samples-data/bigquery/x", &config, env).unwrap();
        assert_eq!(auto.source_id, DEFAULT_GCS);
    });
}

#[test]
fn a_dataset_names_the_connection_that_signs_it() {
    let machine = Machine::new(
        &[
            ("AWS_ACCESS_KEY_ID", "env-key"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
            ("LAB_KEY", "lab-key"),
            ("LAB_SECRET", "lab-secret"),
        ],
        &[],
    );
    with_machine(&machine, |env| {
        let config = with_catalog(
            r#"
[[cloud.connections]]
name = "lab"
kind = "s3"
endpoint_url = "http://127.0.0.1:9000"
access_key_id_env = "LAB_KEY"
secret_access_key_env = "LAB_SECRET"
"#,
            "team",
            r#"
[sales]
name = "Sales"
url = "s3://connection-sales/2024/"
connection = "lab"
"#,
            env,
        );
        // An earlier unsigned read does not overrule the connection the config names.
        remember_access("s3://connection-sales", true);
        let sales = resolve_with("s3://connection-sales/2024/q1.parquet", &config, env).unwrap();
        assert_eq!(sales.source_id, "lab");
        assert_eq!(sales.signing, Signing::Signed);
        assert_eq!(sales.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(sales.s3.access_key_id.as_deref(), Some("lab-key"));
        // Beside the dataset, the bucket is anybody's.
        let beside = resolve_with("s3://connection-sales/2023/", &config, env).unwrap();
        assert_eq!(beside.source_id, DEFAULT_S3);
    });
}

#[test]
fn gcloud_configurations_are_logins() {
    let dir = "/home/u/.config/gcloud";
    let machine = Machine::new(
        &[],
        &[
            (&format!("{dir}/active_config") as &str, "work\n"),
            (
                &format!("{dir}/configurations/config_work"),
                "[core]\naccount = a@example.com\nproject = analytics\n",
            ),
            (
                &format!("{dir}/configurations/config_other-project"),
                "[core]\naccount = a@example.com\nproject = billing\n",
            ),
            (
                &format!("{dir}/configurations/config_Personal"),
                "[core]\naccount = me@example.org\n",
            ),
            (&format!("{dir}/configurations/config_empty"), "[core]\n"),
        ],
    );
    with_machine(&machine, |env| {
        let found = discover(&CloudConfig::default(), env);
        let google: Vec<(&str, Option<&str>, Option<&str>)> = found
            .iter()
            .filter(|s| s.kind == ProviderKind::Gcs)
            .map(|s| (s.id.as_str(), s.gcloud.as_deref(), s.project.as_deref()))
            .collect();
        // Only `gcloud auth login`: the active configuration is the default login,
        // one more account is a source, and a second configuration of the same
        // account is not.
        assert_eq!(
            google,
            [
                (DEFAULT_GCS, Some("work"), Some("analytics")),
                ("gcloud-personal", Some("Personal"), None),
            ]
        );
        // Without gcloud the login cannot sign; the bucket is tried unsigned, and the
        // reason is kept for when it turns out not to be public.
        let resolved =
            resolve_with("gs://some-bucket/key.parquet", &CloudConfig::default(), env).unwrap();
        assert_eq!(resolved.signing, Signing::Unsigned);
        let err = resolved.login_error.unwrap_or_default();
        assert!(err.contains("needs gcloud"), "{err}");
    });
}

#[test]
fn a_google_login_object_store_cannot_read_goes_through_gcloud() {
    let adc = "/home/u/.config/gcloud/application_default_credentials.json";
    let federated = r#"{"type": "external_account", "audience": "//iam.googleapis.com/x"}"#;
    let with_gcloud = Machine::new(
        &[],
        &[
            (adc, federated),
            (
                "/home/u/.config/gcloud/configurations/config_default",
                "[core]\naccount = a@example.com\n",
            ),
        ],
    );
    with_machine(&with_gcloud, |env| {
        let google = discover(&CloudConfig::default(), env)
            .into_iter()
            .find(|s| s.id == DEFAULT_GCS)
            .unwrap();
        assert_eq!(google.gcloud.as_deref(), Some("default"));
        assert_eq!(google.problem, None);
    });
    let without = Machine::new(&[], &[(adc, federated)]);
    with_machine(&without, |env| {
        let google = discover(&CloudConfig::default(), env)
            .into_iter()
            .find(|s| s.id == DEFAULT_GCS)
            .unwrap();
        assert_eq!(
            google.problem.as_deref(),
            Some("unsupported login: external_account")
        );
    });
}

#[test]
fn a_configured_google_source_names_its_configuration_and_project() {
    with_machine(&Machine::new(&[], &[]), |env| {
        let config = CloudConfig {
            connections: vec![CloudConnectionConfig {
                name: "research".to_string(),
                kind: Some("gcs".to_string()),
                configuration: Some("research".to_string()),
                project: Some("research-prod".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let source = discover(&config, env)
            .into_iter()
            .find(|s| s.id == "research")
            .unwrap();
        assert_eq!(source.gcloud.as_deref(), Some("research"));
        assert_eq!(source.project.as_deref(), Some("research-prod"));
        assert_eq!(
            source.bucket_url("research-prod"),
            "cloud://research/research-prod"
        );
    });
}

#[test]
fn configured_azure_sources() {
    let azure = |name: &str| CloudConnectionConfig {
        name: name.to_string(),
        kind: Some("azure".to_string()),
        account: Some(format!("{name}acct")),
        ..Default::default()
    };
    let config = CloudConfig {
        connections: vec![
            CloudConnectionConfig {
                account_key_env: Some("RESEARCH_KEY".to_string()),
                ..azure("research")
            },
            CloudConnectionConfig {
                sas_env: Some("SHARED_SAS".to_string()),
                ..azure("shared")
            },
            CloudConnectionConfig {
                account: None,
                connection_string_env: Some("APP_STORAGE".to_string()),
                ..azure("app")
            },
            azure("signin"),
            CloudConnectionConfig {
                account_key_env: Some("UNSET_KEY".to_string()),
                ..azure("broken")
            },
        ],
        ..Default::default()
    };
    let machine = Machine::new(
        &[
            ("RESEARCH_KEY", "a2V5"),
            ("SHARED_SAS", "?sv=2024&sig=x"),
            (
                "APP_STORAGE",
                "DefaultEndpointsProtocol=https;AccountName=appdata;AccountKey=a2V5;EndpointSuffix=core.windows.net",
            ),
        ],
        &[],
    );
    with_machine(&machine, |env| {
        let found = discover(&config, env);
        let get = |id: &str| found.iter().find(|s| s.id == id).unwrap();
        use crate::cloud::azure::AzureAuth;
        assert_eq!(
            get("research").azure.auth,
            AzureAuth::Key("a2V5".to_string())
        );
        assert_eq!(
            get("shared").azure.auth,
            AzureAuth::Sas("sv=2024&sig=x".to_string())
        );
        assert_eq!(get("app").azure.account.as_deref(), Some("appdata"));
        assert_eq!(get("signin").azure.auth, AzureAuth::AzCli);
        assert_eq!(
            get("broken").problem.as_deref(),
            Some("UNSET_KEY is not set")
        );
        assert!(
            found
                .iter()
                .all(|s| s.kind != ProviderKind::Azure || !s.named_in_urls())
        );

        // A key fetched after a 403 is used for the account from then on.
        crate::cloud::azure::remember_key_for_test("signinacct", "a2V5Mg==");
        let resolved = resolve_with(
            "abfss://data@signinacct.dfs.core.windows.net/x.parquet",
            &config,
            env,
        )
        .unwrap();
        assert_eq!(resolved.source_id, "signin");
        assert_eq!(resolved.azure.auth, AzureAuth::Key("a2V5Mg==".to_string()));
        assert_eq!(resolved.azure.identity, Some(AzureAuth::AzCli));
    });
}

#[test]
fn azure_tools_not_signed_in_do_not_block_public_containers() {
    let machine = Machine::new(&[("PATH", "/usr/bin")], &[("/usr/bin/az", "")]);
    with_machine(&machine, |env| {
        let config = CloudConfig::default();
        let az = discover(&config, env)
            .into_iter()
            .find(|s| s.id == DEFAULT_AZURE_LOGIN)
            .expect("a not signed in row");
        assert!(az.problem.as_deref().unwrap().starts_with("not signed in"));
        let resolved = resolve_with(
            "abfss://nyctlc@azureopendatastorage.dfs.core.windows.net/yellow/",
            &config,
            env,
        )
        .unwrap();
        assert_eq!(resolved.signing, Signing::Unsigned);
    });
    // Signed in by the look of it, but `az` cannot give a token: still readable.
    let expired = Machine::new(&[], &[("/home/u/.azure", "")]);
    with_machine(&expired, |env| {
        let resolved = resolve_with(
            "abfss://release@overturemapswestus2.dfs.core.windows.net/x/",
            &CloudConfig::default(),
            env,
        )
        .unwrap();
        assert_eq!(resolved.signing, Signing::Unsigned);
    });
}

#[test]
fn azure_urls_without_an_account() {
    let config = CloudConfig {
        connections: vec![CloudConnectionConfig {
            name: "research".to_string(),
            kind: Some("azure".to_string()),
            account: Some("datuiresearch".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let expand = |url: &str, browsing: Option<&str>| {
        expand_azure_short_url(Path::new(url), &config, browsing.map(Path::new))
    };
    assert_eq!(
        expand("az://raw/2024/a.parquet", None).unwrap(),
        PathBuf::from("abfss://raw@datuiresearch.dfs.core.windows.net/2024/a.parquet")
    );
    assert_eq!(
        expand("adl://raw/x.csv", Some("cloud://az/lake001")).unwrap(),
        PathBuf::from("abfss://raw@lake001.dfs.core.windows.net/x.csv"),
        "typed inside an account"
    );
    assert_eq!(
        expand("azure://raw@other.blob.core.windows.net/x.csv", None).unwrap(),
        PathBuf::from("abfss://raw@other.dfs.core.windows.net/x.csv")
    );
    assert_eq!(
        expand("s3://bucket/key", None).unwrap(),
        PathBuf::from("s3://bucket/key")
    );
    let none = expand_azure_short_url(Path::new("az://raw/x.csv"), &CloudConfig::default(), None);
    if std::env::var("AZURE_STORAGE_ACCOUNT_NAME").is_err()
        && std::env::var("AZURE_STORAGE_CONNECTION_STRING").is_err()
    {
        assert!(none.unwrap_err().contains("abfss://raw@<account>"));
    }
}

/// A machine whose runner answers `pass show …` with a secret, and fails otherwise.
fn with_secret_runner<T>(machine: &Machine, body: impl FnOnce(&Environment<'_>) -> T) -> T {
    let var = |key: &str| machine.vars.get(key).cloned();
    let exists = |path: &Path| machine.files.contains_key(path);
    let read = |path: &Path| machine.files.get(path).cloned();
    let run = |program: &str, args: &[&str]| match (program, args) {
        ("pass", ["show", "minio/onprem"]) => Ok("s3cr3t-from-pass\n".to_string()),
        ("op", ["read", "op://vault/azure/key"]) => Ok("YWNjb3VudC1rZXk=".to_string()),
        ("pass", _) => Err(CommandError::Failed(
            "Error: minio/missing is not in the password store.".to_string(),
        )),
        _ => Err(CommandError::Missing(program.to_string())),
    };
    let all_vars = Vec::new;
    let list = |_: &Path| Vec::new();
    body(&Environment {
        var: &var,
        exists: &exists,
        read: &read,
        home: Some(PathBuf::from("/home/u")),
        windows: false,
        run: &run,
        all_vars: &all_vars,
        list: &list,
    })
}

#[test]
fn secret_commands_supply_the_secret() {
    let config = CloudConfig {
        connections: vec![
            CloudConnectionConfig {
                secret_access_key_env: None,
                secret_command: Some("pass show minio/onprem".to_string()),
                ..minio("onprem", "https://minio.corp.example:9000")
            },
            CloudConnectionConfig {
                secret_access_key_env: None,
                secret_command: Some("pass show minio/missing".to_string()),
                ..minio("broken", "https://minio.corp.example:9000")
            },
            CloudConnectionConfig {
                name: "research".to_string(),
                kind: Some("azure".to_string()),
                account: Some("research".to_string()),
                secret_command: Some("op read op://vault/azure/key".to_string()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let machine = Machine::new(&[("ONPREM_KEY", "AKIAONPREM"), ("BROKEN_KEY", "k")], &[]);
    with_secret_runner(&machine, |env| {
        let found = discover(&config, env);
        let onprem = found.iter().find(|s| s.id == "onprem").unwrap();
        assert_eq!(
            onprem.problem, None,
            "the command runs when the source is used"
        );
        let resolved = resolve_with("s3://onprem@data/x.parquet", &config, env).unwrap();
        assert_eq!(
            resolved.s3.secret_access_key.as_deref(),
            Some("s3cr3t-from-pass")
        );
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIAONPREM"));

        let err = resolve_with("s3://broken@data/x.parquet", &config, env).unwrap_err();
        assert!(
            err.contains("secret_command failed: Error: minio/missing"),
            "{err}"
        );

        let research = found.iter().find(|s| s.id == "research").unwrap();
        let settings = research.azure.clone().with_token(env).unwrap();
        assert_eq!(
            settings.auth,
            crate::cloud::azure::AzureAuth::Key("YWNjb3VudC1rZXk=".to_string())
        );
    });
}

#[test]
fn a_credentials_file_logs_a_google_source_in() {
    let config = CloudConfig {
        connections: vec![
            CloudConnectionConfig {
                name: "analytics".to_string(),
                kind: Some("gcs".to_string()),
                credentials_file: Some("~/keys/analytics-sa.json".to_string()),
                ..Default::default()
            },
            CloudConnectionConfig {
                name: "gone".to_string(),
                kind: Some("gcs".to_string()),
                credentials_file: Some("/nowhere/sa.json".to_string()),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let machine = Machine::new(
        &[],
        &[(
            "/home/u/keys/analytics-sa.json",
            r#"{"type": "service_account", "project_id": "analytics-prod"}"#,
        )],
    );
    with_machine(&machine, |env| {
        let found = discover(&config, env);
        let analytics = found.iter().find(|s| s.id == "analytics").unwrap();
        assert_eq!(
            analytics.google_credentials.as_deref(),
            Some(Path::new("/home/u/keys/analytics-sa.json"))
        );
        assert_eq!(analytics.project.as_deref(), Some("analytics-prod"));
        let gone = found.iter().find(|s| s.id == "gone").unwrap();
        assert!(gone.problem.as_deref().unwrap().contains("does not exist"));
    });
}

#[test]
fn instance_identity_only_when_asked_or_the_platform_says() {
    let ids = |config: &CloudConfig, vars: &[(&str, &str)]| -> Vec<(String, String)> {
        with_machine(&Machine::new(vars, &[]), |env| {
            discover(config, env)
                .into_iter()
                .map(|s| (s.id, s.origin))
                .collect()
        })
    };
    assert!(
        ids(&CloudConfig::default(), &[]).is_empty(),
        "nothing asks a metadata service"
    );
    let opted_in = CloudConfig {
        instance_identity: true,
        ..Default::default()
    };
    let found = ids(&opted_in, &[]);
    for (id, origin) in [
        (DEFAULT_S3, "instance role"),
        (DEFAULT_GCS, "instance identity"),
        (DEFAULT_AZURE_ENV, "managed identity"),
    ] {
        assert!(
            found.contains(&(id.to_string(), origin.to_string())),
            "{id} in {found:?}"
        );
    }
    assert_eq!(
        ids(&CloudConfig::default(), &[("K_SERVICE", "api")]),
        [(DEFAULT_GCS.to_string(), "instance identity".to_string())],
        "Cloud Run"
    );
    assert_eq!(
        ids(
            &CloudConfig::default(),
            &[("IDENTITY_ENDPOINT", "http://localhost:8081/msi/token")]
        ),
        [(
            DEFAULT_AZURE_ENV.to_string(),
            "managed identity".to_string()
        )],
        "App Service"
    );
    // Without the opt-in, a no-login URL is unsigned: no metadata request.
    with_machine(&Machine::new(&[], &[]), |env| {
        let resolved =
            resolve_with("s3://instance-test/key", &CloudConfig::default(), env).unwrap();
        assert_eq!(resolved.signing, Signing::Unsigned);
    });
}

#[test]
fn a_failed_login_that_may_not_own_the_place_reads_it_unsigned() {
    // A profile whose SSO session expired: `aws` fails.
    let machine = Machine::new(
        &[("AWS_PROFILE", "work")],
        &[(
            "/home/u/.aws/config",
            "[profile work]\nsso_session = corp\nregion = us-east-1\n",
        )],
    );
    let expired = |program: &str, _: &[&str]| -> Result<String, CommandError> {
        match program {
            "aws" => Err(CommandError::Failed(
                "Your session has expired. Please reauthenticate using 'aws login'.".to_string(),
            )),
            other => Err(CommandError::Missing(other.to_string())),
        }
    };
    let var = |key: &str| machine.vars.get(key).cloned();
    let exists = |path: &Path| machine.files.contains_key(path);
    let read = |path: &Path| machine.files.get(path).cloned();
    let all_vars = Vec::new;
    let list = |_: &Path| Vec::new();
    let env = Environment {
        var: &var,
        exists: &exists,
        read: &read,
        home: Some(PathBuf::from("/home/u")),
        windows: false,
        run: &expired,
        all_vars: &all_vars,
        list: &list,
    };
    let config = CloudConfig::from_env(env.var);
    // A bucket the login may not own: unsigned, with the reason kept.
    let public = resolve_with("s3://expired-login-public/x.parquet", &config, &env).unwrap();
    assert_eq!(public.signing, Signing::Unsigned);
    assert!(
        public
            .login_error
            .as_deref()
            .unwrap()
            .contains("session has expired")
    );
    // One it owns (listed by it) still reports the login.
    remember_access("s3://expired-login-owned", false);
    let owned = resolve_with("s3://expired-login-owned/x.parquet", &config, &env).unwrap_err();
    assert!(owned.contains("session has expired"), "{owned}");
}

#[test]
fn urls_within_a_root() {
    assert!(is_within("s3://b/parquet/x", "s3://b/parquet/"));
    assert!(is_within("s3://b/parquet", "s3://b/parquet/"));
    assert!(!is_within("s3://b/parquetx", "s3://b/parquet/"));
    assert!(is_within(
        "https://acct.blob.core.windows.net/release/2026/",
        "abfss://release@acct.dfs.core.windows.net/"
    ));
}

#[test]
fn an_unknown_source_names_the_ones_that_exist() {
    let config = CloudConfig {
        connections: vec![minio("lab", "http://127.0.0.1:9000")],
        ..Default::default()
    };
    with_machine(&Machine::new(&[], &[]), |env| {
        let err = resolve_with("s3://nope@data/key", &config, env).unwrap_err();
        assert!(err.contains("\"nope\"") && err.contains("lab"), "{err}");
    });
}

#[test]
fn an_aws_source_cannot_be_named_in_a_url() {
    let config = CloudConfig {
        connections: vec![CloudConnectionConfig {
            name: "second-account".to_string(),
            kind: Some("s3".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    with_machine(&Machine::new(&[], &[]), |env| {
        let err = resolve_with("s3://second-account@data/key", &config, env).unwrap_err();
        assert!(err.contains("not S3-compatible"), "{err}");
    });
}

#[test]
fn a_named_variable_that_is_unset_is_reported_not_borrowed() {
    let config = CloudConfig {
        connections: vec![minio("lab", "http://127.0.0.1:9000")],
        ..Default::default()
    };
    with_machine(&Machine::new(&[("LAB_KEY", "k")], &[]), |env| {
        let err = resolve_with("s3://lab@data/key", &config, env).unwrap_err();
        assert!(err.contains("LAB_SECRET is not set"), "{err}");
    });
}

#[test]
fn a_bucket_listed_by_an_aws_source_opens_with_that_login() {
    let config = CloudConfig {
        connections: vec![CloudConnectionConfig {
            name: "second-account".to_string(),
            kind: Some("s3".to_string()),
            access_key_id_env: Some("SECOND_KEY".to_string()),
            secret_access_key_env: Some("SECOND_SECRET".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let machine = Machine::new(&[("SECOND_KEY", "k2"), ("SECOND_SECRET", "s2")], &[]);
    with_machine(&machine, |env| {
        let source = configured_source(&config.connections[0], env);
        remember_bucket(&source, "only-in-second-account");
        let resolved = resolve_with("s3://only-in-second-account/x.parquet", &config, env).unwrap();
        assert_eq!(resolved.source_id, "second-account");
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("k2"));
        assert_eq!(source.bucket_url("b"), "s3://b");
    });
}

#[test]
fn a_bucket_an_earlier_run_listed_opens_with_that_login() {
    let config = CloudConfig {
        connections: vec![CloudConnectionConfig {
            name: "cached-account".to_string(),
            kind: Some("s3".to_string()),
            access_key_id_env: Some("CACHED_KEY".to_string()),
            secret_access_key_env: Some("CACHED_SECRET".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let machine = Machine::new(&[("CACHED_KEY", "k3"), ("CACHED_SECRET", "s3")], &[]);
    with_machine(&machine, |env| {
        let source = configured_source(&config.connections[0], env);
        remember_listed(&source, &["only-in-cached-account".to_string()]);
        let resolved = resolve_with("s3://only-in-cached-account/x.parquet", &config, env).unwrap();
        assert_eq!(resolved.source_id, "cached-account");
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("k3"));
    });
}

#[test]
fn discover_limits_found_logins_and_never_configured_sources() {
    use crate::config::CloudDiscover;
    let machine = Machine::new(
        &[
            ("AWS_ACCESS_KEY_ID", "env-key"),
            ("AWS_SECRET_ACCESS_KEY", "env-secret"),
            ("GOOGLE_APPLICATION_CREDENTIALS", "/home/u/sa.json"),
            ("LAB_KEY", "k"),
            ("LAB_SECRET", "s"),
        ],
        &[("/home/u/sa.json", "{}")],
    );
    with_machine(&machine, |env| {
        let shown = |which: Option<CloudDiscover>| {
            let config = CloudConfig {
                connections: vec![minio("lab", "http://127.0.0.1:9000")],
                discover: which,
                ..Default::default()
            };
            let mut ids: Vec<String> = on_home(discover(&config, env), &config)
                .into_iter()
                .map(|s| s.id)
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(
            shown(None),
            [DEFAULT_GCS, "lab", DEFAULT_S3],
            "unset shows every kind"
        );
        assert_eq!(shown(Some(CloudDiscover::All)), shown(None));
        assert_eq!(
            shown(Some(CloudDiscover::Kinds(vec!["gcs".to_string()]))),
            [DEFAULT_GCS, "lab"],
            "the configured S3 source stays; the found one goes"
        );
        assert_eq!(
            shown(Some(CloudDiscover::Kinds(vec!["s3".to_string()]))),
            ["lab", DEFAULT_S3]
        );
        assert_eq!(shown(Some(CloudDiscover::None)), ["lab"]);
    });
}

#[test]
fn configured_sources_join_detected_ones_and_replace_a_matching_id() {
    let machine = Machine::new(
        &[
            ("AWS_ACCESS_KEY_ID", "env-key"),
            ("LAB_KEY", "k"),
            ("LAB_SECRET", "s"),
        ],
        &[],
    );
    with_machine(&machine, |env| {
        let config = CloudConfig {
            connections: vec![minio("lab", "http://127.0.0.1:9000")],
            ..Default::default()
        };
        let found = discover(&config, env);
        let ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["lab", DEFAULT_S3]);
        assert_eq!(found[0].bucket_url("data"), "s3://lab@data");
        assert_eq!(found[1].label, "Amazon S3");

        let replacing = CloudConfig {
            connections: vec![CloudConnectionConfig {
                name: DEFAULT_S3.to_string(),
                label: Some("Work AWS".to_string()),
                kind: Some("s3".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let found = discover(&replacing, env);
        assert_eq!(found.len(), 1, "the replaced source");
        assert_eq!(found[0].label, "Work AWS");
        assert_eq!(found[0].tier, Tier::Config);
    });
}

#[test]
fn the_fingerprint_changes_with_the_endpoint() {
    with_machine(&Machine::new(&[], &[]), |env| {
        let a = configured_source(&minio("lab", "http://127.0.0.1:9000"), env);
        let b = configured_source(&minio("lab", "http://127.0.0.1:9001"), env);
        assert_ne!(a.fingerprint(), b.fingerprint());
    });
}

const AWS_CONFIG: &str = "
[default]
region = us-east-1

[profile work]
region = eu-west-1

[profile lab]
endpoint_url = http://localhost:9000

[profile regional-only]
region = ap-south-1
";

const AWS_CREDENTIALS: &str = "
[default]
aws_access_key_id = AKIADEFAULT
aws_secret_access_key = default-secret

[work]
aws_access_key_id = AKIAWORK
aws_secret_access_key = work-secret

[lab]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadmin
";

fn aws_machine(vars: &[(&str, &str)]) -> Machine {
    Machine::new(
        vars,
        &[
            ("/home/u/.aws/config", AWS_CONFIG),
            ("/home/u/.aws/credentials", AWS_CREDENTIALS),
        ],
    )
}

/// The bug #174 fixes: with `AWS_PROFILE=work`, the keys that sign are work's, not
/// the first ones in the credentials file.
#[test]
fn the_default_source_signs_with_the_active_profile() {
    with_machine(&aws_machine(&[("AWS_PROFILE", "work")]), |env| {
        let config = CloudConfig::from_env(env.var);
        let resolved = resolve_with("s3://bucket/key.parquet", &config, env).unwrap();
        assert_eq!(resolved.source_id, DEFAULT_S3);
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIAWORK"));
        assert_eq!(
            resolved.s3.secret_access_key.as_deref(),
            Some("work-secret")
        );
        assert_eq!(resolved.s3.region.as_deref(), Some("eu-west-1"));
        assert!(!resolved.s3.from_env);
    });
    with_machine(&aws_machine(&[]), |env| {
        let config = CloudConfig::from_env(env.var);
        let resolved = resolve_with("s3://bucket/key.parquet", &config, env).unwrap();
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIADEFAULT"));
    });
}

#[test]
fn keys_in_the_environment_still_beat_a_profile() {
    let machine = aws_machine(&[
        ("AWS_PROFILE", "work"),
        ("AWS_ACCESS_KEY_ID", "AKIAENV"),
        ("AWS_SECRET_ACCESS_KEY", "env-secret"),
    ]);
    with_machine(&machine, |env| {
        let config = CloudConfig::from_env(env.var);
        let resolved = resolve_with("s3://bucket/key", &config, env).unwrap();
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("AKIAENV"));
        let ids: Vec<String> = discover(&config, env).into_iter().map(|s| s.id).collect();
        assert!(ids.contains(&"aws-work".to_string()), "{ids:?}");
    });
}

#[test]
fn every_other_profile_that_can_log_in_is_a_source() {
    with_machine(&aws_machine(&[("AWS_PROFILE", "work")]), |env| {
        let config = CloudConfig::from_env(env.var);
        let found = discover(&config, env);
        let ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
        // The active profile is the default source; a profile with only a region
        // cannot log in and is not listed.
        assert_eq!(ids, [DEFAULT_S3, "aws-default", "aws-lab"]);
        let lab = found.iter().find(|s| s.id == "aws-lab").unwrap();
        assert!(
            lab.named_in_urls(),
            "a profile with an endpoint is S3-compatible"
        );
        assert_eq!(lab.origin, "aws profile");

        let resolved = resolve_with("s3://aws-lab@data/x.parquet", &config, env).unwrap();
        assert_eq!(
            resolved.s3.endpoint.as_deref(),
            Some("http://localhost:9000")
        );
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("minioadmin"));
    });
}

#[test]
fn a_configured_source_can_log_in_through_a_profile() {
    let config = CloudConfig {
        connections: vec![CloudConnectionConfig {
            name: "minio".to_string(),
            kind: Some("s3".to_string()),
            profile: Some("lab".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    with_machine(&aws_machine(&[]), |env| {
        let resolved = resolve_with("s3://minio@data/x", &config, env).unwrap();
        assert_eq!(
            resolved.s3.endpoint.as_deref(),
            Some("http://localhost:9000")
        );
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("minioadmin"));
    });
    let missing = CloudConfig {
        connections: vec![CloudConnectionConfig {
            name: "minio".to_string(),
            kind: Some("s3".to_string()),
            endpoint_url: Some("http://x".to_string()),
            profile: Some("nope".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    with_machine(&aws_machine(&[]), |env| {
        let err = resolve_with("s3://minio@data/x", &missing, env).unwrap_err();
        assert!(
            err.contains("profile nope is not in the AWS config"),
            "{err}"
        );
    });
}

#[test]
fn mc_aliases_mc_host_and_s3cmd_are_sources() {
    let mc = r#"{"version": "10", "aliases": {
            "lab": {"url": "http://127.0.0.1:9000", "accessKey": "minioadmin", "secretKey": "minioadmin", "api": "S3v4", "path": "auto"},
            "Corp MinIO": {"url": "https://minio.corp.example", "accessKey": "corp", "secretKey": "s", "api": "S3v4", "path": "on"}
        }}"#;
    let s3cfg = "[default]\naccess_key = CEPH\nsecret_key = s\nhost_base = ceph.example:7480\nhost_bucket = ceph.example:7480\n";
    let machine = Machine::new(
        &[("MC_HOST_lab", "http://envkey:envsecret@127.0.0.1:9100")],
        &[("/home/u/.mc/config.json", mc), ("/home/u/.s3cfg", s3cfg)],
    );
    with_machine(&machine, |env| {
        let config = CloudConfig::default();
        let found = discover(&config, env);
        let mut ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, ["mc-corp-minio", "mc-lab", "s3cfg"]);
        let lab = found.iter().find(|s| s.id == "mc-lab").unwrap();
        assert_eq!(
            lab.origin, "MC_HOST_lab",
            "the environment replaces the alias"
        );
        assert_eq!(lab.s3.endpoint.as_deref(), Some("http://127.0.0.1:9100"));
        let ceph = found.iter().find(|s| s.id == "s3cfg").unwrap();
        assert_eq!(ceph.label, "ceph.example:7480");
        assert!(ceph.named_in_urls());

        let resolved = resolve_with("s3://mc-corp-minio@data/x.parquet", &config, env).unwrap();
        assert_eq!(
            resolved.s3.endpoint.as_deref(),
            Some("https://minio.corp.example")
        );
        assert_eq!(resolved.s3.access_key_id.as_deref(), Some("corp"));
        assert_eq!(resolved.s3.virtual_hosted, Some(false));
    });
}

#[test]
fn one_server_found_twice_is_one_source() {
    let mc = r#"{"version": "10", "aliases": {
            "lab": {"url": "http://127.0.0.1:9000/", "accessKey": "minioadmin", "secretKey": "minioadmin"}
        }}"#;
    let machine = Machine::new(
        &[("LAB_KEY", "minioadmin"), ("LAB_SECRET", "minioadmin")],
        &[("/home/u/.mc/config.json", mc)],
    );
    with_machine(&machine, |env| {
        let config = CloudConfig {
            connections: vec![CloudConnectionConfig {
                name: "lab".to_string(),
                kind: Some("s3".to_string()),
                endpoint_url: Some("http://127.0.0.1:9000".to_string()),
                access_key_id_env: Some("LAB_KEY".to_string()),
                secret_access_key_env: Some("LAB_SECRET".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let found = discover(&config, env);
        let ids: Vec<&str> = found.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["lab"], "the config's source stays");
        assert_eq!(found[0].origin, "datui config, mc alias");
    });
}

#[test]
fn profile_ids_are_valid_source_ids() {
    assert_eq!(profile_source_id("Prod_Admin.RO"), "aws-prod-admin-ro");
    assert!(crate::config::is_valid_source_id(&profile_source_id(
        "a very long profile name that goes on and on"
    )));
}

/// A resolve asks for the sources each time; they are discovered once for a config,
/// again for a different one, and again when asked to look again.
#[test]
fn sources_are_discovered_once_per_config() {
    let session = SessionSources::default();
    let looked = std::cell::Cell::new(0);
    let look = || {
        looked.set(looked.get() + 1);
        Vec::new()
    };
    let config = CloudConfig::default();
    for _ in 0..8 {
        session.get(&config, look);
    }
    assert_eq!(looked.get(), 1, "eight resolves, one discovery");
    let other = CloudConfig {
        hide: vec!["lab".to_string()],
        ..CloudConfig::default()
    };
    session.get(&other, look);
    session.get(&other, look);
    assert_eq!(looked.get(), 2, "a changed config is discovered for once");
    session.refresh(&other, look);
    session.get(&other, look);
    assert_eq!(looked.get(), 3, "Ctrl+R looks again, once");
}
