use super::*;
use std::path::Path;

fn env_with<'a>(
    vars: &'a HashMap<&'a str, String>,
    files: &'a HashMap<PathBuf, String>,
    run: &'a crate::cloud_command::Runner<'a>,
    body: impl FnOnce(&Environment<'_>),
) {
    let var = |k: &str| vars.get(k).cloned();
    let exists = |p: &Path| files.contains_key(p);
    let read = |p: &Path| files.get(p).cloned();
    let all_vars = Vec::new;
    let list = |_: &Path| Vec::new();
    body(&Environment {
        var: &var,
        exists: &exists,
        read: &read,
        home: Some(PathBuf::from("/home/u")),
        windows: false,
        run,
        all_vars: &all_vars,
        list: &list,
    });
}

#[test]
fn service_principals_from_the_environment() {
    let vars: HashMap<&str, String> = [
        ("AZURE_TENANT_ID", "t"),
        ("AZURE_CLIENT_ID", "c"),
        ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
    ]
    .into_iter()
    .map(|(k, v)| (k, v.to_string()))
    .collect();
    let (settings, origin) = from_environment(&|k| vars.get(k).cloned()).unwrap();
    assert_eq!(origin, "AZURE_FEDERATED_TOKEN_FILE");
    assert_eq!(settings.account, None, "finds its accounts");
    assert!(settings.auth.is_identity());
    assert_eq!(settings.auth.describe(), "workload identity");
    // A key beside the account still wins.
    let mut keyed = vars.clone();
    keyed.insert("AZURE_STORAGE_ACCOUNT_NAME", "acct".to_string());
    keyed.insert("AZURE_STORAGE_ACCOUNT_KEY", "a2V5".to_string());
    let (settings, _) = from_environment(&|k| keyed.get(k).cloned()).unwrap();
    assert_eq!(settings.auth, AzureAuth::Key("a2V5".to_string()));
    // No secret, no principal.
    let mut half = vars.clone();
    half.remove("AZURE_FEDERATED_TOKEN_FILE");
    assert!(from_environment(&|k| half.get(k).cloned()).is_none());
    let debug = format!(
        "{:?}",
        ServicePrincipal {
            secret: Some("hunter2".to_string()),
            ..Default::default()
        }
    );
    assert!(!debug.contains("hunter2"), "{debug}");
}

#[test]
fn a_service_principal_token_from_entra_id() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let authority = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = stream.read(&mut buf).unwrap_or(0);
                request.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&request);
                if n == 0
                    || text.find("\r\n\r\n").is_some_and(|end| {
                        let length = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        request.len() >= end + 4 + length
                    })
                {
                    break;
                }
            }
            tx.send(String::from_utf8_lossy(&request).into_owned())
                .unwrap();
            let body = r#"{"token_type":"Bearer","expires_in":3599,"access_token":"eyJ.sp"}"#;
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    let sp = ServicePrincipal {
        tenant: "tenant-1".to_string(),
        client_id: "client-1".to_string(),
        secret: None,
        token_file: Some(PathBuf::from("/var/run/federated")),
        authority,
    };
    let vars = HashMap::new();
    let files: HashMap<PathBuf, String> = [(
        PathBuf::from("/var/run/federated"),
        "jwt-from-aks\n".to_string(),
    )]
    .into();
    let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
    env_with(&vars, &files, &run, |env| {
        let token = identity_token(
            &AzureAuth::ServicePrincipal(sp.clone()),
            "https://storage.azure.com/.default",
            env,
        )
        .unwrap();
        assert_eq!(token, "eyJ.sp");
    });
    let request = rx.recv().unwrap();
    assert!(
        request.starts_with("POST /tenant-1/oauth2/v2.0/token"),
        "{request}"
    );
    assert!(
        request.contains("client_assertion=jwt-from-aks"),
        "{request}"
    );
    assert!(
        request.contains("grant_type=client_credentials"),
        "{request}"
    );
}

#[test]
fn a_managed_identity_token_from_the_platform() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/msi/token", listener.local_addr().unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            tx.send(String::from_utf8_lossy(&buf[..n]).into_owned())
                .unwrap();
            let body = r#"{"access_token":"eyJ.mi","expires_on":"4102444800","resource":"https://storage.azure.com/","token_type":"Bearer"}"#;
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    let vars: HashMap<&str, String> = [
        ("IDENTITY_ENDPOINT", endpoint),
        ("IDENTITY_HEADER", "header-secret".to_string()),
    ]
    .into();
    let files = HashMap::new();
    let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
    env_with(&vars, &files, &run, |env| {
        let token = identity_token(
            &AzureAuth::ManagedIdentity,
            "https://management.azure.com/.default",
            env,
        )
        .unwrap();
        assert_eq!(token, "eyJ.mi");
    });
    let request = rx.recv().unwrap().to_ascii_lowercase();
    assert!(request.contains("api-version=2019-08-01"), "{request}");
    assert!(
        request.contains("resource=https%3a%2f%2fmanagement.azure.com%2f"),
        "{request}"
    );
    assert!(
        request.contains("x-identity-header: header-secret"),
        "{request}"
    );
}

#[test]
fn powershell_tokens() {
    let tokens = parse_powershell_tokens(
        r#"{"https://storage.azure.com/":{"token":"st","expires":1893553445},"https://management.azure.com/":{"token":"mg","expires":1893553445}}"#,
    )
    .unwrap();
    assert_eq!(tokens.len(), 2);
    assert!(
        tokens
            .iter()
            .any(|(r, t, _)| r == "https://storage.azure.com/" && t == "st")
    );
    assert!(parse_powershell_tokens("WARNING: something").is_none());
    // The script is a constant with nothing from outside in it.
    assert!(!POWERSHELL_SCRIPT.contains('"'));
}

#[test]
fn powershell_is_asked_when_it_is_the_one_signed_in() {
    let vars = HashMap::new();
    let files: HashMap<PathBuf, String> = [(
        PathBuf::from("/home/u/.Azure/AzureRmContext.json"),
        "{}".to_string(),
    )]
    .into();
    let run = |program: &str, args: &[&str]| match program {
        "az" => Err(CommandError::Failed(
            "Please run 'az login' to setup account.".to_string(),
        )),
        "pwsh" => {
            assert_eq!(&args[..3], ["-NoProfile", "-NonInteractive", "-Command"]);
            Ok(r#"{"https://storage.azure.com/":{"token":"from-pwsh","expires":4102444800},"https://management.azure.com/":{"token":"mg","expires":4102444800}}"#.to_string())
        }
        other => Err(CommandError::Missing(other.to_string())),
    };
    env_with(&vars, &files, &run, |env| {
        assert!(powershell_login_evidence(env));
        let token = identity_token(&AzureAuth::AzCli, STORAGE_SCOPE, env).unwrap();
        assert_eq!(token, "from-pwsh");
    });
}

#[test]
fn not_signed_in_needs_azure_tooling() {
    // Joined with the host's separator, which is what `split_paths` splits on:
    // `:` is one long directory on Windows.
    let path = std::env::join_paths(["/opt/az/bin", "/usr/bin"]).unwrap();
    let vars: HashMap<&str, String> = [
        ("PATH", path.to_string_lossy().into_owned()),
        (
            "PSModulePath",
            "/home/u/.local/share/powershell/Modules".to_string(),
        ),
    ]
    .into();
    let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
    let az_only: HashMap<PathBuf, String> =
        [(PathBuf::from("/opt/az/bin/az"), String::new())].into();
    env_with(&vars, &az_only, &run, |env| {
        assert_eq!(
            not_signed_in(env).as_deref(),
            Some("not signed in: run az login")
        );
    });
    let both: HashMap<PathBuf, String> = [
        (PathBuf::from("/opt/az/bin/az"), String::new()),
        (
            PathBuf::from("/home/u/.local/share/powershell/Modules/Az.Accounts"),
            String::new(),
        ),
    ]
    .into();
    env_with(&vars, &both, &run, |env| {
        assert!(not_signed_in(env).unwrap().contains("Connect-AzAccount"));
    });
    let nothing = HashMap::new();
    env_with(&vars, &nothing, &run, |env| {
        assert_eq!(not_signed_in(env), None)
    });
    let signed_in: HashMap<PathBuf, String> = [
        (PathBuf::from("/opt/az/bin/az"), String::new()),
        (PathBuf::from("/home/u/.azure"), String::new()),
    ]
    .into();
    env_with(&vars, &signed_in, &run, |env| {
        assert_eq!(not_signed_in(env), None)
    });
}

#[test]
fn account_keys_only_after_a_missing_data_role() {
    let vars = HashMap::new();
    let files = HashMap::new();
    let run = |p: &str, _: &[&str]| Err(CommandError::Missing(p.to_string()));
    let signed_in = AzureSettings {
        auth: AzureAuth::Bearer("t".to_string()),
        identity: Some(AzureAuth::AzCli),
        ..Default::default()
    };
    let mismatch = "403 AuthorizationPermissionMismatch: This request is not authorized";
    env_with(&vars, &files, &run, |env| {
        // Turned off: the refusal as it was.
        let off = with_account_key("keysoff", &signed_in, mismatch, false, env).unwrap_err();
        assert_eq!(off, mismatch);
        // Another refusal: nothing to get past.
        let other = with_account_key(
            "keysother",
            &signed_in,
            "403 AuthorizationFailure",
            true,
            env,
        )
        .unwrap_err();
        assert_eq!(other, "403 AuthorizationFailure");
        // A key or SAS was refused: no identity to fetch keys with.
        let keyed = AzureSettings {
            auth: AzureAuth::Key("k".to_string()),
            ..Default::default()
        };
        assert_eq!(
            with_account_key("keyskeyed", &keyed, mismatch, true, env).unwrap_err(),
            mismatch
        );
        // The identity cannot fetch keys: the refusal, and why no key was used.
        let failed = with_account_key("keysnoaz", &signed_in, mismatch, true, env).unwrap_err();
        assert!(
            failed.starts_with(mismatch) && failed.contains("needs the Azure CLI"),
            "{failed}"
        );
        // Fetched before: used, with the identity kept.
        remember_key_for_test("keyshad", "a2V5");
        let used = with_account_key("keyshad", &signed_in, mismatch, true, env).unwrap();
        assert_eq!(used.auth, AzureAuth::Key("a2V5".to_string()));
        assert_eq!(used.identity, Some(AzureAuth::AzCli));
    });
    assert_eq!(
        parse_account_id(
            r#"{"data":[{"id":"/subscriptions/s/resourceGroups/g/providers/Microsoft.Storage/storageAccounts/a","sharedKey":false}]}"#
        ),
        Some((
            "/subscriptions/s/resourceGroups/g/providers/Microsoft.Storage/storageAccounts/a"
                .to_string(),
            false
        ))
    );
    assert_eq!(parse_account_id(r#"{"data":[]}"#), None);
    assert_eq!(
        parse_keys(r#"{"keys":[{"keyName":"key1","value":"k1","permissions":"FULL"},{"keyName":"key2","value":"k2"}]}"#).as_deref(),
        Some("k1")
    );
}

#[test]
fn connection_strings_in_their_common_shapes() {
    let key = parse_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=datalake001;AccountKey=a2V5;EndpointSuffix=core.windows.net",
    )
    .unwrap();
    assert_eq!(key.account.as_deref(), Some("datalake001"));
    assert_eq!(key.auth, AzureAuth::Key("a2V5".to_string()));
    assert_eq!(
        key.blob_endpoint.as_deref(),
        Some("https://datalake001.blob.core.windows.net")
    );

    let sas = parse_connection_string(
        "BlobEndpoint=https://datalake001.blob.core.windows.net/;SharedAccessSignature=sv=2022-11-02&sig=abc",
    )
    .unwrap();
    assert_eq!(sas.account.as_deref(), Some("datalake001"));
    assert_eq!(
        sas.auth,
        AzureAuth::Sas("sv=2022-11-02&sig=abc".to_string())
    );

    let azurite = parse_connection_string("UseDevelopmentStorage=true").unwrap();
    assert!(azurite.use_emulator);
    assert_eq!(
        azurite.blob_endpoint_for("devstoreaccount1"),
        "http://127.0.0.1:10000/devstoreaccount1/"
    );

    assert!(
        parse_connection_string("AccountName=x").is_none(),
        "no key or SAS"
    );
}

#[test]
fn the_environment_names_an_account_with_a_key_or_sas() {
    let vars = |pairs: &'static [(&'static str, &'static str)]| {
        move |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    };
    let (settings, origin) = from_environment(&vars(&[
        ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
        ("AZURE_STORAGE_SAS_TOKEN", "?sv=1&sig=2"),
    ]))
    .unwrap();
    assert_eq!(settings.auth, AzureAuth::Sas("sv=1&sig=2".to_string()));
    assert_eq!(origin, "AZURE_STORAGE_SAS_TOKEN");
    assert!(from_environment(&vars(&[("AZURE_STORAGE_ACCOUNT_NAME", "acct")])).is_none());
}

#[test]
fn a_token_with_and_without_an_expiry() {
    let (token, expires) = parse_token(
        r#"{"accessToken": "eyJ0", "expiresOn": "2026-09-16 16:32:37.000000", "expires_on": 1789600000, "tokenType": "Bearer"}"#,
    )
    .unwrap();
    assert_eq!(token, "eyJ0");
    assert_eq!(
        expires,
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_789_600_000))
    );
    let (_, expires) =
        parse_token(r#"{"accessToken": "eyJ0", "expiresOn": "2026-09-16 16:32:37.000000"}"#)
            .unwrap();
    assert_eq!(expires, None);
    assert!(parse_token(r#"{"accessToken": ""}"#).is_none());
}

#[test]
fn accounts_from_resource_graph() {
    let (accounts, next) = parse_accounts(
        r#"{"totalRecords": 2, "$skipToken": "page2", "data": [
                {"name": "datalake001", "subscription": "Azure subscription 1", "location": "eastus",
                 "hns": true, "blob": "https://datalake001.blob.core.windows.net/",
                 "publicNetworkAccess": "Enabled", "defaultAction": "Allow", "sharedKey": null},
                {"name": "locked001", "hns": false, "publicNetworkAccess": "Disabled", "sharedKey": false}
            ]}"#,
    )
    .unwrap();
    assert_eq!(next.as_deref(), Some("page2"));
    assert_eq!(accounts[0].name, "datalake001");
    assert!(accounts[0].hierarchical_namespace && accounts[0].shared_key_access);
    assert!(!accounts[0].private_network);
    assert!(accounts[1].private_network && !accounts[1].shared_key_access);

    let err = parse_accounts(r#"{"error": {"code": "AuthorizationFailed", "message": "no"}}"#)
        .unwrap_err();
    assert_eq!(err, "no");
}

#[test]
fn containers_and_the_next_page() {
    let (names, next) = parse_containers(
        r#"<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="https://a.blob.core.windows.net/"><Containers><Container><Name>datui-test</Name><Properties/></Container><Container><Name>raw</Name></Container></Containers><NextMarker>/a/raw</NextMarker></EnumerationResults>"#,
    )
    .unwrap();
    assert_eq!(names, ["datui-test", "raw"]);
    assert_eq!(next.as_deref(), Some("/a/raw"));
    let (none, next) =
        parse_containers("<EnumerationResults><Containers /><NextMarker /></EnumerationResults>")
            .unwrap();
    assert!(none.is_empty() && next.is_none());
}

#[test]
fn a_permission_error_says_how_to_get_permission() {
    let text = describe_error(
        403,
        "<?xml version=\"1.0\"?><Error><Code>AuthorizationPermissionMismatch</Code><Message>This request is not authorized to perform this operation using this permission.\nRequestId:x</Message></Error>",
    );
    assert!(text.starts_with("403 AuthorizationPermissionMismatch: This request"));
    assert!(text.contains("Storage Blob Data Reader"));
    assert!(!text.contains("RequestId"));
}

#[test]
fn folder_markers_are_not_data() {
    let prefixes = vec!["demo/".to_string()];
    assert!(is_folder_marker("demo", 0, &prefixes));
    assert!(!is_folder_marker("demo", 10, &prefixes));
    assert!(!is_folder_marker("empty.csv", 0, &prefixes));
}
