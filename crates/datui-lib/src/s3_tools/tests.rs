use super::*;

const MC_CONFIG: &str = r#"{
        "version": "10",
        "aliases": {
            "gcs": {"url": "https://storage.googleapis.com", "accessKey": "YOUR-ACCESS-KEY-HERE", "secretKey": "YOUR-SECRET-KEY-HERE", "api": "S3v2", "path": "dns"},
            "local": {"url": "http://localhost:9000", "accessKey": "", "secretKey": "", "api": "S3v4", "path": "auto"},
            "play": {"url": "https://play.min.io", "accessKey": "Q3AM3UQ867SPQQA43P2F", "secretKey": "zuf+tfteSlswRu7BJ86wtrueekitnifILbZam1KYY3TG", "api": "S3v4", "path": "auto"},
            "lab": {"url": "http://127.0.0.1:9000/", "accessKey": "minioadmin", "secretKey": "minioadmin", "api": "S3v4", "path": "on"},
            "corp": {"url": "https://minio.corp.example", "accessKey": "k", "secretKey": "s", "sessionToken": "t", "api": "s3v4", "path": "off"}
        }
    }"#;

#[test]
fn mc_aliases_with_keys_become_servers() {
    let servers = parse_mc_config(MC_CONFIG);
    let names: Vec<&str> = servers.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names,
        ["corp", "lab"],
        "no placeholders, no empty keys, no play"
    );
    let lab = &servers[1];
    assert_eq!(lab.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    assert_eq!(lab.virtual_hosted, Some(false));
    assert_eq!(servers[0].virtual_hosted, Some(true));
    assert_eq!(servers[0].session_token.as_deref(), Some("t"));
    assert!(parse_mc_config("not json").is_empty());
}

#[test]
fn mc_host_variables_read_like_mc_reads_them() {
    let plain = parse_mc_host("lab", "http://minioadmin:minio/admin@127.0.0.1:9000").unwrap();
    assert_eq!(plain.access_key_id, "minioadmin");
    assert_eq!(plain.secret_access_key, "minio/admin");
    assert_eq!(plain.session_token, None);
    assert_eq!(plain.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    assert_eq!(plain.origin, "MC_HOST_lab");

    let token = parse_mc_host("corp", "https://AK:SK:TOKEN@minio.corp.example").unwrap();
    assert_eq!(token.secret_access_key, "SK");
    assert_eq!(token.session_token.as_deref(), Some("TOKEN"));

    assert!(parse_mc_host("x", "https://minio.corp.example").is_none());
    assert!(parse_mc_host("x", "ftp://a:b@host").is_none());

    let found = mc_hosts(&[
        ("MC_HOST_b".to_string(), "http://k:s@b:9000".to_string()),
        ("PATH".to_string(), "/usr/bin".to_string()),
        ("MC_HOST_a".to_string(), "http://k:s@a:9000".to_string()),
    ]);
    let names: Vec<&str> = found.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["a", "b"]);
}

#[test]
fn s3cmd_config_for_ceph_and_for_aws() {
    let ceph = parse_s3cfg(
        "[default]\naccess_key = CEPHKEY\nsecret_key = cephsecret\nhost_base = ceph.example:7480\n\
         host_bucket = ceph.example:7480\nuse_https = False\nbucket_location = US\n",
    )
    .unwrap();
    assert_eq!(ceph.endpoint.as_deref(), Some("http://ceph.example:7480"));
    assert_eq!(ceph.virtual_hosted, Some(false));
    assert_eq!(ceph.region.as_deref(), Some("us-east-1"));

    let spaces = parse_s3cfg(
        "[default]\naccess_key = DO\nsecret_key = s\nhost_base = nyc3.digitaloceanspaces.com\n\
         host_bucket = %(bucket)s.nyc3.digitaloceanspaces.com\n",
    )
    .unwrap();
    assert_eq!(
        spaces.endpoint.as_deref(),
        Some("https://nyc3.digitaloceanspaces.com")
    );
    assert_eq!(spaces.virtual_hosted, Some(true));

    let aws = parse_s3cfg("[default]\naccess_key = AKIA\nsecret_key = s\n").unwrap();
    assert_eq!(aws.endpoint, None, "no host_base is AWS");

    assert!(
        parse_s3cfg("[default]\nhost_base = x\n").is_none(),
        "no keys"
    );
    assert!(parse_s3cfg("[other]\naccess_key = a\nsecret_key = b\n").is_none());
}

#[test]
fn where_each_tool_keeps_its_config() {
    let run =
        |_: &str, _: &[&str]| Err(crate::cloud_command::CommandError::Missing("x".to_string()));
    let env_with = |vars: &'static [(&'static str, &'static str)], windows: bool| {
        move |f: &dyn Fn(&Environment<'_>)| {
            let var = |key: &str| {
                vars.iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.to_string())
            };
            let env = Environment {
                var: &var,
                exists: &|_| false,
                read: &|_| None,
                home: Some(PathBuf::from(if windows {
                    r"C:\Users\u"
                } else {
                    "/home/u"
                })),
                windows,
                run: &run,
                all_vars: &|| Vec::new(),
                list: &|_| Vec::new(),
            };
            f(&env);
        }
    };
    env_with(&[], false)(&|env| {
        assert_eq!(
            mc_config_paths(env),
            [
                PathBuf::from("/home/u/.mc/config.json"),
                PathBuf::from("/home/u/.mcli/config.json")
            ]
        );
        assert_eq!(s3cfg_path(env), Some(PathBuf::from("/home/u/.s3cfg")));
    });
    env_with(&[("APPDATA", r"C:\Users\u\AppData\Roaming")], true)(&|env| {
        assert_eq!(
            mc_config_paths(env)[0],
            PathBuf::from(r"C:\Users\u").join("mc").join("config.json")
        );
        assert_eq!(
            s3cfg_path(env),
            Some(PathBuf::from(r"C:\Users\u\AppData\Roaming").join("s3cmd.ini"))
        );
    });
    env_with(
        &[("MC_CONFIG_DIR", "/etc/mc"), ("S3CMD_CONFIG", "/etc/s3cfg")],
        false,
    )(&|env| {
        assert_eq!(mc_config_paths(env), [PathBuf::from("/etc/mc/config.json")]);
        assert_eq!(s3cfg_path(env), Some(PathBuf::from("/etc/s3cfg")));
    });
}
