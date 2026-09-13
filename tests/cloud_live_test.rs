//! Cloud discovery and listing, against real object stores.
//!
//! Every test here is `#[ignore]`d and gated on an environment variable. They talk to
//! services over the network, using whatever credentials the machine has, and neither
//! of those belongs in a run of `cargo test`. CI would either skip them, which is a
//! test that lies, or need credentials, which is a secret in a workflow for the sake of
//! a developer convenience.
//!
//! They exist because the alternative is worse. The code they cover signs requests and
//! parses responses from services that cannot be usefully mocked: a fake that agrees
//! with my reading of the S3 signing rules proves only that I am self-consistent. These
//! were run against a live MinIO and a live Google Cloud Storage project before the
//! feature was merged, and they are committed so the next person can do the same
//! without reconstructing the setup.
//!
//! # Google Cloud Storage
//!
//! ```bash
//! gcloud auth application-default login
//! DATUI_LIVE_GCS=1 cargo test --test cloud_live_test -- --ignored --nocapture
//! ```
//!
//! # MinIO, or anything else speaking S3
//!
//! ```bash
//! docker run -d --name datui-minio -p 127.0.0.1:9000:9000 \
//!   -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//!   quay.io/minio/minio server /data
//! docker run --rm --network host --entrypoint sh quay.io/minio/mc -c "
//!   mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
//!   mc mb -p local/datui-sales local/datui-logs
//!   printf 'id,region,amount\n1,north,10\n' > /tmp/sales.csv
//!   mc cp /tmp/sales.csv local/datui-sales/top-level.csv
//!   mc cp /tmp/sales.csv local/datui-sales/2024/january.csv"
//! DATUI_LIVE_S3=http://127.0.0.1:9000 cargo test --test cloud_live_test -- --ignored --nocapture
//! ```

#![cfg(feature = "cloud")]

mod common;

use datui::cloud_browse::{self, Environment, ProviderKind};
use datui::config::CloudConfig;

/// The MinIO credentials the documented container runs with. Not a secret in any sense:
/// they are the defaults printed in MinIO's own quick-start, and this only ever points
/// at a container on the loopback interface.
const MINIO_KEY: &str = "minioadmin";
const MINIO_SECRET: &str = "minioadmin";

fn minio_config(endpoint: &str) -> CloudConfig {
    CloudConfig {
        s3_endpoint_url: Some(endpoint.to_string()),
        s3_access_key_id: Some(MINIO_KEY.to_string()),
        s3_secret_access_key: Some(MINIO_SECRET.to_string()),
        s3_region: Some("us-east-1".to_string()),
    }
}

#[test]
#[ignore = "talks to Google Cloud Storage; set DATUI_LIVE_GCS=1"]
fn gcs_is_discovered_and_its_buckets_listed() {
    if std::env::var("DATUI_LIVE_GCS").is_err() {
        eprintln!("skipped: set DATUI_LIVE_GCS=1 to run");
        return;
    }

    let config = CloudConfig::default();
    let env = Environment::current();
    let providers = cloud_browse::detect(&config, &env);
    let gcs = providers
        .iter()
        .find(|p| p.kind == ProviderKind::Gcs)
        .expect("GCS should be discovered after `gcloud auth application-default login`");

    println!("provider: {} ({})", gcs.label, gcs.note);
    println!("project: {:?}", gcs.project);
    assert!(
        gcs.can_list_buckets(),
        "a logged-in gcloud writes quota_project_id, so a project should have been found"
    );

    let runtime = common::test_runtime();
    let buckets = runtime
        .block_on(cloud_browse::list_buckets(gcs, &config))
        .expect("listing buckets");
    println!("buckets: {buckets:?}");
    assert!(
        !buckets.is_empty(),
        "the project has no buckets, so this proves nothing; create one and re-run"
    );

    // Listing the first bucket exercises the other half: a delimited listing turning
    // objects and prefixes into home-screen rows.
    let url = format!("gs://{}", buckets[0]);
    let rows = runtime
        .block_on(cloud_browse::list_objects(&url, &config))
        .expect("listing objects");
    println!("{} holds {} rows at the top level", url, rows.len());
    for row in rows.iter().take(10) {
        println!("  {:?} {} {:?}", row.kind, row.name, row.size);
    }
    for row in &rows {
        assert!(
            row.path.to_string_lossy().starts_with(&url),
            "every row should be addressable by the URL it came from: {:?}",
            row.path
        );
    }
}

#[test]
#[ignore = "talks to a local MinIO; set DATUI_LIVE_S3=http://127.0.0.1:9000"]
fn minio_is_discovered_and_its_buckets_listed() {
    let Ok(endpoint) = std::env::var("DATUI_LIVE_S3") else {
        eprintln!("skipped: set DATUI_LIVE_S3 to an endpoint to run");
        return;
    };

    let config = minio_config(&endpoint);
    let env = Environment::current();
    let providers = cloud_browse::detect(&config, &env);
    let s3 = providers
        .iter()
        .find(|p| p.kind == ProviderKind::S3)
        .expect("configured keys should be enough to discover S3");

    println!("provider: {} ({})", s3.label, s3.note);
    assert!(
        s3.label.contains("S3-compatible"),
        "a custom endpoint should not be labelled Amazon: {}",
        s3.label
    );

    let runtime = common::test_runtime();
    let buckets = runtime
        .block_on(cloud_browse::list_buckets(s3, &config))
        .expect("listing buckets");
    println!("buckets: {buckets:?}");
    assert!(
        buckets.contains(&"datui-sales".to_string()),
        "expected the documented test bucket; got {buckets:?}"
    );

    let rows = runtime
        .block_on(cloud_browse::list_objects("s3://datui-sales", &config))
        .expect("listing objects");
    for row in &rows {
        println!("  {:?} {} {:?}", row.kind, row.name, row.size);
    }

    // A prefix and an object at the same level, which is the case a listing has to get
    // right: one is somewhere to descend into and the other is something to open.
    let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
    assert!(names.contains(&"top-level.csv"), "got {names:?}");
    assert!(names.contains(&"2024"), "got {names:?}");

    let prefix = rows
        .iter()
        .find(|r| r.name == "2024")
        .expect("the prefix row");
    assert_eq!(prefix.kind, datui::discover::EntryKind::Directory);
    let object = rows
        .iter()
        .find(|r| r.name == "top-level.csv")
        .expect("the object row");
    assert_eq!(object.kind, datui::discover::EntryKind::File);
    assert!(object.size.unwrap_or(0) > 0, "an object should have a size");

    // Descending, which is what pressing Enter on the prefix will do.
    let nested = runtime
        .block_on(cloud_browse::list_objects(
            &prefix.path.to_string_lossy(),
            &config,
        ))
        .expect("listing the prefix");
    let nested_names: Vec<&str> = nested.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(nested_names, vec!["january.csv"], "got {nested_names:?}");
}
