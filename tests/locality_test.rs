//! Where a dataset lives, and what that says about opening it.

use datui::locality::{Locality, Mounts, Source};
use std::path::Path;

/// A mountinfo excerpt shaped like a real one, including the autofs/nfs4 pair that
/// an automounted share produces.
const MOUNTINFO: &str = "\
23 1 0:22 / /proc rw,relatime - proc proc rw
25 1 0:6 / /dev rw,nosuid - devtmpfs devtmpfs rw
30 1 0:24 / /tmp rw,nosuid,nodev - tmpfs tmpfs rw
40 1 259:2 /home /home rw,relatime - btrfs /dev/nvme0n1p2 rw
55 1 0:45 / /mnt/gilead/data rw,relatime - autofs systemd-1 rw
70 55 0:52 / /mnt/gilead/data rw,relatime - nfs4 nas:/data rw
80 1 0:60 / /media/backup rw - fuse.sshfs user@host:/ rw
";

fn mounts() -> Mounts {
    Mounts::parse(MOUNTINFO)
}

#[test]
fn test_the_filesystem_is_named_not_merely_classified() {
    // "network" covers nfs4, cifs and sshfs, which fail in three different ways.
    // The kernel's own name for it costs one word and says considerably more.
    let m = mounts();
    assert_eq!(
        m.fstype_for(Path::new("/home/derek/data.parquet")),
        Some("btrfs")
    );
    assert_eq!(
        m.fstype_for(Path::new("/tmp/staged.parquet")),
        Some("tmpfs")
    );
    assert_eq!(
        m.fstype_for(Path::new("/media/backup/old.csv")),
        Some("fuse.sshfs")
    );
}

#[test]
fn test_an_automounted_share_reports_the_share_not_the_automounter() {
    // mountinfo lists them in mount order at the same point, so the later entry
    // shadows the earlier one -- and it is the nfs4 entry that describes what a read
    // will actually do.
    let m = mounts();
    assert_eq!(
        m.fstype_for(Path::new("/mnt/gilead/data/sets/prices.parquet")),
        Some("nfs4")
    );
    assert!(m.is_network(Path::new("/mnt/gilead/data/sets/prices.parquet")));
}

#[test]
fn test_the_deepest_mount_wins() {
    let m = mounts();
    // /home is btrfs and /mnt/gilead/data is nfs4; neither should claim the other.
    assert_eq!(m.fstype_for(Path::new("/home/x")), Some("btrfs"));
    assert_eq!(m.fstype_for(Path::new("/mnt/gilead/data/x")), Some("nfs4"));
}

#[test]
fn test_locality_separates_what_behaves_differently() {
    let m = mounts();
    assert_eq!(m.describe(Path::new("/home/x")).locality, Locality::Local);
    assert_eq!(m.describe(Path::new("/tmp/x")).locality, Locality::Memory);
    assert_eq!(
        m.describe(Path::new("/mnt/gilead/data/x")).locality,
        Locality::Network
    );
    assert_eq!(
        m.describe(Path::new("/media/backup/x")).locality,
        Locality::Network,
        "sshfs reads cross a network even though it is a fuse mount"
    );
}

#[test]
fn test_local_disk_is_not_worth_flagging_but_everything_else_is() {
    // Saying "ext4" on every row would be noise. Saying "nfs4" on one row is not.
    let m = mounts();
    assert!(!m.describe(Path::new("/home/x")).notable());
    assert!(m.describe(Path::new("/tmp/x")).notable());
    assert!(m.describe(Path::new("/mnt/gilead/data/x")).notable());
}

#[test]
fn test_object_store_urls_are_recognised_without_touching_the_network() {
    let m = mounts();
    for (url, scheme) in [
        ("s3://bucket/prefix/data.parquet", "s3"),
        ("gs://bucket/data.parquet", "gs"),
        ("https://example.com/data.parquet", "http"),
    ] {
        let source = m.describe(Path::new(url));
        assert_eq!(source.fstype, scheme, "for {url}");
        assert_eq!(source.locality, Locality::Object, "for {url}");
        assert!(source.notable());
    }
}

#[test]
fn test_a_path_no_mount_covers_is_unknown_rather_than_wrong() {
    let m = Mounts::parse("");
    let source = m.describe(Path::new("/anywhere"));
    assert_eq!(source.locality, Locality::Unknown);
    assert_eq!(source.fstype, "unknown");
}

#[test]
fn test_a_relative_path_resolves_against_the_working_directory() {
    // Mount points are absolute. Without joining, a file sitting on the disk under
    // the caller's feet reports "unknown".
    let m = Mounts::current();
    let here = m.describe(Path::new("Cargo.toml"));
    assert_ne!(
        here.locality,
        Locality::Unknown,
        "a file in the working directory should resolve to a real filesystem"
    );
}

#[test]
fn test_a_recorded_filesystem_name_classifies_the_same_way() {
    // A source read from the cache has no path to resolve, only the name.
    assert_eq!(Source::from_fstype("nfs4").locality, Locality::Network);
    assert_eq!(Source::from_fstype("ext4").locality, Locality::Local);
    assert_eq!(Source::from_fstype("tmpfs").locality, Locality::Memory);
    assert_eq!(Source::from_fstype("s3").locality, Locality::Object);
}
