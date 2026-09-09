//! Release notes, and the wiring that gets them onto the GitHub release.
//!
//! The release body is not cosmetic. `publish-packages.yml` runs about a minute
//! after the release is created, and komac copies whatever the body says into
//! the winget manifest as `ReleaseNotes`. An empty body there produces a winget
//! PR that is silently missing metadata the previous version had, which winget
//! flags only after a moderator is already looking at it. That is how 0.3.1
//! shipped: the notes were written on the release page six minutes too late.
//!
//! So the workflow composes the body itself: hand-written notes from
//! `release-notes/<tag>.md` when they exist, and the commit log when they do
//! not. Notes are optional; a body is not. These tests guard that the composing
//! step is still wired up, and that a notes file which does exist is finished.
//!
//! CI has to pass on the release commit before the tag is pushed, so these run
//! at exactly the moment they can still prevent a bad release.

const RELEASE_WORKFLOW: &str = ".github/workflows/release.yml";
const PUBLISH_WORKFLOW: &str = ".github/workflows/publish-packages.yml";
const NOTES_TODO_SENTINEL: &str = "TODO: write the release notes";

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path} should exist: {e}"))
}

/// The version in Cargo.toml, and whether this is a release commit.
///
/// Between releases the version carries a `-dev` suffix and there is nothing to
/// check yet; the notes are written when `bump_version.py release` drops it.
fn release_version() -> Option<&'static str> {
    let version = env!("CARGO_PKG_VERSION");
    if version.ends_with("-dev") {
        None
    } else {
        Some(version)
    }
}

#[test]
fn test_notes_for_this_release_are_finished() {
    // Notes are optional, so absence is fine. A file that exists is what ships,
    // and a half-written one would ship instead of the generated body.
    let Some(version) = release_version() else {
        return;
    };
    let path = format!("release-notes/v{version}.md");
    let Ok(notes) = std::fs::read_to_string(&path) else {
        return;
    };

    assert!(
        !notes.trim().is_empty(),
        "{path} is empty; delete it to let the release generate a body from the commit log"
    );
    assert!(
        !notes.contains(NOTES_TODO_SENTINEL),
        "{path} still has the scaffolded '{NOTES_TODO_SENTINEL}' line; finish it or delete it"
    );
}

#[test]
fn test_release_workflow_composes_a_body() {
    // Without this the release is created with an empty body, and the publish
    // pipeline reads that empty body before anyone can type into it.
    let workflow = read(RELEASE_WORKFLOW);
    assert!(
        workflow.contains("name: Compose release body"),
        "{RELEASE_WORKFLOW} should compose the release body before creating the release"
    );
    assert!(
        workflow.contains("body_path: ${{ steps.release_body.outputs.path }}"),
        "{RELEASE_WORKFLOW} should hand the composed body to the release as body_path; \
         without it the release is created empty and winget copies the emptiness"
    );
}

#[test]
fn test_winget_publish_refuses_an_empty_release_body() {
    // Belt and braces for the case where a tag is pushed by hand: better to fail
    // the job than to open a winget PR that is missing metadata.
    let workflow = read(PUBLISH_WORKFLOW);
    assert!(
        workflow.contains("Check the release body is populated (preflight)"),
        "{PUBLISH_WORKFLOW} should preflight the release body before running komac"
    );
}

#[test]
fn test_notes_are_named_for_their_tag() {
    // release.yml interpolates the tag straight into the filename, so a file
    // named anything else is invisible to the release no matter what it says.
    let dir = std::fs::read_dir("release-notes").expect("release-notes/ should exist");
    for entry in dir {
        let name = entry.expect("readable entry").file_name();
        let name = name.to_string_lossy().to_string();
        if name == "README.md" {
            continue;
        }
        assert!(
            name.starts_with('v') && name.ends_with(".md"),
            "release-notes/{name} should be named v<version>.md to match its tag"
        );
        let version = &name[1..name.len() - 3];
        assert!(
            version.split('.').count() == 3
                && version
                    .split('.')
                    .all(|part| part.chars().all(|c| c.is_ascii_digit())),
            "release-notes/{name} should be named for an X.Y.Z version"
        );
    }
}
