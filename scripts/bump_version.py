#!/usr/bin/env python3
"""
Bump version number in all Cargo.toml packages, README.md, and Python bindings.

Updated files:
  - Cargo.toml (root: package version and dependency versions for datui-lib, datui-cli)
  - crates/datui-cli/Cargo.toml (package version only; no workspace path deps)
  - crates/datui-lib/Cargo.toml (package version and datui-cli dependency version)
  - crates/datui-pyo3/Cargo.toml
  - python/pyproject.toml
  - README.md (version badge; release only)

With -dev suffix workflow:
  1. During development: version is "X.Y.Z-dev"
  2. Prepare release: "X.Y.Z-dev" -> "X.Y.Z" (remove -dev, commit, tag)
  3. Start next cycle: "X.Y.Z" -> "X.Y.Z+1-dev" (bump + add -dev, commit)

Release notes are optional. The Release workflow always composes a body: it uses
release-notes/vX.Y.Z.md when that file is committed, and otherwise generates one
from the commit subjects since the last tag. Either way the body exists the
moment the release is created, which is what matters, because publishing copies
it into the winget manifest about a minute later and editing the release page
afterwards never reaches winget.

So write notes when a release deserves them and skip it when it does not:
  bump_version.py notes    scaffolds release-notes/vX.Y.Z.md from the commit log
The release commands only stop you for a file that exists but is unfinished, or
one written but left uncommitted at tag time. See release-notes/README.md.

Best practice (release): CI must pass for the release commit before the Release
workflow will build. The script walks you through; use --tag-only to create and
push the tag (no manual git tag commands). Recommended flow:
  0. bump_version.py notes              (optional; write release-notes/vX.Y.Z.md)
  1. bump_version.py release --commit   (commits release version and any notes,
                                         no tag yet)
  2. git push                           (push to main only)
  3. Wait for CI to pass on that commit
  4. bump_version.py release --tag-only (creates vX.Y.Z from Cargo.toml, pushes tag)
  5. bump_version.py patch --commit     (start next dev cycle)
  6. git push

If you use --tag and push main + tag together, Release may start before CI
finishes; re-run the Release workflow after CI passes, or use the flow above.

Usage:
    python scripts/bump_version.py release [--commit] [--tag]
    python scripts/bump_version.py [major|minor|patch] [--commit]

Commands:
    release         Remove -dev suffix (0.2.11-dev -> 0.2.11) for release
    major           Bump major version and add -dev (0.2.11 -> 1.0.0-dev)
    minor           Bump minor version and add -dev (0.2.11 -> 0.3.0-dev)
    patch           Bump patch version and add -dev (0.2.11 -> 0.2.12-dev)

Options:
    --commit        Commit the version changes
    --tag           Create a git tag (only for 'release' command, implies --commit)
    --tag-only      Create and push tag for current commit only (release; run after CI passes)

Examples:
    # Prepare for release (commit only, then push, wait CI, then run --tag-only)
    python scripts/bump_version.py release --commit
    git push
    # After CI passes:
    python scripts/bump_version.py release --tag-only

    # Start next development cycle (bump + add -dev)
    python scripts/bump_version.py patch --commit
    git push
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def parse_version(version_str: str) -> tuple[int, int, int, str]:
    """Parse version string like '0.2.1' or '0.2.1-dev' into (major, minor, patch, suffix)."""
    match = re.match(r'^(\d+)\.(\d+)\.(\d+)(-dev)?$', version_str)
    if not match:
        raise ValueError(f"Invalid version format: {version_str} (expected X.Y.Z or X.Y.Z-dev)")
    major, minor, patch, suffix = match.groups()
    return (int(major), int(minor), int(patch), suffix or "")


def format_version(major: int, minor: int, patch: int, suffix: str = "") -> str:
    """Format version tuple into string like '0.2.1' or '0.2.1-dev'."""
    return f"{major}.{minor}.{patch}{suffix}"


def bump_version(current: str, bump_type: str) -> str:
    """Bump version according to type (major, minor, or patch) and add -dev suffix."""
    major, minor, patch, suffix = parse_version(current)

    if bump_type == "major":
        return format_version(major + 1, 0, 0, "-dev")
    elif bump_type == "minor":
        return format_version(major, minor + 1, 0, "-dev")
    elif bump_type == "patch":
        return format_version(major, minor, patch + 1, "-dev")
    else:
        raise ValueError(f"Invalid bump type: {bump_type}. Must be major, minor, or patch.")


def prepare_release(current: str) -> str:
    """Remove -dev suffix to prepare for release (0.2.11-dev -> 0.2.11)."""
    major, minor, patch, suffix = parse_version(current)
    if suffix != "-dev":
        raise ValueError(f"Current version {current} is not a dev version (expected X.Y.Z-dev)")
    return format_version(major, minor, patch)


def _package_section_bounds(content: str) -> tuple[int, int] | None:
    """Return (start, end) byte range of the [package] section, or None if not found."""
    match = re.search(r'^\[package\]\s*$', content, re.MULTILINE)
    if not match:
        return None
    start = match.end()
    # End at next section header [.*] or end of file
    rest = content[start:]
    end_match = re.search(r'^\s*\[', rest, re.MULTILINE)
    end = start + (end_match.start() if end_match else len(rest))
    return (start, end)


def _update_dependency_version_in_line(line: str, path_fragment: str, new_version: str) -> str:
    """If line is a dependency with path containing path_fragment, add or update version. Else return unchanged."""
    if path_fragment not in line or "path" not in line or "=" not in line:
        return line
    if "version" in line:
        return re.sub(r'version\s*=\s*"[^"]*"', f'version = "{new_version}"', line)
    # Add version before the closing }
    return re.sub(r'\}\s*$', f', version = "{new_version}" }}', line)


def update_dependency_versions_in_cargo_toml(
    file_path: Path, path_fragments: list[str], new_version: str, project_root: Path
) -> None:
    """Ensure each dependency whose path contains one of path_fragments has version = new_version."""
    content = file_path.read_text()
    lines = content.split("\n")
    updated = []
    for line in lines:
        new_line = line
        for frag in path_fragments:
            new_line = _update_dependency_version_in_line(new_line, frag, new_version)
        updated.append(new_line)
    new_content = "\n".join(updated)
    if new_content != content:
        file_path.write_text(new_content)
        try:
            rel = file_path.relative_to(project_root)
        except ValueError:
            rel = file_path
        print(f"Updated dependency version(s) in {rel} -> {new_version}")


def update_cargo_toml(
    file_path: Path, old_version: str, new_version: str, project_root: Path
) -> None:
    """Update version in the [package] section of a Cargo.toml file only."""
    content = file_path.read_text()
    bounds = _package_section_bounds(content)
    if not bounds:
        raise ValueError(f"Could not find [package] section in {file_path}")
    start, end = bounds
    head, section, tail = content[:start], content[start:end], content[end:]
    pattern = rf'version\s*=\s*"{re.escape(old_version)}"'
    replacement = f'version = "{new_version}"'
    new_section = re.sub(pattern, replacement, section)
    if new_section == section:
        raise ValueError(f"Could not find version {old_version} in [package] of {file_path}")
    new_content = head + new_section + tail
    file_path.write_text(new_content)
    try:
        rel = file_path.relative_to(project_root)
    except ValueError:
        rel = file_path
    print(f"Updated {rel}: {old_version} -> {new_version}")


def update_cargo_toml_to_version(
    file_path: Path, target_version: str, project_root: Path
) -> None:
    """Update version in the [package] section to target_version (used for datui-cli)."""
    content = file_path.read_text()
    bounds = _package_section_bounds(content)
    if not bounds:
        raise ValueError(f"Could not find [package] section in {file_path}")
    start, end = bounds
    head, section, tail = content[:start], content[start:end], content[end:]
    old_match = re.search(r'version\s*=\s*"([^"]+)"', section)
    if not old_match:
        raise ValueError(f"Could not find version field in [package] of {file_path}")
    old_version = old_match.group(1)
    if old_version == target_version:
        try:
            rel = file_path.relative_to(project_root)
        except ValueError:
            rel = file_path
        print(f"{rel} already at version {target_version} (no change needed)")
        return
    pattern = r'version\s*=\s*"[^"]+"'
    new_section = re.sub(pattern, f'version = "{target_version}"', section, count=1)
    file_path.write_text(head + new_section + tail)
    try:
        rel = file_path.relative_to(project_root)
    except ValueError:
        rel = file_path
    print(f"Updated {rel}: {old_version} -> {target_version}")


def _project_section_bounds(content: str) -> tuple[int, int] | None:
    """Return (start, end) byte range of the [project] section, or None if not found."""
    match = re.search(r'^\[project\]\s*$', content, re.MULTILINE)
    if not match:
        return None
    start = match.end()
    rest = content[start:]
    end_match = re.search(r'^\s*\[', rest, re.MULTILINE)
    end = start + (end_match.start() if end_match else len(rest))
    return (start, end)


def update_pyproject_toml(
    file_path: Path, new_version: str, project_root: Path, is_release: bool
) -> None:
    """Update version in the [project] section of python/pyproject.toml.
    Release: X.Y.Z. Bump: X.Y.Z.dev0 (PEP 440) to match next release.
    """
    content = file_path.read_text()
    bounds = _project_section_bounds(content)
    if not bounds:
        raise ValueError(f"Could not find [project] section in {file_path}")
    start, end = bounds
    head, section, tail = content[:start], content[start:end], content[end:]
    if is_release:
        py_version = new_version  # e.g. 0.2.17
    else:
        # Convert 0.2.18-dev -> 0.2.18.dev0 (PEP 440)
        py_version = new_version.replace("-dev", ".dev0")
    old_match = re.search(r'version\s*=\s*"([^"]+)"', section)
    if not old_match:
        raise ValueError(f"Could not find version field in [project] of {file_path}")
    old_version = old_match.group(1)
    if old_version == py_version:
        try:
            rel = file_path.relative_to(project_root)
        except ValueError:
            rel = file_path
        print(f"{rel} already at version {py_version} (no change needed)")
        return
    pattern = r'version\s*=\s*"[^"]+"'
    new_section = re.sub(pattern, f'version = "{py_version}"', section, count=1)
    file_path.write_text(head + new_section + tail)
    try:
        rel = file_path.relative_to(project_root)
    except ValueError:
        rel = file_path
    print(f"Updated {rel}: {old_version} -> {py_version}")


def update_readme(file_path: Path, new_version: str) -> None:
    """Update version badge in README.md to the desired version (any current value)."""
    content = file_path.read_text()
    # Match badge with any version: ![Version](https://img.shields.io/badge/version-X.Y.Z-orange.svg)
    # Also match versions with -dev suffix
    pattern = r'!\[Version\]\(https://img\.shields\.io/badge/version-[^)-]+-orange\.svg\)'
    # URL-encode the version (replace - with --)
    url_version = new_version.replace('-', '--')
    replacement = f'![Version](https://img.shields.io/badge/version-{url_version}-orange.svg)'
    new_content = re.sub(pattern, replacement, content)

    if new_content == content:
        raise ValueError(
            "Could not find version badge in README.md "
            '(expected: ![Version](https://img.shields.io/badge/version-X.Y.Z-orange.svg))'
        )

    file_path.write_text(new_content)
    print(f"Updated README.md badge -> {new_version}")


def get_current_version(cargo_toml_path: Path) -> str:
    """Extract current version from [package] section of Cargo.toml."""
    content = cargo_toml_path.read_text()
    bounds = _package_section_bounds(content)
    if not bounds:
        raise ValueError("Could not find [package] section in Cargo.toml")
    start, end = bounds
    section = content[start:end]
    match = re.search(r'version\s*=\s*"([^"]+)"', section)
    if not match:
        raise ValueError("Could not find version in [package] of Cargo.toml")
    return match.group(1)


def commit_version_changes(project_root: Path, version: str, script_name: str, is_release: bool) -> None:
    """Commit version changes to git."""
    try:
        # Stage the files (Cargo.lock may not exist for library crates)
        files_to_add = ["Cargo.toml"]
        # Only include README.md for releases (badge only updated for releases)
        if is_release:
            files_to_add.append("README.md")
            # The notes have to be in the release commit: the Release workflow
            # reads them out of the tagged commit to build the release body.
            notes_rel = release_notes_relpath(version)
            if (project_root / notes_rel).exists():
                files_to_add.append(notes_rel)
        if (project_root / "crates" / "datui-cli" / "Cargo.toml").exists():
            files_to_add.append("crates/datui-cli/Cargo.toml")
        if (project_root / "crates" / "datui-lib" / "Cargo.toml").exists():
            files_to_add.append("crates/datui-lib/Cargo.toml")
        if (project_root / "crates" / "datui-pyo3" / "Cargo.toml").exists():
            files_to_add.append("crates/datui-pyo3/Cargo.toml")
        if (project_root / "python" / "pyproject.toml").exists():
            files_to_add.append("python/pyproject.toml")
        cargo_lock_path = project_root / "Cargo.lock"
        if cargo_lock_path.exists():
            files_to_add.append("Cargo.lock")
        # The two lockfiles outside the root workspace, refreshed above. Leaving them
        # out is what let v0.3.2 be tagged with both still naming the dev version.
        for side_lock in ("crates/datui-pyo3/Cargo.lock", "fuzz/Cargo.lock"):
            if (project_root / side_lock).exists():
                files_to_add.append(side_lock)

        subprocess.run(
            ["git", "add"] + files_to_add,
            cwd=project_root,
            check=True,
        )

        # Commit with appropriate message
        if is_release:
            commit_message = f"Release {version}\n\nGenerated by {script_name}"
        else:
            commit_message = f"Bump version to {version}\n\nGenerated by {script_name}"

        subprocess.run(
            ["git", "commit", "-m", commit_message],
            cwd=project_root,
            check=True,
        )
        print(f"Committed changes: {commit_message}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Git commit failed: {e}")


def create_version_tag(project_root: Path, version: str) -> None:
    """Create a git tag for the version."""
    try:
        tag_name = f"v{version}"
        tag_message = f"Release {version}"
        subprocess.run(
            ["git", "tag", "-a", tag_name, "-m", tag_message],
            cwd=project_root,
            check=True,
        )
        print(f"Created tag: {tag_name} - {tag_message}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Git tag creation failed: {e}")


def push_tag(project_root: Path, version: str) -> None:
    """Push the version tag to origin."""
    tag_name = f"v{version}"
    try:
        subprocess.run(
            ["git", "push", "origin", tag_name],
            cwd=project_root,
            check=True,
        )
        print(f"Pushed tag: {tag_name}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Git push tag failed: {e}")


# Optional hand-written release notes, one file per tag. release.yml prefers
# this file and falls back to the commit log, so the release body is never empty
# and never late. That timing is the whole point: publishing runs about a minute
# after the release is created and komac copies the body into the winget
# manifest, so notes typed onto the release page afterwards arrive too late.
# 0.3.1 shipped to winget with no ReleaseNotes that way.
RELEASE_NOTES_DIR = "release-notes"

# A scaffolded file still carrying this has not been written yet. Shipping that
# text would be worse than shipping the generated body, so the release commands
# stop for it.
NOTES_TODO_SENTINEL = "TODO: write the release notes"


def release_notes_relpath(version: str) -> str:
    """Repo-relative path to the notes for a version (release-notes/v0.3.2.md)."""
    return f"{RELEASE_NOTES_DIR}/v{version}.md"


def release_notes_path(project_root: Path, version: str) -> Path:
    """Absolute path to the notes for a version."""
    return project_root / release_notes_relpath(version)


def previous_release_tag(project_root: Path) -> str | None:
    """Most recent vX.Y.Z tag reachable from HEAD, or None on the first release."""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0", "--match", "v*"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        return None
    return result.stdout.strip() or None


def commit_subjects_since(project_root: Path, since_tag: str | None) -> list[str]:
    """Commit subjects since a tag, newest first, for scaffolding the notes."""
    rev_range = f"{since_tag}..HEAD" if since_tag else "HEAD"
    try:
        result = subprocess.run(
            ["git", "log", "--no-merges", "--format=%h %s", rev_range],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        return []
    return [line for line in result.stdout.splitlines() if line.strip()]


def scaffold_release_notes(project_root: Path, version: str) -> Path:
    """Write a starter notes file for a version. Never overwrites an existing one."""
    path = release_notes_path(project_root, version)
    if path.exists():
        return path

    previous = previous_release_tag(project_root)
    commits = commit_subjects_since(project_root, previous)
    repo = "https://github.com/derekwisong/datui"

    lines = [
        "## What's changed",
        "",
        f"{NOTES_TODO_SENTINEL} for v{version}, then delete this line.",
        "",
        "<!--",
        "This file becomes the GitHub release body verbatim, and komac copies it",
        "into the winget manifest. Write it for someone deciding whether to",
        "upgrade, not as a commit log. Delete the file to release without it; the",
        "body is then generated from the commits below.",
        "",
    ]
    if commits:
        since = previous or "the start of the project"
        lines.append(f"Commits since {since}, for reference. Delete what you do not use:")
        lines.extend(f"  {commit}" for commit in commits)
    else:
        lines.append("No commits found since the last tag.")
    lines.extend(["-->", ""])
    if previous:
        lines.append(f"**Full changelog**: {repo}/compare/{previous}...v{version}")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _describe_notes_problems(
    text: str | None, rel_path: str, location: str, required: bool
) -> list[str]:
    """Reasons the given notes content is not fit to ship.

    Absence is fine when the notes are optional: the workflow falls back to the
    commit log. Content that exists but is unfinished never is.
    """
    if text is None:
        return [f"{rel_path} does not exist {location}"] if required else []
    if not text.strip():
        return [f"{rel_path} is empty"]
    if NOTES_TODO_SENTINEL in text:
        return [f"{rel_path} still has the scaffolded '{NOTES_TODO_SENTINEL}' line"]
    return []


def check_release_notes_worktree(
    project_root: Path, version: str, required: bool = False
) -> list[str]:
    """Problems with the notes on disk. An empty list means there is nothing to fix."""
    path = release_notes_path(project_root, version)
    text = path.read_text(encoding="utf-8") if path.exists() else None
    return _describe_notes_problems(text, release_notes_relpath(version), "on disk", required)


def check_release_notes_committed(project_root: Path, version: str) -> list[str]:
    """Problems with the notes about to be tagged.

    The workflow reads the file out of the tagged commit, so what is on disk is
    not what ships. Notes sitting uncommitted at tag time are the trap this
    catches: the release would silently fall back to the generated body while
    the good text stays on the author's machine.
    """
    rel_path = release_notes_relpath(version)
    on_disk = release_notes_path(project_root, version).exists()
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{rel_path}"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        if on_disk:
            return [
                f"{rel_path} exists but is not committed, so the tag would fall back "
                f"to the generated body and ignore it"
            ]
        return []
    return _describe_notes_problems(result.stdout, rel_path, "at HEAD", required=False)


def report_notes_problems(problems: list[str], version: str) -> None:
    """Print what is wrong with the notes, and how to move on."""
    # Keep the two streams in order when the run is piped to a log.
    sys.stdout.flush()
    print("Error: the release notes are not ready to ship.", file=sys.stderr)
    for problem in problems:
        print(f"  - {problem}", file=sys.stderr)
    print(file=sys.stderr)
    print(f"Finish {release_notes_relpath(version)} and commit it, or delete it to", file=sys.stderr)
    print("let the release generate its body from the commit log instead.", file=sys.stderr)
    print(file=sys.stderr)
    print("Whichever you pick, the body has to be right before the tag is pushed:", file=sys.stderr)
    print("publishing copies it into the winget manifest about a minute later, and", file=sys.stderr)
    print("editing the release afterwards does not reach winget. See", file=sys.stderr)
    print("release-notes/README.md.", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Bump version number in Cargo.toml and README.md",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  release         Remove -dev suffix (0.2.11-dev -> 0.2.11) for release
  major           Bump major version and add -dev (0.2.11 -> 1.0.0-dev)
  minor           Bump minor version and add -dev (0.2.11 -> 0.3.0-dev)
  patch           Bump patch version and add -dev (0.2.11 -> 0.2.12-dev)

Release notes are optional; the release generates a body from the commit log
when there is no file. Write them when a release deserves it:
  python scripts/bump_version.py notes

Best practice (release): Push main first, wait for CI, then run --tag-only.
  python scripts/bump_version.py release --commit
  git push
  # After CI passes:
  python scripts/bump_version.py release --tag-only

Start next dev cycle:
  python scripts/bump_version.py patch --commit
  git push
        """,
    )
    parser.add_argument(
        "command",
        choices=["release", "major", "minor", "patch", "notes"],
        help="Version operation: 'release' removes -dev, others bump and add -dev; "
             "'notes' scaffolds the release notes for the upcoming release",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Commit the version changes",
    )
    parser.add_argument(
        "--tag",
        action="store_true",
        help="Create a git tag (only for 'release' command, implies --commit)",
    )
    parser.add_argument(
        "--tag-only",
        action="store_true",
        dest="tag_only",
        help="Create and push tag for current commit only (release command; run after CI passes)",
    )

    args = parser.parse_args()

    # --tag and --tag-only only valid for release
    if (args.tag or args.tag_only) and args.command != "release":
        print("Error: --tag and --tag-only can only be used with 'release' command", file=sys.stderr)
        sys.exit(1)

    # 'notes' writes one file and changes no version, so the git flags are meaningless
    if args.command == "notes" and args.commit:
        print("Error: 'notes' only writes the notes file; commit it with 'release --commit'", file=sys.stderr)
        sys.exit(1)

    # --tag-only and --commit/--tag are mutually exclusive
    if args.tag_only and (args.commit or args.tag):
        print("Error: --tag-only cannot be used with --commit or --tag", file=sys.stderr)
        sys.exit(1)

    # --tag implies --commit
    if args.tag:
        args.commit = True

    command = args.command.lower()
    is_release = (command == "release")

    # Get project root (parent of scripts/)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    script_name = Path(__file__).name

    cargo_toml_path = project_root / "Cargo.toml"
    readme_path = project_root / "README.md"
    cargo_lock_path = project_root / "Cargo.lock"

    if not cargo_toml_path.exists():
        print(f"Error: Could not find {cargo_toml_path}", file=sys.stderr)
        sys.exit(1)
    if not readme_path.exists():
        print(f"Error: Could not find {readme_path}", file=sys.stderr)
        sys.exit(1)

    # Get current version from main Cargo.toml (source of truth)
    current_version = get_current_version(cargo_toml_path)
    print(f"Current version (from main Cargo.toml): {current_version}")

    # 'notes' only writes the notes file; it touches no version at all.
    if command == "notes":
        # Works both before the release commit (X.Y.Z-dev) and after it (X.Y.Z),
        # so the notes can still be written once the version has been bumped.
        _, _, _, suffix = parse_version(current_version)
        release_version = prepare_release(current_version) if suffix == "-dev" else current_version
        path = release_notes_path(project_root, release_version)
        existed = path.exists()
        scaffold_release_notes(project_root, release_version)
        rel_path = release_notes_relpath(release_version)
        if existed:
            print(f"{rel_path} already exists; leaving it alone.")
        else:
            print(f"Scaffolded {rel_path} from the commits since the last tag.")
        print()
        print("Write it, then commit it with the release:")
        print(f"  python scripts/{script_name} release --commit")
        print()
        print("Skipping it is fine too. Delete the file and the release body will be")
        print("generated from the commit log instead.")
        return

    # --tag-only: create and push tag for current commit (run after CI passes)
    if is_release and args.tag_only:
        _, _, _, suffix = parse_version(current_version)
        if suffix == "-dev":
            print(
                "Error: Current version has -dev suffix. Run 'release --commit' first, push, wait for CI, then run --tag-only.",
                file=sys.stderr,
            )
            sys.exit(1)
        # Last gate before the Release workflow fires. Check the committed file,
        # not the working tree: the workflow reads it out of the tagged commit.
        problems = check_release_notes_committed(project_root, current_version)
        if problems:
            report_notes_problems(problems, current_version)
            sys.exit(1)
        tag_name = f"v{current_version}"
        try:
            create_version_tag(project_root, current_version)
            push_tag(project_root, current_version)
        except RuntimeError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        print()
        print(f"Tag {tag_name} created and pushed. Release workflow will run.")
        print()
        print("Next step: start the next dev cycle")
        print("  python scripts/bump_version.py patch --commit")
        print("  git push")
        return

    # Calculate new version
    try:
        if is_release:
            new_version = prepare_release(current_version)
        else:
            new_version = bump_version(current_version, command)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"New version: {new_version}")
    print()

    # Notes are optional: the Release workflow generates a body from the commit
    # log when there is no file. A half-written file is the one case worth
    # stopping for, since that is what would ship. Checked before anything is
    # modified, so a stopped run leaves the tree clean and can just be re-run.
    if is_release:
        problems = check_release_notes_worktree(project_root, new_version, required=False)
        if problems:
            report_notes_problems(problems, new_version)
            sys.exit(1)
        rel_path = release_notes_relpath(new_version)
        if (project_root / rel_path).exists():
            print(f"Release notes ready: {rel_path}")
        else:
            print(f"No {rel_path}; the release body will be generated from the commit log.")
            print(f"  To write them yourself: python scripts/{script_name} notes")
        print()

    # All Cargo.toml package versions to keep in sync (main is source of truth; others set to new_version)
    cargo_toml_files = [
        ("Cargo.toml", None),  # root: use update_cargo_toml with old -> new
        ("crates/datui-cli/Cargo.toml", "datui-cli"),
        ("crates/datui-lib/Cargo.toml", "datui-lib"),
        ("crates/datui-pyo3/Cargo.toml", "datui-pyo3"),
    ]
    pyproject_path = project_root / "python" / "pyproject.toml"

    # Update files
    try:
        # Update main Cargo.toml (source of truth)
        update_cargo_toml(cargo_toml_path, current_version, new_version, project_root)

        # Update datui-cli, datui-lib, datui-pyo3 to new_version (keep all crates in sync)
        for rel_path, _ in cargo_toml_files[1:]:
            crate_cargo = project_root / rel_path
            if crate_cargo.exists():
                update_cargo_toml_to_version(crate_cargo, new_version, project_root)

        # Keep datui and datui-cli dependency version in sync (for crates.io publish)
        update_dependency_versions_in_cargo_toml(
            cargo_toml_path,
            ["crates/datui-lib", "crates/datui-cli"],
            new_version,
            project_root,
        )
        datui_lib_cargo = project_root / "crates" / "datui-lib" / "Cargo.toml"
        if datui_lib_cargo.exists():
            update_dependency_versions_in_cargo_toml(
                datui_lib_cargo, ["../datui-cli"], new_version, project_root
            )

        # datui-pyo3 depends on datui-lib by path *and* version, and nothing used to
        # update that version, so every bump left it naming the previous one. A caret
        # requirement carrying a prerelease only accepts prereleases of that exact
        # version, so "0.3.2-dev" stopped matching the moment datui-lib became
        # "0.3.3-dev" and the crate would not resolve at all. It survived this long
        # because a *release* bump happens to stay in range: 0.3.2 satisfies
        # ^0.3.2-dev, and the wheels are only ever built from a release commit.
        datui_pyo3_cargo = project_root / "crates" / "datui-pyo3" / "Cargo.toml"
        if datui_pyo3_cargo.exists():
            update_dependency_versions_in_cargo_toml(
                datui_pyo3_cargo, ["../datui-lib"], new_version, project_root
            )

        # Update python/pyproject.toml: release -> X.Y.Z, bump -> X.Y.Z.dev0 (PEP 440)
        if pyproject_path.exists():
            update_pyproject_toml(pyproject_path, new_version, project_root, is_release)

        # Only update README badge for releases (not dev versions)
        if is_release:
            update_readme(readme_path, new_version)
        else:
            print("README.md badge unchanged (only updated for releases)")

        print()
        print(f"Version updated successfully: {current_version} -> {new_version}")

        # Update Cargo.lock by running cargo build/check
        print()
        print("Updating Cargo.lock...")
        try:
            subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=project_root,
                check=True,
                capture_output=True,
            )
            print("Cargo.lock updated")
        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to update Cargo.lock: {e}", file=sys.stderr)
            # Continue anyway - Cargo.lock might not exist or might be in .gitignore

        # crates/datui-pyo3 and fuzz are excluded from the root workspace, so the
        # cargo check above does not see their lockfiles, and both record the local
        # crate versions. Nothing refreshed them, so they went stale the moment a
        # version changed: v0.3.2 was tagged with two lockfiles still saying
        # 0.3.2-dev, and they had to be corrected by hand. `cargo metadata`
        # re-resolves and rewrites a lockfile without compiling anything.
        for manifest in ("crates/datui-pyo3/Cargo.toml", "fuzz/Cargo.toml"):
            manifest_path = project_root / manifest
            if not manifest_path.exists():
                continue
            try:
                subprocess.run(
                    [
                        "cargo",
                        "metadata",
                        "--manifest-path",
                        str(manifest_path),
                        "--format-version",
                        "1",
                    ],
                    cwd=project_root,
                    check=True,
                    capture_output=True,
                )
                print(f"Refreshed {Path(manifest).parent.as_posix()}/Cargo.lock")
            except subprocess.CalledProcessError as e:
                print(
                    f"Warning: Failed to refresh the lockfile for {manifest}: {e}",
                    file=sys.stderr,
                )

        # Handle git operations if requested
        if args.commit:
            commit_version_changes(project_root, new_version, script_name, is_release)

            if args.tag:
                create_version_tag(project_root, new_version)
                print()
                print(f"Release complete: {current_version} -> {new_version} (committed and tagged)")
                print()
                print("Next steps (best practice: push main first, wait for CI, then push tag):")
                print("  1. git push")
                print("  2. Wait for CI to pass on that commit")
                print("  3. python scripts/bump_version.py release --tag-only   # Creates and pushes v"
                      + new_version + " (triggers Release workflow)")
                print("  4. python scripts/bump_version.py patch --commit       # Start next dev cycle")
                print("  5. git push")
                print()
                print("If you push main and tag together (git push && git push --tags), Release may")
                print("run before CI finishes; re-run the Release workflow after CI passes.")
            else:
                print()
                if is_release:
                    print(f"Release version committed: {current_version} -> {new_version}")
                    print()
                    print("Next steps (best practice):")
                    print("  1. git push")
                    print("  2. Wait for CI to pass on this commit")
                    print("  3. python scripts/bump_version.py release --tag-only   # Creates and pushes v"
                          + new_version + " (triggers Release workflow)")
                    print("  4. python scripts/bump_version.py patch --commit       # Start next dev cycle")
                    print("  5. git push")
                else:
                    print(f"Version update complete: {current_version} -> {new_version} (committed)")
                    print()
                    print("Next step: git push")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
