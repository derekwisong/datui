#!/usr/bin/env python3
"""
Bump version number in all Cargo.toml packages, README.md, and Python bindings.

Updated files:
  - Cargo.toml (root / main package)
  - crates/datui-cli/Cargo.toml
  - crates/datui-lib/Cargo.toml
  - crates/datui-pyo3/Cargo.toml
  - python/pyproject.toml
  - README.md (version badge; release only)

With -dev suffix workflow:
  1. During development: version is "X.Y.Z-dev"
  2. Prepare release: "X.Y.Z-dev" -> "X.Y.Z" (remove -dev, commit, tag)
  3. Start next cycle: "X.Y.Z" -> "X.Y.Z+1-dev" (bump + add -dev, commit)

Best practice (release): CI must pass for the release commit before the Release
workflow will build. The script walks you through; use --tag-only to create and
push the tag (no manual git tag commands). Recommended flow:
  1. bump_version.py release --commit   (commits release version, no tag yet)
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
        choices=["release", "major", "minor", "patch"],
        help="Version operation: 'release' removes -dev, others bump and add -dev",
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

    # --tag-only: create and push tag for current commit (run after CI passes)
    if is_release and args.tag_only:
        _, _, _, suffix = parse_version(current_version)
        if suffix == "-dev":
            print(
                "Error: Current version has -dev suffix. Run 'release --commit' first, push, wait for CI, then run --tag-only.",
                file=sys.stderr,
            )
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
