#!/usr/bin/env python3
"""
Bump version number in Cargo.toml, README.md, Python bindings (datui-pyo3, python/pyproject.toml).

With -dev suffix workflow:
  1. During development: version is "X.Y.Z-dev"
  2. Prepare release: "X.Y.Z-dev" -> "X.Y.Z" (remove -dev, commit, tag)
  3. Start next cycle: "X.Y.Z" -> "X.Y.Z+1-dev" (bump + add -dev, commit)

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

Examples:
    # Prepare for release (remove -dev)
    python scripts/bump_version.py release --commit --tag
    git push && git push --tags

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
            commit_message = f"chore: release {version}"
        else:
            commit_message = f"chore: bump version to {version}"
        
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

Examples:
  # Prepare for release (remove -dev)
  python scripts/bump_version.py release --commit --tag
  git push && git push --tags

  # Start next development cycle (bump + add -dev)
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
    
    args = parser.parse_args()
    
    # --tag only valid for release
    if args.tag and args.command != "release":
        print("Error: --tag can only be used with 'release' command", file=sys.stderr)
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

    datui_cli_cargo = project_root / "crates" / "datui-cli" / "Cargo.toml"
    datui_lib_cargo = project_root / "crates" / "datui-lib" / "Cargo.toml"
    datui_pyo3_cargo = project_root / "crates" / "datui-pyo3" / "Cargo.toml"
    pyproject_path = project_root / "python" / "pyproject.toml"

    # Update files
    try:
        # Update main Cargo.toml (source of truth)
        update_cargo_toml(cargo_toml_path, current_version, new_version, project_root)
        
        # Update datui-cli Cargo.toml to match the new version (regardless of its current version)
        if datui_cli_cargo.exists():
            update_cargo_toml_to_version(datui_cli_cargo, new_version, project_root)
        
        # Update crates/datui-lib Cargo.toml to match the new version
        if datui_lib_cargo.exists():
            update_cargo_toml_to_version(datui_lib_cargo, new_version, project_root)
        
        # Update datui-pyo3 Cargo.toml to match the new version
        if datui_pyo3_cargo.exists():
            update_cargo_toml_to_version(datui_pyo3_cargo, new_version, project_root)
        
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
                print("Next steps:")
                print("  1. git push && git push --tags")
                print("  2. python scripts/bump_version.py patch --commit  # Start next dev cycle")
            else:
                print()
                print(f"Version update complete: {current_version} -> {new_version} (committed)")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
