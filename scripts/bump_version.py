#!/usr/bin/env python3
"""
Bump version number in Cargo.toml and README.md

Usage:
    python scripts/bump_version.py [major|minor|patch] [--commit] [--tag]

Options:
    --commit    Commit the version changes with message "Version bumped to <version> with <script_name>"
    --tag       Create a git tag for the version bump commit with message "Release <version>"
                (implies --commit)

Examples:
    python scripts/bump_version.py patch           # 0.2.1 -> 0.2.2
    python scripts/bump_version.py patch --commit  # Bump and commit
    python scripts/bump_version.py minor --tag     # Bump, commit, and tag
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def parse_version(version_str: str) -> tuple[int, int, int]:
    """Parse version string like '0.2.1' into (major, minor, patch)."""
    match = re.match(r'^(\d+)\.(\d+)\.(\d+)$', version_str)
    if not match:
        raise ValueError(f"Invalid version format: {version_str}")
    return tuple(map(int, match.groups()))


def format_version(major: int, minor: int, patch: int) -> str:
    """Format version tuple into string like '0.2.1'."""
    return f"{major}.{minor}.{patch}"


def bump_version(current: str, bump_type: str) -> str:
    """Bump version according to type (major, minor, or patch)."""
    major, minor, patch = parse_version(current)

    if bump_type == "major":
        return format_version(major + 1, 0, 0)
    elif bump_type == "minor":
        return format_version(major, minor + 1, 0)
    elif bump_type == "patch":
        return format_version(major, minor, patch + 1)
    else:
        raise ValueError(f"Invalid bump type: {bump_type}. Must be major, minor, or patch.")


def update_cargo_toml(
    file_path: Path, old_version: str, new_version: str, project_root: Path
) -> None:
    """Update version in a Cargo.toml file."""
    content = file_path.read_text()
    pattern = rf'version\s*=\s*"{re.escape(old_version)}"'
    replacement = f'version = "{new_version}"'
    new_content = re.sub(pattern, replacement, content)

    if new_content == content:
        raise ValueError(f"Could not find version {old_version} in {file_path}")
    file_path.write_text(new_content)
    try:
        rel = file_path.relative_to(project_root)
    except ValueError:
        rel = file_path
    print(f"✓ Updated {rel}: {old_version} -> {new_version}")


def update_readme(file_path: Path, old_version: str, new_version: str) -> None:
    """Update version badge in README.md."""
    content = file_path.read_text()
    # Match: ![Version](https://img.shields.io/badge/version-0.2.1-orange.svg)
    pattern = rf'!\[Version\]\(https://img\.shields\.io/badge/version-{re.escape(old_version)}-orange\.svg\)'
    replacement = f'![Version](https://img.shields.io/badge/version-{new_version}-orange.svg)'
    new_content = re.sub(pattern, replacement, content)

    if new_content == content:
        raise ValueError(f"Could not find version {old_version} in README.md badge")
    
    file_path.write_text(new_content)
    print(f"✓ Updated README.md: {old_version} -> {new_version}")


def get_current_version(cargo_toml_path: Path) -> str:
    """Extract current version from Cargo.toml."""
    content = cargo_toml_path.read_text()
    match = re.search(r'version\s*=\s*"([^"]+)"', content)
    if not match:
        raise ValueError("Could not find version in Cargo.toml")
    return match.group(1)


def commit_version_changes(project_root: Path, version: str, script_name: str) -> None:
    """Commit version changes to git."""
    try:
        # Stage the files (Cargo.lock may not exist for library crates)
        files_to_add = ["Cargo.toml", "README.md"]
        if (project_root / "crates" / "datui-cli" / "Cargo.toml").exists():
            files_to_add.append("crates/datui-cli/Cargo.toml")
        cargo_lock_path = project_root / "Cargo.lock"
        if cargo_lock_path.exists():
            files_to_add.append("Cargo.lock")
        
        subprocess.run(
            ["git", "add"] + files_to_add,
            cwd=project_root,
            check=True,
        )
        
        # Commit with message
        commit_message = f"Version bumped to {version} with {script_name}"
        subprocess.run(
            ["git", "commit", "-m", commit_message],
            cwd=project_root,
            check=True,
        )
        print(f"✓ Committed changes: {commit_message}")
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
        print(f"✓ Created tag: {tag_name} - {tag_message}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Git tag creation failed: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Bump version number in Cargo.toml and README.md",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/bump_version.py patch           # 0.2.1 -> 0.2.2
  python scripts/bump_version.py patch --commit  # Bump and commit
  python scripts/bump_version.py minor --tag     # Bump, commit, and tag
        """,
    )
    parser.add_argument(
        "bump_type",
        choices=["major", "minor", "patch"],
        help="Type of version bump (major, minor, or patch)",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help='Commit the version changes with message "Version bumped to <version> with <script_name>"',
    )
    parser.add_argument(
        "--tag",
        action="store_true",
        help='Create a git tag for the version bump commit with message "Release <version>" (implies --commit)',
    )
    
    args = parser.parse_args()
    
    # --tag implies --commit
    if args.tag:
        args.commit = True
    
    bump_type = args.bump_type.lower()

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

    # Get current version
    current_version = get_current_version(cargo_toml_path)
    print(f"Current version: {current_version}")

    # Calculate new version
    new_version = bump_version(current_version, bump_type)
    print(f"New version: {new_version}")
    print()

    datui_cli_cargo = project_root / "crates" / "datui-cli" / "Cargo.toml"

    # Update files
    try:
        update_cargo_toml(cargo_toml_path, current_version, new_version, project_root)
        if datui_cli_cargo.exists():
            update_cargo_toml(datui_cli_cargo, current_version, new_version, project_root)
        update_readme(readme_path, current_version, new_version)
        print()
        print(f"✓ Version bumped successfully: {current_version} -> {new_version}")
        
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
            print("✓ Cargo.lock updated")
        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to update Cargo.lock: {e}", file=sys.stderr)
            # Continue anyway - Cargo.lock might not exist or might be in .gitignore
        
        # Handle git operations if requested
        if args.commit:
            commit_version_changes(project_root, new_version, script_name)
            
            if args.tag:
                create_version_tag(project_root, new_version)
                print()
                print(f"✓ Version bump complete: {current_version} -> {new_version} (committed and tagged)")
            else:
                print()
                print(f"✓ Version bump complete: {current_version} -> {new_version} (committed)")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
