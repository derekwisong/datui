#!/usr/bin/env python3
"""
Bump version number in Cargo.toml and README.md

Usage:
    python scripts/bump_version.py [major|minor|patch]

Examples:
    python scripts/bump_version.py patch   # 0.2.1 -> 0.2.2
    python scripts/bump_version.py minor   # 0.2.1 -> 0.3.0
    python scripts/bump_version.py major   # 0.2.1 -> 1.0.0
"""

import re
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


def update_cargo_toml(file_path: Path, old_version: str, new_version: str) -> None:
    """Update version in Cargo.toml."""
    content = file_path.read_text()
    # Match: version = "0.2.1"
    pattern = rf'version\s*=\s*"{re.escape(old_version)}"'
    replacement = f'version = "{new_version}"'
    new_content = re.sub(pattern, replacement, content)

    if new_content == content:
        raise ValueError(f"Could not find version {old_version} in Cargo.toml")
    
    file_path.write_text(new_content)
    print(f"✓ Updated Cargo.toml: {old_version} -> {new_version}")


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


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    bump_type = sys.argv[1].lower()
    if bump_type not in ("major", "minor", "patch"):
        print(f"Error: Invalid bump type '{bump_type}'. Must be major, minor, or patch.", file=sys.stderr)
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    # Get project root (parent of scripts/)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    cargo_toml_path = project_root / "Cargo.toml"
    readme_path = project_root / "README.md"

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

    # Update files
    try:
        update_cargo_toml(cargo_toml_path, current_version, new_version)
        update_readme(readme_path, current_version, new_version)
        print()
        print(f"✓ Version bumped successfully: {current_version} -> {new_version}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
