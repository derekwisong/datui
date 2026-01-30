#!/usr/bin/env python3
"""
Rebuild the index page for the documentation.
Scans the book directory for all version directories and generates index.html
using a Jinja2 template.
"""

import os
import subprocess
import sys
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple

try:
    from jinja2 import Environment, FileSystemLoader, TemplateNotFound
except ImportError:
    print("Error: jinja2 is required. Install it with: pip install jinja2", file=sys.stderr)
    sys.exit(1)


def get_repo_root() -> Path:
    """Get the repository root directory."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True
        )
        return Path(result.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback to current directory if git is not available
        return Path.cwd()


def get_git_date(ref: str, timezone: str = "UTC") -> Optional[str]:
    """Get the commit date for a git reference in UTC."""
    try:
        env = os.environ.copy()
        env["TZ"] = timezone
        # Use format-local: to respect TZ environment variable
        # This ensures we get actual UTC time, not local time labeled as UTC
        result = subprocess.run(
            ["git", "log", "-1", "--date=format-local:%Y-%m-%d %H:%M UTC", "--format=%ad", ref],
            capture_output=True,
            text=True,
            env=env,
            check=False
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return None


def check_git_ref_exists(ref: str) -> bool:
    """Check if a git reference exists."""
    try:
        subprocess.run(
            ["git", "rev-parse", ref],
            capture_output=True,
            check=False
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


# Match vX.Y.Z or vX.Y (patch 0) or vX (minor/patch 0). No leading zeros in numbers.
_VERSION_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$|^v(\d+)\.(\d+)$|^v(\d+)$")


def parse_version(name: str) -> Optional[Tuple[int, int, int]]:
    """Parse a version string like v1.2.3 into (major, minor, patch). Returns None if not parseable."""
    m = _VERSION_RE.match(name)
    if not m:
        return None
    g = m.groups()
    if g[0] is not None:
        return (int(g[0]), int(g[1]), int(g[2]))
    if g[3] is not None:
        return (int(g[3]), int(g[4]), 0)
    if g[5] is not None:
        return (int(g[5]), 0, 0)
    return None


def sort_version_dirs(version_dirs: List[Path]) -> List[Path]:
    """Sort version directories by 3-part semantic version (newest first). Unparseable names last."""
    def key(path: Path):
        t = parse_version(path.name)
        if t is None:
            return (0, 0, 0)  # put unparseable at end when reverse=True
        return t

    return sorted(version_dirs, key=key, reverse=True)


def get_dev_version(repo_root: Path) -> Optional[str]:
    """Get the version from Cargo.toml for 'main' branch docs display.

    Reads the actual version from the current checkout (main when called from
    the docs build). No inference - uses whatever version main actually has.
    """
    try:
        cargo_toml = repo_root / "Cargo.toml"
        if not cargo_toml.exists():
            return None
        content = cargo_toml.read_text()
        match = re.search(r'^\[package\]', content, re.MULTILINE)
        if not match:
            return None
        start = match.end()
        rest = content[start:]
        next_section = re.search(r'^\[', rest, re.MULTILINE)
        section = rest[:next_section.start()] if next_section else rest
        version_match = re.search(r'version\s*=\s*"([^"]+)"', section)
        return version_match.group(1) if version_match else None
    except Exception:
        return None


def collect_versions(output_dir: Path) -> Tuple[List[Dict], List[Dict]]:
    """Collect version directories and return (recent_versions, older_versions).
    recent_versions: main (if present) + 5 most recent tags.
    older_versions: remaining tags, sorted newest first.
    """
    recent: List[Dict] = []
    older: List[Dict] = []

    # Add main/latest version first if it exists
    main_dir = output_dir / "main"
    if main_dir.exists() and main_dir.is_dir():
        date_str = "Development version - most current features"
        if check_git_ref_exists("main"):
            main_date = get_git_date("main")
            if main_date:
                date_str = main_date
        
        # Get dev version from Cargo.toml if available
        repo_root = get_repo_root()
        dev_version = get_dev_version(repo_root)
        # Add "v" prefix to match release version format
        display_name = f"v{dev_version}" if dev_version else "main"
        
        recent.append({
            "name": display_name,
            "path": "main",
            "is_development": True,
            "is_latest_stable": False,
            "date_str": date_str,
        })

    if not output_dir.exists():
        return (recent, older)

    version_dirs = [d for d in output_dir.iterdir() if d.is_dir() and d.name.startswith("v")]
    sorted_dirs = sort_version_dirs(version_dirs)

    RECENT_TAG_COUNT = 5
    tag_entries: List[Dict] = []
    for idx, version_dir in enumerate(sorted_dirs):
        version_name = version_dir.name
        date_str = "Release version"
        if check_git_ref_exists(version_name):
            tag_date = get_git_date(version_name)
            if tag_date:
                date_str = tag_date
        tag_entries.append({
            "name": version_name,
            "path": version_name,
            "is_development": False,
            "is_latest_stable": idx == 0,
            "date_str": date_str,
        })

    recent.extend(tag_entries[:RECENT_TAG_COUNT])
    older.extend(tag_entries[RECENT_TAG_COUNT:])
    return (recent, older)


def main():
    """Main function to rebuild the index page."""
    repo_root = get_repo_root()
    os.chdir(repo_root)
    
    output_dir = repo_root / "book"
    output_dir.mkdir(exist_ok=True)
    
    # Locate the template file
    script_dir = Path(__file__).parent
    template_file = script_dir / "index.html.j2"
    
    if not template_file.exists():
        print(f"Error: Template file not found: {template_file}", file=sys.stderr)
        sys.exit(1)
    
    # Set up Jinja2 environment
    env = Environment(
        loader=FileSystemLoader(str(script_dir)),
        autoescape=False
    )
    
    try:
        template = env.get_template("index.html.j2")
    except TemplateNotFound:
        print(f"Error: Could not load template: {template_file}", file=sys.stderr)
        sys.exit(1)
    
    # Collect version information
    print("Rebuilding index page...")
    recent_versions, older_versions = collect_versions(output_dir)

    # Render the template
    output_html = template.render(
        recent_versions=recent_versions,
        older_versions=older_versions,
    )
    
    # Write the output file
    output_file = output_dir / "index.html"
    output_file.write_text(output_html, encoding="utf-8")
    
    print("✓ Index page regenerated")


if __name__ == "__main__":
    main()
