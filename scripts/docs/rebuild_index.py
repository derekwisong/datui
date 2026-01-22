#!/usr/bin/env python3
"""
Rebuild the index page for the documentation.
Scans the book directory for all version directories and generates index.html
using a Jinja2 template.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional

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
    """Get the commit date for a git reference."""
    try:
        env = os.environ.copy()
        env["TZ"] = timezone
        result = subprocess.run(
            ["git", "log", "-1", "--date=format:%Y-%m-%d %H:%M UTC", "--format=%ad", ref],
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


def collect_versions(output_dir: Path) -> List[Dict[str, any]]:
    """Collect all version directories and their metadata."""
    versions = []
    
    # Add main/latest version first if it exists
    main_dir = output_dir / "main"
    if main_dir.exists() and main_dir.is_dir():
        date_str = "Development version - most current features"
        if check_git_ref_exists("main"):
            main_date = get_git_date("main")
            if main_date:
                date_str = main_date
        
        versions.append({
            "name": "main",
            "path": "main",
            "is_development": True,
            "is_latest_stable": False,
            "date_str": date_str
        })
    
    # Get all version tags from the book directory
    if output_dir.exists():
        # Find all directories that look like version tags (v*)
        version_dirs = sorted(
            [d for d in output_dir.iterdir() if d.is_dir() and d.name.startswith("v")],
            key=lambda x: x.name,
            reverse=True
        )
        
        # Mark the first (newest) tag as latest stable
        for idx, version_dir in enumerate(version_dirs):
            version_name = version_dir.name
            
            date_str = "Release version"
            if check_git_ref_exists(version_name):
                tag_date = get_git_date(version_name)
                if tag_date:
                    date_str = tag_date
            
            versions.append({
                "name": version_name,
                "path": version_name,
                "is_development": False,
                "is_latest_stable": idx == 0,  # First tag is latest stable
                "date_str": date_str
            })
    
    return versions


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
    versions = collect_versions(output_dir)
    
    # Render the template
    output_html = template.render(versions=versions)
    
    # Write the output file
    output_file = output_dir / "index.html"
    output_file.write_text(output_html, encoding="utf-8")
    
    print("✓ Index page regenerated")


if __name__ == "__main__":
    main()
