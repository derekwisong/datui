#!/usr/bin/env python3
"""
Development environment setup script for datui.

This script:
- Creates and manages a Python virtual environment (.venv)
- Installs Python dependencies from scripts/requirements.txt (and requirements-wheel.txt on Linux/macOS)
- Installs/updates pre-commit hooks
- Ensures mdbook is installed at the correct version (matching CI)
- Regenerates test data
- Builds local documentation

The Rust workspace has the main app at the root (datui) and library crates (datui-lib, datui-cli). The Python
binding crate (datui-pyo3) is not in the workspace and is built separately
with maturin. See the final "Next steps" output for build/test commands.

Can be run multiple times safely - it's idempotent and non-destructive.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path


# Configuration
# Script is in scripts/, so go up one level to get repo root
REPO_ROOT = Path(__file__).parent.parent.resolve()
VENV_DIR = REPO_ROOT / ".venv"
REQUIREMENTS_FILE = Path(__file__).parent / "requirements.txt"
REQUIREMENTS_WHEEL_FILE = Path(__file__).parent / "requirements-wheel.txt"
REQUIREMENTS_WHEEL_WINDOWS_FILE = Path(__file__).parent / "requirements-wheel-windows.txt"
MDBOOK_VERSION = "0.5.2"  # Must match .github/workflows/ci.yml and release.yml


def get_venv_python():
    """Get the path to the venv's Python executable."""
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "python.exe"
    else:
        return VENV_DIR / "bin" / "python"


def get_venv_pip():
    """Get the path to the venv's pip executable."""
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "pip"
    else:
        return VENV_DIR / "bin" / "pip"


def run_command(cmd, check=True, cwd=None, env=None, stdin=None):
    """Run a command and return the result."""
    print(f"Running: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    result = subprocess.run(
        cmd,
        shell=isinstance(cmd, str),
        check=check,
        cwd=cwd or REPO_ROOT,
        env=env,
        stdin=stdin,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Error output: {result.stderr}", file=sys.stderr)
    return result


def create_venv():
    """Create the virtual environment if it doesn't exist."""
    if VENV_DIR.exists():
        print(f"✓ Virtual environment already exists at {VENV_DIR}")
        return False
    else:
        print(f"Creating virtual environment at {VENV_DIR}...")
        run_command([sys.executable, "-m", "venv", str(VENV_DIR)])
        print(f"✓ Virtual environment created")
        return True


def ensure_venv_activated():
    """Check if we're running in the venv, and provide instructions if not."""
    venv_python = get_venv_python()
    if not venv_python.exists():
        print("Error: Virtual environment Python not found. Please run this script again.")
        sys.exit(1)
    
    # Check if we're using the venv's Python
    current_python = Path(sys.executable).resolve()
    if current_python != venv_python.resolve():
        print(f"Note: Not running in venv. The script will use {venv_python} for commands.")
        print("   For interactive use, activate the venv with:")
        if sys.platform == "win32":
            print(f"   {VENV_DIR}\\Scripts\\activate")
        else:
            print(f"   source {VENV_DIR}/bin/activate")


def upgrade_pip():
    """Upgrade pip in the virtual environment."""
    print("Upgrading pip...")
    venv_pip = get_venv_pip()
    run_command([str(venv_pip), "install", "--upgrade", "pip", "--quiet"])


def install_requirements():
    """Install Python requirements from scripts/requirements.txt."""
    if not REQUIREMENTS_FILE.exists():
        print(f"Warning: {REQUIREMENTS_FILE} not found. Skipping requirements installation.")
        return
    
    print(f"Installing requirements from {REQUIREMENTS_FILE}...")
    venv_pip = get_venv_pip()
    run_command([str(venv_pip), "install", "-r", str(REQUIREMENTS_FILE)])
    print("✓ Requirements installed")

    # Wheel build deps: Linux/macOS use patchelf + maturin + pytest; Windows uses maturin + pytest only
    if sys.platform == "win32":
        wheel_file = REQUIREMENTS_WHEEL_WINDOWS_FILE
    else:
        wheel_file = REQUIREMENTS_WHEEL_FILE
    if wheel_file.exists():
        print(f"Installing wheel build deps from {wheel_file.name}...")
        run_command([str(venv_pip), "install", "-r", str(wheel_file)])
        print("✓ Wheel build requirements installed")


def get_venv_pre_commit():
    """Get the path to the venv's pre-commit executable."""
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "pre-commit.exe"
    else:
        return VENV_DIR / "bin" / "pre-commit"


def install_pre_commit_hooks():
    """Install or update pre-commit hooks."""
    print("Installing/updating pre-commit hooks...")
    
    # Check if pre-commit config exists
    pre_commit_config = REPO_ROOT / ".pre-commit-config.yaml"
    if not pre_commit_config.exists():
        print(f"Warning: {pre_commit_config} not found. Skipping pre-commit hook installation.")
        return
    
    # Try to find pre-commit executable
    venv_pre_commit = get_venv_pre_commit()
    
    # Check if pre-commit is installed in venv
    if not venv_pre_commit.exists():
        # Try to find it in PATH (might be installed globally)
        pre_commit_path = shutil.which("pre-commit")
        if not pre_commit_path:
            print("Warning: pre-commit not found. It should be in requirements.txt.")
            print("  Skipping pre-commit hook installation.")
            return
        pre_commit_cmd = [pre_commit_path]
    else:
        pre_commit_cmd = [str(venv_pre_commit)]
    
    # Run pre-commit install
    result = run_command(
        pre_commit_cmd + ["install"],
        check=False
    )
    
    if result.returncode == 0:
        print("✓ Pre-commit hooks installed/updated")
    else:
        print("Warning: Failed to install pre-commit hooks.")
        print("  You can manually run: pre-commit install")


def find_mdbook():
    """Find mdbook executable in common locations."""
    # Check PATH first
    mdbook_path = shutil.which("mdbook")
    if mdbook_path:
        return mdbook_path
    
    # Check common cargo install location
    cargo_bin = Path.home() / ".cargo" / "bin" / "mdbook"
    if cargo_bin.exists():
        return str(cargo_bin)
    
    return None


def check_mdbook_installed():
    """Check if mdbook is installed and return the version, or None if not installed."""
    mdbook_path = find_mdbook()
    if not mdbook_path:
        return None
    
    try:
        result = run_command([mdbook_path, "--version"], check=False)
        if result.returncode == 0:
            # mdbook --version outputs something like "mdbook v0.5.2"
            version_line = result.stdout.strip()
            # Extract version number
            if "v" in version_line:
                installed_version = version_line.split("v")[-1].split()[0]
                return installed_version
            return None
    except (FileNotFoundError, subprocess.SubprocessError):
        pass
    return None


def install_mdbook():
    """Install mdbook at the correct version using cargo."""
    print(f"Checking mdbook installation (required version: {MDBOOK_VERSION})...")
    
    installed_version = check_mdbook_installed()
    
    if installed_version == MDBOOK_VERSION:
        print(f"✓ mdbook {MDBOOK_VERSION} is already installed")
        return
    
    if installed_version:
        print(f"  Found mdbook {installed_version}, but need {MDBOOK_VERSION}")
        print(f"  Installing mdbook {MDBOOK_VERSION}...")
    else:
        print(f"  mdbook not found. Installing mdbook {MDBOOK_VERSION}...")
    
    # Check if cargo is available
    cargo_result = run_command(["cargo", "--version"], check=False)
    if cargo_result.returncode != 0:
        print("Error: cargo is not installed or not in PATH.")
        print("Please install Rust and cargo first: https://rustup.rs/")
        sys.exit(1)
    
    # Install mdbook
    print(f"  Running: cargo install mdbook --version {MDBOOK_VERSION} --locked")
    result = run_command(
        ["cargo", "install", "mdbook", "--version", MDBOOK_VERSION, "--locked"],
        check=False
    )
    
    if result.returncode != 0:
        print("Error: Failed to install mdbook. Please check the error messages above.")
        sys.exit(1)
    
    # Verify installation
    installed_version = check_mdbook_installed()
    if installed_version == MDBOOK_VERSION:
        print(f"✓ mdbook {MDBOOK_VERSION} installed successfully")
    else:
        print(f"Warning: mdbook was installed but version check failed.")
        print(f"  Expected: {MDBOOK_VERSION}, Got: {installed_version}")


def regenerate_test_data():
    """Regenerate test data using the venv's Python."""
    print("Regenerating test data...")
    venv_python = get_venv_python()
    script_path = Path(__file__).parent / "generate_sample_data.py"
    
    if not script_path.exists():
        print(f"Warning: {script_path} not found. Skipping test data generation.")
        return
    
    run_command([str(venv_python), str(script_path)])
    print("✓ Test data regenerated")


def build_local_documentation():
    """Build local documentation using the build script."""
    print("Building local documentation...")
    doc_script = Path(__file__).parent / "docs" / "build_all_docs_local.py"
    
    if not doc_script.exists():
        print(f"Warning: {doc_script} not found. Skipping documentation build.")
        return
    
    # Check if mdbook is available (required for docs)
    mdbook_path = find_mdbook()
    if not mdbook_path:
        print("Warning: mdbook not found. Skipping documentation build.")
        print("  Documentation will be built after mdbook is installed.")
        return
    
    # Ensure mdbook is in PATH for the script
    env = os.environ.copy()
    if mdbook_path and str(Path(mdbook_path).parent) not in env.get("PATH", ""):
        cargo_bin = str(Path(mdbook_path).parent)
        if sys.platform == "win32":
            env["PATH"] = f"{cargo_bin};{env.get('PATH', '')}"
        else:
            env["PATH"] = f"{cargo_bin}:{env.get('PATH', '')}"
    
    result = run_command(
        [sys.executable, str(doc_script)],
        check=False,
        env=env,
        stdin=subprocess.DEVNULL
    )
    
    if result.returncode == 0:
        print("✓ Local documentation built successfully")
        print(f"  Documentation is available in: {REPO_ROOT / 'book'}")
    else:
        print("Warning: Documentation build had errors. Check output above.")
        print("  You can manually run: python3 scripts/docs/build_all_docs_local.py")


def main():
    """Main setup function."""
    print("=" * 60)
    print("datui Development Environment Setup")
    print("=" * 60)
    print()
    
    # Change to repo root
    os.chdir(REPO_ROOT)
    
    # Create venv if needed
    venv_created = create_venv()
    
    # Check venv activation status
    ensure_venv_activated()
    
    # Get venv Python for subsequent commands
    venv_python = get_venv_python()
    if not venv_python.exists():
        print("Error: Virtual environment Python executable not found.")
        sys.exit(1)
    
    # Upgrade pip (especially important for new venvs)
    if venv_created:
        upgrade_pip()
    
    # Install/update requirements
    install_requirements()
    
    # Install/update pre-commit hooks
    install_pre_commit_hooks()
    
    # Install mdbook
    install_mdbook()
    
    # Regenerate test data
    regenerate_test_data()
    
    # Build local documentation
    build_local_documentation()
    
    print()
    print("=" * 60)
    print("✓ Setup complete!")
    print("=" * 60)
    print()
    print("To activate the virtual environment:")
    if sys.platform == "win32":
        print(f"  {VENV_DIR}\\Scripts\\activate")
    else:
        print(f"  source {VENV_DIR}/bin/activate")
    print()
    print("Rust (workspace: root = datui binary, crates/datui-lib, crates/datui-cli):")
    print("  cargo build --workspace")
    print("  cargo test --workspace")
    print("  cargo run -- <args>   # run the CLI (from repo root)")
    print()
    print("Python bindings (optional; requires maturin and compatible polars versions):")
    print("  cd python && maturin develop")
    print("  pytest python/tests/ -v")
    print()
    print("You can run this script again at any time to update dependencies.")


if __name__ == "__main__":
    main()
