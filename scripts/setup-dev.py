#!/usr/bin/env python3
"""
Development environment setup script for datui.

This script:
- Creates and manages a Python virtual environment (.venv)
- Installs Python dependencies from scripts/requirements.txt
- Ensures mdbook is installed at the correct version (matching CI)
- Regenerates test data
- Builds local documentation

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
    doc_script = Path(__file__).parent / "docs" / "build_all_docs_local.sh"
    
    if not doc_script.exists():
        print(f"Warning: {doc_script} not found. Skipping documentation build.")
        return
    
    # Make sure the script is executable
    if sys.platform != "win32":
        os.chmod(doc_script, 0o755)
    
    # Check if mdbook is available (required for docs)
    mdbook_path = find_mdbook()
    if not mdbook_path:
        print("Warning: mdbook not found. Skipping documentation build.")
        print("  Documentation will be built after mdbook is installed.")
        return
    
    # Ensure mdbook is in PATH for the script
    env = os.environ.copy()
    if mdbook_path and str(Path(mdbook_path).parent) not in env.get("PATH", ""):
        # Add mdbook's directory to PATH if it's not already there
        cargo_bin = str(Path(mdbook_path).parent)
        if sys.platform == "win32":
            env["PATH"] = f"{cargo_bin};{env.get('PATH', '')}"
        else:
            env["PATH"] = f"{cargo_bin}:{env.get('PATH', '')}"
    
    # Run the documentation build script
    # Use bash explicitly for better cross-platform compatibility
    # Redirect stdin to /dev/null to ensure non-interactive mode
    if sys.platform == "win32":
        # On Windows, try to use Git Bash or WSL if available
        result = run_command(
            ["bash", str(doc_script)],
            check=False,
            env=env,
            stdin=subprocess.DEVNULL
        )
    else:
        result = run_command(
            ["bash", str(doc_script)],
            check=False,
            env=env,
            stdin=subprocess.DEVNULL
        )
    
    if result.returncode == 0:
        print("✓ Local documentation built successfully")
        print(f"  Documentation is available in: {REPO_ROOT / 'book'}")
    else:
        print("Warning: Documentation build had errors. Check output above.")
        print("  You can manually run: bash scripts/docs/build_all_docs_local.sh")


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
    print("You can run this script again at any time to update dependencies.")


if __name__ == "__main__":
    main()
