#!/usr/bin/env python3
"""
Generate all demo GIFs from VHS .tape files.

This script builds the debug binary, ensures it's on PATH, and then generates
GIFs from all .tape files in the scripts/demos directory.

Must be run from repository root for paths to work correctly.

Usage:
    python scripts/demos/generate-all.py
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def prepare_tape_file(tape_file: Path, header_file: Path) -> Path:
    """
    Prepare a tape file by replacing the header section with the standard header.
    
    Looks for "# DO NOT DELETE - HEADER ABOVE" and replaces that line and everything
    above it with the Output line and content from header_file.
    
    Returns:
        Path to a temporary file with the prepared content, or the original file
        if no header marker is found.
    """
    # Read the tape file
    with open(tape_file, 'r', encoding='utf-8') as f:
        tape_lines = f.readlines()
    
    # Look for the header marker
    marker = "# DO NOT DELETE - HEADER ABOVE"
    marker_line_idx = None
    
    for i, line in enumerate(tape_lines):
        if marker in line:
            marker_line_idx = i
            break
    
    if marker_line_idx is None:
        # No marker found, return original file (backward compatibility)
        return tape_file
    
    # Read the header template
    with open(header_file, 'r', encoding='utf-8') as f:
        header_template = f.read().strip()
    
    # Generate output filename based on tape filename
    # e.g., "02-querying.tape" -> "demos/02-querying.gif"
    # Use relative path from repo root (vhs runs from repo root)
    output_name = tape_file.stem + ".gif"
    output_line = f"Output demos/{output_name}"
    
    # Build the new content: Output line + header template + everything after marker
    new_lines = [output_line + "\n", header_template + "\n"]
    
    # Add everything after the marker line (skip the marker line itself)
    if marker_line_idx + 1 < len(tape_lines):
        new_lines.extend(tape_lines[marker_line_idx + 1:])
    
    new_content = "".join(new_lines)
    
    # Write to temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.tape', delete=False)
    temp_path = Path(temp_file.name)
    temp_file.write(new_content)
    temp_file.flush()
    temp_file.close()
    
    return temp_path


def main():
    # Get script directory and repo root
    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent.parent.resolve()
    tapes_dir = script_dir
    demos_dir = repo_root / "demos"
    header_file = script_dir / "demo-tape-header.txt"
    
    # Ensure we're in the repo root (or at least can find it)
    if not (repo_root / "Cargo.toml").exists():
        print("Error: Could not find repository root. Please run from repository root.", file=sys.stderr)
        sys.exit(1)
    
    # Check if header file exists
    if not header_file.exists():
        print(f"Error: Header file not found: {header_file}", file=sys.stderr)
        sys.exit(1)
    
    # Build debug binary
    print("Building debug binary...")
    try:
        subprocess.run(
            ["cargo", "build", "--bin", "datui"],
            cwd=repo_root,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to build binary: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Check if binary exists
    binary_path = repo_root / "target" / "debug" / "datui"
    if not binary_path.exists():
        print("Error: datui binary not found. Please build it first.", file=sys.stderr)
        sys.exit(1)
    
    # Add binary directory to PATH
    binary_dir = binary_path.parent
    env = os.environ.copy()
    env["PATH"] = f"{binary_dir}:{env.get('PATH', '')}"
    
    # Create demos directory if it doesn't exist
    demos_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if vhs is installed
    try:
        subprocess.run(["vhs", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: vhs not found. Please install VHS and ensure it's in your PATH.", file=sys.stderr)
        sys.exit(1)
    
    # Find all .tape files
    tape_files = sorted(tapes_dir.glob("*.tape"))
    
    if not tape_files:
        print(f"Error: No .tape files found in {tapes_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Process each .tape file
    print(f"\nFound {len(tape_files)} tape file(s) to process...\n")
    
    temp_files = []  # Track temp files for cleanup
    
    for tape_file in tape_files:
        print(f"Generating GIF from {tape_file.name}...")
        
        # Prepare the tape file (replace header if marker is found)
        try:
            prepared_tape = prepare_tape_file(tape_file, header_file)
            
            # Track temp files for cleanup (if it's different from original)
            if prepared_tape != tape_file:
                temp_files.append(prepared_tape)
            
            # Run from repo root so output paths work correctly
            subprocess.run(
                ["vhs", str(prepared_tape)],
                cwd=repo_root,
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"  ⚠️  Warning: Failed to generate GIF from {tape_file.name}: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"  ⚠️  Warning: Error processing {tape_file.name}: {e}", file=sys.stderr)
            continue
    
    # Clean up temporary files
    for temp_file in temp_files:
        try:
            temp_file.unlink()
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to clean up temp file {temp_file}: {e}", file=sys.stderr)
    
    print(f"\n✅ All demos generated in {demos_dir}/")
    
    # List generated GIFs
    gif_files = sorted(demos_dir.glob("*.gif"))
    if gif_files:
        print(f"\nGenerated {len(gif_files)} GIF file(s):")
        for gif_file in gif_files:
            size_kb = gif_file.stat().st_size / 1024
            print(f"  - {gif_file.name} ({size_kb:.1f} KB)")
    else:
        print("  No GIFs found (check VHS output paths)")


if __name__ == "__main__":
    main()
