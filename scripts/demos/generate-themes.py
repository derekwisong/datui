#!/usr/bin/env python3
"""
Generate VHS demo GIFs for all available themes.

This script takes a tape file and generates a GIF for each VHS theme,
saving them to the specified output directory with theme names as filenames.
Runs parallel workers (by default one per CPU core) and reports progress.

Usage:
    python generate-themes.py <tape_file> <output_dir>
    python generate-themes.py 01-basic-navigation.tape /tmp/theme-demos -n 4

Example:
    python generate-themes.py 01-basic-navigation.tape /tmp/theme-demos
"""

import argparse
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import NamedTuple
import re


# List of VHS themes (from https://github.com/charmbracelet/vhs/blob/main/THEMES.md)
THEMES = [
    "3024 Day", "3024 Night", "Aardvark Blue", "Abernathy", "Adventure",
    "AdventureTime", "Afterglow", "Alabaster", "AlienBlood", "Andromeda",
    "Apple Classic", "arcoiris", "Argonaut", "Arthur", "AtelierSulphurpool",
    "Atom", "AtomOneLight", "Aurora", "ayu", "Ayu Mirage", "ayu_light",
    "Banana Blueberry", "Batman", "Belafonte Day", "Belafonte Night",
    "BirdsOfParadise", "Blazer", "Blue Matrix", "BlueBerryPie", "BlueDolphin",
    "BlulocoDark", "BlulocoLight", "Borland", "Breeze", "Bright Lights",
    "Broadcast", "Brogrammer", "Bubbles", "Builtin Dark", "Builtin Light",
    "Builtin Pastel Dark", "Builtin Solarized Dark", "Builtin Solarized Light",
    "Builtin Tango Dark", "Builtin Tango Light", "C64", "Calamity",
    "Catppuccin Frappe", "Catppuccin Latte", "Catppuccin Macchiato",
    "Catppuccin Mocha", "catppuccin-frappe", "catppuccin-latte",
    "catppuccin-macchiato", "catppuccin-mocha", "CGA", "Chalk", "Chalkboard",
    "ChallengerDeep", "Chester", "Ciapre", "CLRS", "Cobalt Neon", "Cobalt2",
    "coffee_theme", "Contrast Light", "coolnight", "CrayonPonyFish",
    "Crystal Violet", "Cyber Cube", "Cyberdyne", "cyberpunk", "CyberPunk2077",
    "Dark Pastel", "Dark+", "darkermatrix", "darkmatrix", "Darkside", "deep",
    "Desert", "DimmedMonokai", "Django", "DjangoRebornAgain", "DjangoSmooth",
    "Doom Peacock", "DoomOne", "DotGov", "Dracula", "Dracula+", "DraculaPlus",
    "duckbones", "Duotone Dark", "Earthsong", "Elemental", "Elementary", "ENCOM",
    "Espresso", "Espresso Libre", "Everblush", "Fahrenheit", "Fairyfloss",
    "Fideloper", "FirefoxDev", "Firewatch", "FishTank", "Flat", "Flatland",
    "Floraverse", "ForestBlue", "Framer", "FrontEndDelight", "FunForrest",
    "Galaxy", "Galizur", "Ganyu", "Github", "GitHub Dark", "Glacier", "Glorious",
    "Grape", "Grass", "Grey-green", "Gruvbox Light", "GruvboxDark",
    "GruvboxDarkHard", "Guezwhoz", "h4rithd", "h4rithd.com", "Hacktober",
    "Hardcore", "Harper", "HaX0R_BLUE", "HaX0R_GR33N", "HaX0R_R3D", "Highway",
    "Hipster Green", "Hivacruz", "Homebrew", "Hopscotch", "Hopscotch.256",
    "Horizon", "Hurtado", "Hybrid", "Hyper", "IC_Green_PPL", "IC_Orange_PPL",
    "iceberg-dark", "iceberg-light", "idea", "idleToes", "IR_Black",
    "iTerm2 Dark Background", "iTerm2 Default", "iTerm2 Light Background",
    "iTerm2 Pastel Dark Background", "iTerm2 Smoooooth", "iTerm2 Solarized Dark",
    "iTerm2 Solarized Light", "iTerm2 Tango Dark", "iTerm2 Tango Light",
    "Jackie Brown", "Japanesque", "Jellybeans", "JetBrains Darcula", "jubi",
    "Juicy Colors", "Kanagawa", "kanagawabones", "Kibble", "Kolorit", "Konsolas",
    "Lab Fox", "Laser", "Later This Evening", "Lavandula", "LiquidCarbon",
    "LiquidCarbonTransparent", "LiquidCarbonTransparentInverse", "lovelace",
    "Man Page", "Mariana", "Material", "MaterialDark", "MaterialDarker",
    "MaterialDesignColors", "MaterialOcean", "Mathias", "matrix", "Medallion",
    "midnight-in-mojave", "Mirage", "Misterioso", "Molokai", "MonaLisa",
    "Monokai Cmder", "Monokai Pro", "Monokai Pro (Filter Octagon)",
    "Monokai Pro (Filter Ristretto)", "Monokai Remastered", "Monokai Soda",
    "Monokai Vivid", "Moonlight II", "N0tch2k", "neobones_dark",
    "neobones_light", "Neon", "Neopolitan", "Neutron", "Night Owlish Light",
    "NightLion v1", "NightLion v2", "niji", "Nocturnal Winter", "nord",
    "nord-light", "Novel", "Obsidian", "Ocean", "Oceanic-Next", "OceanicMaterial",
    "Ollie", "OneDark", "OneHalfDark", "OneHalfLight", "OneStar",
    "Operator Mono Dark", "Overnight Slumber", "PaleNightHC", "Pandora",
    "Paraiso Dark", "PaulMillr", "PencilDark", "PencilLight", "Peppermint",
    "Piatto Light", "Pnevma", "Popping and Locking", "primary", "Primer", "Pro",
    "Pro Light", "Purple Rain", "purplepeter", "QB64 Super Dark Blue", "Rapture",
    "Raycast_Dark", "Raycast_Light", "rebecca", "Red Alert", "Red Planet",
    "Red Sands", "Relaxed", "Retro", "Retrowave", "Rippedcasts", "Rose Pine",
    "rose-pine", "rose-pine-dawn", "rose-pine-moon", "Rouge 2", "Royal",
    "Ryuuko", "Sakura", "Scarlet Protocol", "Seafoam Pastel", "SeaShells",
    "seoulbones_dark", "seoulbones_light", "Serendipity Midnight",
    "Serendipity Morning", "Serendipity Sunset", "Seti", "shades-of-purple",
    "Shaman", "Slate", "SleepyHollow", "Smyck", "Snazzy", "SoftServer",
    "Solarized Darcula", "Solarized Dark - Patched",
    "Solarized Dark Higher Contrast", "Sonoran Gothic", "Sonoran Sunrise",
    "Spacedust", "SpaceGray", "SpaceGray Eighties", "SpaceGray Eighties Dull",
    "Spiderman", "Spring", "Square", "Sublette", "Subliminal", "Sundried",
    "Symfonic", "synthwave", "synthwave-everything", "SynthwaveAlpha",
    "Tango Adapted", "Tango Half Adapted", "Teerb", "Terminal Basic",
    "Thayer Bright", "The Hulk", "Tinacious Design (Dark)",
    "Tinacious Design (Light)", "TokyoNight", "tokyonight", "tokyonight-day",
    "tokyonight-storm", "TokyoNightLight", "TokyoNightStorm", "Tomorrow",
    "Tomorrow Night", "Tomorrow Night Blue", "Tomorrow Night Bright",
    "Tomorrow Night Burns", "Tomorrow Night Eighties", "ToyChest", "Treehouse",
    "Twilight", "Ubuntu", "UltraDark", "UltraViolent", "UnderTheSea", "Unholy",
    "Unikitty", "Urple", "Vaughn", "VibrantInk", "vimbones", "Violet Dark",
    "Violet Light", "WarmNeon", "Wez", "Whimsy", "WildCherry", "wilmersdorf",
    "Wombat", "Wryan", "zenbones", "zenbones_dark", "zenbones_light",
    "Zenburn", "zenburned", "zenwritten_dark", "zenwritten_light", "Zeonica",
]


def sanitize_filename(name: str) -> str:
    """Convert theme name to a safe filename."""
    # Replace spaces and special chars with underscores
    name = re.sub(r'[^\w\-_\.]', '_', name)
    # Remove multiple consecutive underscores
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    return name


def read_tape_file(tape_path: Path) -> str:
    """Read the tape file content."""
    with open(tape_path, 'r', encoding='utf-8') as f:
        return f.read()


def create_themed_tape(tape_content: str, theme: str, output_path: Path) -> str:
    """Create a modified tape file with the theme set and custom output path."""
    lines = tape_content.split('\n')
    modified_lines = []
    
    # Track if we've added the theme and modified the output
    theme_added = False
    output_modified = False
    
    # Quote theme name if it contains spaces or special characters that need quoting
    # VHS requires quoting for: spaces, +, -, and other special shell characters
    needs_quoting = any(char in theme for char in [' ', '+', '-', '(', ')', '[', ']'])
    quoted_theme = f'"{theme}"' if needs_quoting else theme
    
    for line in lines:
        # Modify the Output line to use our custom path
        if line.startswith('Output ') and not output_modified:
            modified_lines.append(f'Output {output_path}')
            output_modified = True
        # Insert theme after Output line (or at start if no Output found yet)
        elif not theme_added:
            modified_lines.append(line)
            # Add theme after Output line, or after first line if no Output
            if output_modified or (line.strip() and not line.startswith('Set ')):
                modified_lines.append(f'Set Theme {quoted_theme}')
                theme_added = True
        else:
            modified_lines.append(line)
    
    # If we haven't added the theme yet, add it at the beginning
    if not theme_added:
        modified_lines.insert(0, f'Set Theme {quoted_theme}')
        # Ensure Output is set if it wasn't found
        if not output_modified:
            # Insert Output after theme
            modified_lines.insert(1, f'Output {output_path}')
    
    return '\n'.join(modified_lines)


def _generate_one_theme_impl(
    tape_path: Path,
    theme: str,
    output_dir: Path,
    repo_root: Path,
    env: dict | None = None,
) -> tuple[bool, float | None, str | None]:
    """Generate a GIF for a specific theme. No printing (for use in workers).

    Returns:
        (success, size_kb_or_none, error_message_or_none)
    """
    theme_filename = sanitize_filename(theme)
    output_gif = output_dir / f"{theme_filename}.gif"

    tape_content = read_tape_file(tape_path)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.tape', delete=False) as tmp_tape:
        tmp_tape_path = Path(tmp_tape.name)
        themed_content = create_themed_tape(tape_content, theme, output_gif)
        tmp_tape.write(themed_content)
        tmp_tape.flush()

    try:
        result = subprocess.run(
            ['vhs', str(tmp_tape_path)],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        tmp_tape_path.unlink(missing_ok=True)

        if result.returncode == 0:
            if output_gif.exists():
                return (True, output_gif.stat().st_size / 1024.0, None)
            return (False, None, "GIF not created")
        err = (result.stderr or result.stdout or "vhs error")[:200]
        return (False, None, err.strip() or "vhs error")
    except subprocess.TimeoutExpired:
        tmp_tape_path.unlink(missing_ok=True)
        return (False, None, "timeout")
    except Exception as e:
        tmp_tape_path.unlink(missing_ok=True)
        return (False, None, str(e))


class _ThemeArgs(NamedTuple):
    tape_path: str
    theme: str
    output_dir: str
    repo_root: str
    env: dict


def _run_one_theme(args: _ThemeArgs) -> tuple[str, bool, float | str]:
    """Worker entry point for process pool. Returns (theme, success, size_kb or error_str)."""
    tape_path = Path(args.tape_path)
    output_dir = Path(args.output_dir)
    repo_root = Path(args.repo_root)
    success, size_kb, err = _generate_one_theme_impl(
        tape_path, args.theme, output_dir, repo_root, args.env
    )
    if success:
        return (args.theme, True, size_kb or 0.0)
    return (args.theme, False, err or "unknown error")


def _progress_bar(current: int, total: int, width: int = 30) -> str:
    """Return a simple percentage progress bar string."""
    if total <= 0:
        pct = 100
    else:
        pct = min(100, int(100 * current / total))
    filled = int(width * current / total) if total else width
    bar = "=" * filled + "-" * (width - filled)
    return f"\r  [{bar}] {pct}% ({current}/{total})"


def main():
    parser = argparse.ArgumentParser(
        description='Generate VHS demo GIFs for all available themes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate-themes.py 01-basic-navigation.tape /tmp/theme-demos
  python generate-themes.py demos/02-querying.tape ./theme-output -n 4
  python generate-themes.py 01-basic-navigation.tape /tmp/theme-demos --release
        """
    )
    parser.add_argument(
        'tape_file',
        type=Path,
        help='Path to the VHS tape file'
    )
    parser.add_argument(
        'output_dir',
        type=Path,
        help='Directory to save generated GIFs'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force regeneration even if GIF already exists (default: skip existing files)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of themes to process (for testing)'
    )
    parser.add_argument(
        '--workers',
        '-n',
        type=int,
        default=os.cpu_count() or 1,
        metavar='N',
        help='Number of parallel workers (default: all available cores).'
    )
    parser.add_argument(
        '--release',
        action='store_true',
        help='Use release build of datui (cargo build --release).'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.tape_file.exists():
        print(f"Error: Tape file not found: {args.tape_file}", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find repo root (assume script is in scripts/demos/)
    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent.parent
    
    if not (repo_root / "Cargo.toml").exists():
        print("Error: Could not find repository root. Please run from repository root.", file=sys.stderr)
        sys.exit(1)
    
    # Build binary (debug or release)
    build_type = "release" if args.release else "debug"
    print(f"Building {build_type} binary...")
    cargo_args = ["cargo", "build", "--bin", "datui"]
    if args.release:
        cargo_args.append("--release")
    try:
        subprocess.run(
            cargo_args,
            cwd=repo_root,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to build binary: {e}", file=sys.stderr)
        sys.exit(1)
    
    binary_path = repo_root / "target" / build_type / "datui"
    if not binary_path.exists():
        print("Error: datui binary not found. Please build it first.", file=sys.stderr)
        sys.exit(1)
    
    binary_dir = str(binary_path.parent)
    env = os.environ.copy()
    env["PATH"] = f"{binary_dir}:{env.get('PATH', '')}"
    if "PS1" in env:
        del env["PS1"]
    
    # Check if vhs is installed
    try:
        subprocess.run(["vhs", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: vhs not found. Please install VHS and ensure it's in your PATH.", file=sys.stderr)
        sys.exit(1)
    
    themes_to_process = THEMES
    if args.limit:
        themes_to_process = themes_to_process[:args.limit]
    
    skip_existing = not args.force
    work_list = []
    skipped = 0
    for theme in themes_to_process:
        theme_filename = sanitize_filename(theme)
        output_gif = args.output_dir / f"{theme_filename}.gif"
        if skip_existing and output_gif.exists():
            skipped += 1
        else:
            work_list.append(_ThemeArgs(
                tape_path=str(args.tape_file),
                theme=theme,
                output_dir=str(args.output_dir),
                repo_root=str(repo_root),
                env=env,
            ))
    
    total = len(work_list)
    workers = min(args.workers, total) if total else 1
    
    print(f"Generating GIFs for {len(themes_to_process)} themes ({total} to run, {skipped} skipped).")
    print(f"Tape file: {args.tape_file}")
    print(f"Output directory: {args.output_dir}")
    print(f"Workers: {workers}")
    print()
    
    if total == 0:
        print("Nothing to do (all outputs already exist; use --force to regenerate).")
        return
    
    successful = 0
    failed_list = []
    completed = 0
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_run_one_theme, a): a for a in work_list}
        for future in as_completed(futures):
            theme, success, size_or_err = future.result()
            completed += 1
            sys.stdout.write(_progress_bar(completed, total))
            sys.stdout.flush()
            if success:
                successful += 1
            else:
                failed_list.append((theme, size_or_err))
    
    print()
    print(f"Complete: {successful} successful, {len(failed_list)} failed, {skipped} skipped")
    print(f"GIFs saved to: {args.output_dir}")
    
    if failed_list:
        print("\nFailed themes:", file=sys.stderr)
        for theme, err in failed_list:
            print(f"  - {theme}: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
