# How to Generate Demo GIFs with VHS

## Quick Start

### 1. Build datui (if not already built)

```bash
cargo build --release
```

### 2. Generate a Demo

**From the repository root:**

```bash
# Generate a specific demo
vhs scripts/demos/01-basic-navigation.tape

# Or generate all demos at once
./scripts/demos/generate-all.sh
```

### 3. View the Result

```bash
# The GIF will be created in demos/
ls demos/*.gif

# Open in your default viewer
open demos/01-basic-navigation.gif      # macOS
xdg-open demos/01-basic-navigation.gif  # Linux
```

## Detailed Instructions

### File Locations

- **VHS scripts**: `scripts/demos/*.tape`
- **Generated GIFs**: `demos/*.gif` (at repository root)
- **Sample data**: `tests/sample-data/people.parquet` (used by scripts)

### Running VHS

**Important**: Always run `vhs` commands from the **repository root** directory, not from `scripts/demos/`.

This is because:
1. The `.tape` files reference output paths like `demos/01-basic-navigation.gif` (relative to root)
2. The datui binary path in `.tape` files is relative to root: `datui tests/sample-data/...`

**Correct way**:
```bash
cd /path/to/datui              # Repository root
vhs scripts/demos/01-basic-navigation.tape
```

**Incorrect way**:
```bash
cd /path/to/datui/scripts/demos
vhs 01-basic-navigation.tape   # ❌ Output path will be wrong
```

### Understanding .tape Files

VHS `.tape` files are simple scripts that describe what to type and when:

```tape
Output demos/01-basic-navigation.gif    # Output location
Set Width 1000                          # Terminal width
Set Height 600                          # Terminal height
Set Theme "catppuccin-frappe"          # Color theme

Type "datui tests/sample-data/people.parquet"  # Type command
Enter                                       # Press Enter
Sleep 3s                                    # Wait 3 seconds
Type "j"                                    # Press 'j' key
Sleep 0.5s                                  # Wait 0.5 seconds
Type "q"                                    # Press 'q' to quit
```

### Common Operations

**Generate one demo**:
```bash
vhs scripts/demos/01-basic-navigation.tape
```

**Generate all demos**:
```bash
./scripts/demos/generate-all.sh
```

**Edit a demo script**:
```bash
vim scripts/demos/01-basic-navigation.tape
# Make changes
vhs scripts/demos/01-basic-navigation.tape  # Regenerate
```

**Check what will happen** (dry run - not available in VHS, but you can view the .tape file):
```bash
cat scripts/demos/01-basic-navigation.tape
```

## Troubleshooting

### VHS Not Found

```bash
# Check if installed
which vhs

# If not found, install VHS and ensure it's in your PATH
```

### Output GIF is Empty or Wrong

- Check that you're running from repository root
- Verify datui binary exists: `ls target/release/datui`
- Check the `Output` path in the `.tape` file matches `demos/` structure

### TUI Not Appearing

- VHS needs a proper TTY to run TUIs
- Make sure you're in a real terminal (not some special environment)
- Try running `datui tests/sample-data/people.parquet` manually first to verify it works

### Timing Issues

If the demo seems rushed or slow:
- Edit the `.tape` file
- Adjust `Sleep` durations (e.g., `Sleep 2s` → `Sleep 3s`)
- Regenerate: `vhs scripts/demos/01-basic-navigation.tape`

## Example: Creating a New Demo

1. **Create a new `.tape` file**:
   ```bash
   vim scripts/demos/03-filtering.tape
   ```

2. **Write the script** (see existing `.tape` files as examples)

3. **Test it**:
   ```bash
   vhs scripts/demos/03-filtering.tape
   ```

4. **View the result**:
   ```bash
   open demos/03-filtering.gif
   ```

5. **Iterate**: Adjust timing, add more steps, etc.

## Tips

- **Keep demos short**: 15-30 seconds is ideal for GIFs
- **Clear actions**: Make sure each step is visible and clear
- **Consistent themes**: Use the same `Set Theme` across all demos
- **Test paths**: Make sure `Output` paths create files in `demos/`
