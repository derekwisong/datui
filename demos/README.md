# Demo GIFs

This directory contains animated GIF demos showcasing datui features.

## How to Generate Demos

Demos are generated using VHS (Video for General). See `scripts/demos/` for `.tape` script files.

### Prerequisites

VHS must be installed and available in your PATH. Verify installation:
```bash
vhs --version
```

### Generating Demos

#### Option 1: Generate a Single Demo

From the repository root:
```bash
vhs scripts/demos/01-basic-navigation.tape
```

This will generate `demos/01-basic-navigation.gif` in the root `demos/` directory.

#### Option 2: Generate All Demos

From the repository root:
```bash
./scripts/demos/generate-all.sh
```

Or if you're in `scripts/demos/`:
```bash
cd scripts/demos
./generate-all.sh
```

This will generate all GIFs from all `.tape` files in `scripts/demos/`.

### Testing a Demo

1. **Edit the `.tape` file** (if needed):
   ```bash
   vim scripts/demos/01-basic-navigation.tape
   ```

2. **Generate the GIF**:
   ```bash
   vhs scripts/demos/01-basic-navigation.tape
   ```

3. **View the result**:
   ```bash
   # Open in browser (Linux)
   xdg-open demos/01-basic-navigation.gif
   
   # Open in default viewer (macOS)
   open demos/01-basic-navigation.gif
   ```

### Troubleshooting

**"vhs: command not found"**
- Ensure VHS is installed and available in your PATH

**GIF not generated or wrong location**
- Make sure you run `vhs` from the repository root
- Check that the `Output` path in the `.tape` file is correct

**TUI not appearing in GIF**
- VHS needs a TTY to run the TUI. Make sure you're running in a terminal (not SSH without TTY)
- Check that datui builds correctly: `cargo build --release`

## Demo Files

- `01-basic-navigation.gif` - Basic navigation and scrolling
- `02-querying.gif` - Querying data with select and where clauses

## Usage in Documentation

### In README.md:
```markdown
![Query Demo](demos/02-querying.gif)
```

### In mdbook docs (docs/*.md):
```markdown
![Query Demo](../demos/02-querying.gif)
```
