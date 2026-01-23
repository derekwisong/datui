# Contributing

Thank you for your interest in contributing to Datui!

Once you've gotten [the repo](https://www.github.com/derekwisong/datui) cloned
for the first time, follow the [Setup](#setup) instructions below to get started.

## Setup

### Setup Script

**TLDR:** The entire setup process can be automated by running
```
python scripts/setup-dev.py
```

The script will:

- Set up the [Python Virtual Enviroment](#python-virtual-environment)
  - Updates it if it already exists
- Install/update [pre-commit hooks](#pre-commit-hooks)
- Generates sample data needed to run the tests
- Configure and build the [documentation](documentation.md)

### Python Virtual Environment

There are Python scripts in the `/scripts` directory that are
used to do things like build test data, documentation, and demo gifs.

Setting up a virtual environment with dependencies for these scripts will
ensure you can run them all.

A common convention is to create a virtual environment in the `.venv/` directory
of the repository. In fact, the `.gitignore` is already set up to ignore this location
so that files there aren't added by mistake.

```bash
python -m venv .venv
```

Then activate the virtual environment.

```bash
source .venv/bin/activate
```

Once activated, install dependencies used to run the availble Python scripts.

```bash
pip install -r scripts/requirements.txt
```

You're now ready to run the [tests](tests.md).


### Pre-commit Hooks

To encourage consistency and quality, the CI build checks the source code of the
application for formatting and linter warnings.

This project uses [pre-commit](https://pre-commit.com/) to manage git pre-commit hooks 
which automatically run the same code quality checks in your repository before commits
are made.

#### Installing Pre-commit and Hooks

> If you used the [Setup Script](#setup-script), the pre-commit hooks are already
> installed.

1. **Install pre-commit**:

   If you set up a Python virtual environment using the instructions above then you
   already have everything you need. **Activate it and skip this step.**

   Otherwise, install `pre-commit` using your desired method.

   ```bash
   # Using pip
   pip install pre-commit
   
   # Or using homebrew (macOS)
   brew install pre-commit
   
   # Or using conda
   conda install -c conda-forge pre-commit
   ```

2. **Install the git hooks**:
   ```bash
   pre-commit install
   ```

   This installs the hooks into `.git/hooks/` so they run automatically on commit.

   **Note:** You only need the `pre-commit` command accessible when you need
   to use it to manually run or update the hooks. Once installed into your repo, 
   the hooks themselves do not require `pre-commit`.

   See the `pre-commit` documentation for more information about its features.

The following hooks are configured:

- **cargo-fmt**: Automatically formats Rust code with `cargo fmt`
  - If code needs formatting, it will be formatted and the commit will fail
  - Stage the formatted changes and commit again

- **cargo-clippy**: Runs `cargo clippy --all-targets -- -D warnings`
  - Fails if clippy finds any warnings
  - Fix them and commit again

Hooks run automatically when you `git commit`. If any hook fails, the commit is aborted.

#### Running Hooks

Run all hooks manually:
```bash
pre-commit run --all-files
```

Run a specific hook:
```bash
pre-commit run cargo-fmt --all-files
pre-commit run cargo-clippy --all-files
```

#### Skipping Hooks

If you need to skip hooks for a specific commit (not recommended):
```bash
git commit --no-verify -m "Emergency fix"
```

#### Updating hooks

Update hook versions and configurations:
```bash
pre-commit autoupdate
```

#### Troubleshooting

**Hook not running?**
- Make sure you ran `pre-commit install`
- Check `.git/hooks/pre-commit` exists

**Hooks too slow?**
- Only changed files are checked by default
- Use `SKIP=hook-name git commit` to skip specific hooks

## Adding Configuration Options

When adding new configuration options to datui, follow this process:

### 1. Add Field to Config Struct

Add the new field to the appropriate config struct in `src/config.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DisplayConfig {
    pub pages_lookahead: usize,
    pub pages_lookback: usize,
    pub row_numbers: bool,
    pub row_start_index: usize,
    pub font_size: Option<u8>,  // NEW FIELD
}
```

### 2. Update Default Implementation

Add the default value in the `Default` trait:

```rust
impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            pages_lookahead: 3,
            pages_lookback: 3,
            row_numbers: false,
            row_start_index: 1,
            font_size: None,  // NEW: None = use terminal default
        }
    }
}
```

### 3. Update Merge Logic

Add merge handling in the section's `merge()` method:

```rust
impl DisplayConfig {
    pub fn merge(&mut self, other: Self) {
        let default = DisplayConfig::default();
        // ... existing merge logic ...
        
        // NEW: Merge font_size (Option fields)
        if other.font_size.is_some() {
            self.font_size = other.font_size;
        }
    }
}
```

**Merge rules:**
- **Option fields**: If `other.field.is_some()`, take the value
- **Non-Option fields**: If `other.field != default.field`, take the value

### 4. Update Config Template

Add to `config/default.toml` with helpful comments:

```toml
[display]
# ... existing fields ...

# Font size for terminal display (optional)
# Set to null to use terminal default, or 8-16 for explicit size
# font_size = null
```

### 5. Use in Application Code

Access the config value where needed:

```rust
let font_size = config.display.font_size.unwrap_or(terminal_default);
```

Or pass through App if needed globally:
```rust
app.font_size = config.display.font_size;
```

### 6. Add Tests

Add tests in `tests/config_test.rs` or `tests/config_integration_test.rs`:

```rust
#[test]
fn test_font_size_config() {
    let mut config = AppConfig::default();
    config.display.font_size = Some(12);
    
    assert_eq!(config.display.font_size, Some(12));
    assert!(config.validate().is_ok());
}
```

### 7. Update Documentation

Update documentation:
- Add to the [Configuration](../user-guide/configuration.md) page
- Add to embedded template comments in `config/default.toml`

### Checklist

- [ ] Field added to config struct
- [ ] Default implementation updated
- [ ] Merge logic implemented
- [ ] Template file updated with comments
- [ ] Used in application code
- [ ] Tests added
- [ ] Documentation updated
- [ ] All tests passing (`cargo test`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Code formatted (`cargo fmt`)

### Tips

- **Keep It Simple**: Use `Option<T>` for optional settings, plain types for required
- **Sensible Defaults**: Ensure defaults match current behavior (backward compatible)
- **Clear Comments**: Template comments should explain the option and show examples
- **Validate**: Add validation in `AppConfig::validate()` if there are constraints
- **Test Edge Cases**: Test with missing values, invalid ranges, boundary conditions

### Color Configuration

When adding new colors to the theme:

1. Add field to `ColorConfig` struct
2. Add to `ColorConfig::default()` with current hardcoded color
3. Add to `ColorConfig::validate()` macro
4. Add to `ColorConfig::merge()` with default comparison
5. Add to `Theme::from_config()` to parse the color
6. Update template with example
7. Replace hardcoded `Color::` usage with `self.color("name")` or `theme.get("name")`
