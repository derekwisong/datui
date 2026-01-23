# Adding Configuration Options

When adding new configuration options to datui, follow this process:

## Process Overview

Adding a new configuration option requires updates in 7 places:

1. Config struct definition
2. Default implementation
3. Merge logic
4. Comment constants (for generated configs)
5. Application code usage
6. Tests
7. Documentation

## Step-by-Step Guide

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

### 4. Add Comments to Comment Constants

Add comments to the comment constant array right after the struct definition in `src/config.rs`:

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

// Field comments for DisplayConfig
const DISPLAY_COMMENTS: &[(&str, &str)] = &[
    // ... existing fields ...
    (
        "font_size",
        "Font size for terminal display (optional)\nSet to null to use terminal default, or 8-16 for explicit size",
    ),
];
```

**Note**: Comments are defined next to the struct definition. The config template is generated from Rust code defaults, with all fields commented out so users can uncomment to override.

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
- Add to `docs/user-guide/configuration.md`
- Mention in README.md if it's a major feature

**Note:** Configuration comments are defined in comment constants next to struct definitions (e.g., `DISPLAY_COMMENTS`, `PERFORMANCE_COMMENTS`) in `src/config.rs`. The config template is generated programmatically from these constants.

## Implementation Checklist

- [ ] Field added to config struct
- [ ] Default implementation updated
- [ ] Merge logic implemented
- [ ] Comments added to comment constant (next to struct)
- [ ] Used in application code
- [ ] Tests added
- [ ] Documentation updated
- [ ] All tests passing (`cargo test`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Code formatted (`cargo fmt`)

## Best Practices

### Choosing Field Types

- **Option fields**: Use `Option<T>` for optional settings
  ```rust
  pub font_size: Option<u8>,  // None = use default
  ```

- **Required fields**: Use plain types with sensible defaults
  ```rust
  pub pages_lookahead: usize,  // Always has a value
  ```

- **Strings**: Use `String` for text values
  ```rust
  pub delimiter: String,  // CSV delimiter character
  ```

### Sensible Defaults

Ensure defaults match existing behavior:

```rust
impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            pages_lookahead: 3,
            pages_lookback: 3,
            row_numbers: false,
            row_start_index: 1,
        }
    }
}
```

### Clear Config Comments

Comments in the comment constants should:
- Explain what the option does
- Show valid values or ranges
- Provide examples
- Note any interactions with other settings

**Good example:**
```rust
const PERFORMANCE_COMMENTS: &[(&str, &str)] = &[
    (
        "sampling_threshold",
        "Sampling threshold: datasets >= this size will be sampled for statistics\nSet to higher value to avoid sampling, or lower to sample more aggressively",
    ),
];
```

**Poor example:**
```rust
const PERFORMANCE_COMMENTS: &[(&str, &str)] = &[
    ("sampling_threshold", "Sampling threshold"),
];
```

### Validation

Add validation in `AppConfig::validate()` for constraints:

```rust
fn validate(&self) -> Result<()> {
    // ... existing validation ...
    
    // Validate new field
    if self.performance.sampling_threshold == 0 {
        return Err(eyre!("sampling_threshold must be greater than 0"));
    }
    
    Ok(())
}
```

### Testing Edge Cases

Test important scenarios:
- Missing values (uses default)
- Invalid ranges (validation catches)
- Boundary conditions
- Config merging (CLI overrides config)
- TOML parsing (valid syntax)

## Adding Colors to Theme

When adding new colors to the theme system, follow these additional steps:

### 1. Add to ColorConfig Struct

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ColorConfig {
    // ... existing colors ...
    pub new_color: String,  // NEW
}
```

### 2. Add to ColorConfig Default

```rust
impl Default for ColorConfig {
    fn default() -> Self {
        Self {
            // ... existing colors ...
            new_color: "cyan".to_string(),  // NEW
        }
    }
}
```

### 3. Add to Validation

```rust
impl ColorConfig {
    fn validate(&self, parser: &ColorParser) -> Result<()> {
        macro_rules! validate_color {
            ($field:expr, $name:expr) => {
                parser
                    .parse($field)
                    .map_err(|e| eyre!("Invalid color value for '{}': {}", $name, e))?;
            };
        }
        
        // ... existing validations ...
        validate_color!(&self.new_color, "new_color");  // NEW
        
        Ok(())
    }
}
```

### 4. Add to Merge Logic

```rust
impl ColorConfig {
    pub fn merge(&mut self, other: Self) {
        let default = ColorConfig::default();
        // ... existing merge logic ...
        
        if other.new_color != default.new_color {  // NEW
            self.new_color = other.new_color;
        }
    }
}
```

### 5. Add to Theme Parsing

```rust
impl Theme {
    pub fn from_config(config: &ThemeConfig) -> Result<Self> {
        let parser = ColorParser::new();
        let mut colors = HashMap::new();
        
        // ... existing color parsing ...
        colors.insert(
            "new_color".to_string(),
            parser.parse(&config.colors.new_color)?,
        );  // NEW
        
        Ok(Self { colors })
    }
}
```

### 6. Add Comments to Comment Constant

```rust
// Field comments for ColorConfig
const COLOR_COMMENTS: &[(&str, &str)] = &[
    // ... existing colors ...
    (
        "new_color",
        "Description of the new color and where it's used",
    ),
];
```

**Note**: Comments are simple text - they'll be prefixed with `#` when generating the config. The field itself will appear as `# new_color = "cyan"` (commented out).

### 7. Replace Hardcoded Usage

Find and replace hardcoded colors in widgets:

**Before:**
```rust
Style::default().fg(Color::Cyan)
```

**After:**
```rust
Style::default().fg(self.color("new_color"))
// or
Style::default().fg(theme.get("new_color"))
```

### Color Naming Conventions

- Use descriptive names: `sidebar_border` not `sb`
- Be specific: `modal_border_active` vs `sidebar_border` (modals vs sidebars)
- Group logically: `distribution_normal`, `distribution_skewed`, `distribution_other`
- Consider purpose: `text_primary`, `text_secondary`, `text_inverse`

## Common Patterns

### Option Field Pattern

```rust
// Config struct
pub struct Config {
    pub optional_field: Option<T>,
}

// Default
impl Default for Config {
    fn default() -> Self {
        Self {
            optional_field: None,  // No default value
        }
    }
}

// Merge
impl Config {
    pub fn merge(&mut self, other: Self) {
        if other.optional_field.is_some() {
            self.optional_field = other.optional_field;
        }
    }
}

// Usage
let value = config.optional_field.unwrap_or(fallback);
```

### Required Field Pattern

```rust
// Config struct
pub struct Config {
    pub required_field: usize,
}

// Default
impl Default for Config {
    fn default() -> Self {
        Self {
            required_field: 10,  // Sensible default
        }
    }
}

// Merge
impl Config {
    pub fn merge(&mut self, other: Self) {
        let default = Config::default();
        if other.required_field != default.required_field {
            self.required_field = other.required_field;
        }
    }
}

// Usage
let value = config.required_field;
```

### String Field Pattern

```rust
// Config struct
pub struct Config {
    pub mode: String,
}

// Default
impl Default for Config {
    fn default() -> Self {
        Self {
            mode: "auto".to_string(),
        }
    }
}

// Merge
impl Config {
    pub fn merge(&mut self, other: Self) {
        let default = Config::default();
        if other.mode != default.mode {
            self.mode = other.mode;
        }
    }
}

// Validation
fn validate(&self) -> Result<()> {
    match self.mode.as_str() {
        "option1" | "option2" | "option3" => Ok(()),
        _ => Err(eyre!("Invalid mode: {}. Must be one of: option1, option2, option3", self.mode))
    }
}
```

## Resources

- See `src/config.rs` for existing implementations and comment constants (e.g., `PERFORMANCE_COMMENTS`, `DISPLAY_COMMENTS`)
- See `tests/config_test.rs` for test examples
- Run `datui --generate-config` to see the generated config template (all fields commented out)

## Questions?

If you're unsure about:
- **Which config section to use**: Look at similar settings in existing config
- **Merge logic**: Follow the patterns in existing merge implementations
- **Validation**: Add validation if there are constraints on the value
- **Testing**: Look at existing tests for similar config types
