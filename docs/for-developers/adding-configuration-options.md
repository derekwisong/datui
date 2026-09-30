# Add configuration options

Start in `crates/datui-lib/src/config.rs`. Use an existing option in the same
section as a model, and update all seven parts:

| Part | Change |
|---|---|
| Struct | Add the field to the appropriate config section |
| Default | Set its value in that section's `Default` implementation |
| Merge | Handle it in the section's `merge()` method |
| Generated comments | Add its description to the section's `*_COMMENTS` array |
| Usage | Read the merged setting where the behavior is implemented |
| Tests | Cover deserialization, defaults, merging and the affected behavior |
| Documentation | Add it to the [settings reference](../reference/settings.md) and relevant guide |

## Example: `notes_accent`

This existing display option controls whether unread dataset notes accent the
Info key. The relevant pieces in `config.rs` are:

```rust
// Field in DisplayConfig:
pub notes_accent: bool,

// Value in DisplayConfig::default():
notes_accent: true,

// Entry in DISPLAY_COMMENTS:
("notes_accent", "Accent the i key when dataset notes are unread"),

// Inside DisplayConfig::merge(), where default is DisplayConfig::default():
if other.notes_accent != default.notes_accent {
    self.notes_accent = other.notes_accent;
}
```

These are excerpts from separate locations, not one Rust block to paste.
The control-bar construction in `lib.rs` reads the setting in
`with_notes_pending`. Follow that path when
checking whether a new setting reaches its intended behavior.

The generated config shows ordinary fields as commented examples. The public
dataset catalog is active TOML; preserve that distinction.

## Merge and validation rules

| Field | Existing merge rule |
|---|---|
| `Option<T>` | Replace when the incoming value is `Some` |
| Plain scalar | Replace when the incoming value differs from its default |
| Color | Compare with the built-in default, then validate with `ColorParser` |

A plain field set to its default cannot reliably override a non-default import.
Do not assume the merge records whether a user explicitly wrote a value.

Add range or format checks to the appropriate validation method when needed.
Test an omitted field, an explicit value, merging over an existing value, and
invalid/boundary values where relevant. A test that only assigns a field and
reads it back does not check configuration loading.

## Add a CLI override

If the setting also needs a flag:

1. Add it to `Args` in `crates/datui-cli/src/lib.rs`.
2. Apply it after config loading; cloud overrides use `effective_cloud`.
3. Test precedence and regenerate the CLI reference:

```bash
.venv/bin/python scripts/docs/generate_command_line_options.py -o docs/reference/command-line-options.md
```

Check whether the Python options in `crates/datui-pyo3` should expose it too.

## Add a color

In addition to the checklist, update `ColorConfig::validate`,
`ColorConfig::merge`, `Theme::from_config`, and the theme's default values.
Test both light and dark modes. Use the theme field in rendering code;
do not put a hardcoded color in a widget.

Add the slot and its defaults to the [color reference](../reference/settings.md#colors).
Use a name that describes its purpose, such as `modal_border_active`.

## Check the change

```bash
cargo fmt
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace
cargo run -- --generate-config
```

The last command writes to your config location and refuses to overwrite an
existing file without `--force`. Inspect the generated field and its comments
without replacing a personal config. See [Tests](tests.md) for fixtures.
