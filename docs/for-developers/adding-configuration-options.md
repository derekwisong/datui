# Add configuration options

Start in `crates/datui-lib/src/config.rs`. Use an existing option in the same
section as a model, and update all six parts:

| Part | Change |
|---|---|
| Struct | Add the field to the appropriate config section |
| Default | Set its value in that section's `Default` implementation |
| Generated comments | Add its description to the section's `*_COMMENTS` array; if it is unset by default, add an example to `UNSET_EXAMPLES` |
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
```

These are excerpts from separate locations, not one Rust block to paste.
The control-bar construction in `lib.rs` reads the setting in
`with_notes_pending`. Follow that path when
checking whether a new setting reaches its intended behavior.

The generated config shows ordinary fields as commented examples. The public
dataset catalog is active TOML; preserve that distinction.

## Merge and validation rules

Each config file is a `ConfigLayer`: the TOML keys it wrote, nothing filled in.
Layers merge in import order, then `AppConfig::from_layers` applies the defaults
once. A new field needs no merge code.

| Key | Across layers |
|---|---|
| Any value | The last layer that writes it wins, even when it writes the default |
| Table | Merged key by key |
| `COMBINED_KEYS` lists | Added up (`Union`) or matched by `name` (`ByName`) |
| `CLOUD_BLANK_IS_UNSET` | A blank string is no value |
| `theme.colors` | Laid over the palette for the resolved `theme.mode` |

Add a key to `COMBINED_KEYS` only when it is a list that should add up or a
named array of tables. Environment and command-line cloud settings go through
`CloudConfig::overlay` in `effective_cloud`, after the files.

Add range or format checks to the appropriate validation method when needed.
Test an omitted field, an explicit value, an explicit default over an
imported value, and invalid/boundary values where relevant. A test that only assigns a field and
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
`Theme::from_config`, and the theme's default values.
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
