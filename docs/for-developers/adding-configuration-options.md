# Add configuration options

Every config key is one entry in `SETTINGS` in
`crates/datui-cli/src/settings.rs`:

```rust
s("display.notes_accent", Bool, Value("true"), "Accent the i key when datui has noticed something about the data."),
```

| Part | What it is |
|---|---|
| Key | `section.name`, matching the field's place in `AppConfig` |
| Kind | How `-c` and the reference read the value: `Bool`, `Count`, `Text`, `Path`, `List`, `Choice(&[..])`, `Size`, `Duration`, `Color`, `Toml(shape)`, `Tables` |
| Default | `Value("…")` as TOML; `Unset("example")` when there is none; `Color { dark, light }` for a theme slot |
| Doc | One or two sentences. It is the generated config's comment, the reference's description and the flag's help |
| `.flag("name")` | The dedicated flag, only when one invocation needs it. Anything else is reachable with `-c` |
| `.kwarg("name")` | The keyword Python's `datui.view()` takes for it, when it has one |

Then add the field it fills to the section's struct in
`crates/datui-lib/src/config/mod.rs`, with its value in the section's `Default`, and
read the merged setting where the behavior lives.

From the entry, without more code:

| Generated | From |
|---|---|
| `-c KEY=VALUE` | `Override` parses the value for the kind; unknown keys get the nearest ones |
| `datui config init` | `generate_default_config` writes every entry, commented, at its default |
| `docs/reference/settings.md` | `render_settings_markdown` |
| The keyword table of `docs/reference/python-api.md` | `render_python_options_markdown` in `docgen.rs` |

Write the generated pages, which `the_generated_docs_are_current` compares with
the registry:

```bash,repo
cargo run -p datui-cli --bin gen_docs -- write
```

`the_registry_and_the_config_structs_agree` in `config/mod.rs` fails when a key the
defaults serialize is not registered, or a registered default differs from the
struct's.

## Merge and validation rules

Each config file is a `ConfigLayer`: the TOML keys it wrote, nothing filled in.
Layers merge in import order, then the `-c` layer, then `AppConfig::from_layers`
applies the defaults once. Flags are applied after. A new key needs no merge code.

| Key | Across layers |
|---|---|
| Any value | The last layer that writes it wins, even when it writes the default |
| Table | Merged key by key |
| `COMBINED_KEYS` lists | Added up (`Union`) or matched by `name` (`ByName`) |
| `CLOUD_BLANK_IS_UNSET` | A blank string is no value |
| `theme.colors` | Laid over the palette for the resolved `theme.mode` |

Add a key to `COMBINED_KEYS` only when it is a list that should add up or a
named array of tables. Add range or format checks to `AppConfig::validate`.
Test an omitted field, an explicit value, an explicit default over an imported
value, and invalid or boundary values where relevant.

## Add a flag

A flag exists when one invocation needs it: what to open, how to read this
file, what to do at start. Give the entry `.flag("name")`, add the field to `Args`
in `crates/datui-cli/src/lib.rs`, apply it after config loading in
`startup::apply_args` or `OpenOptions::from_args_and_config`, and test that it
beats `-c`. `gen_docs write`, as above, rewrites the command-line reference
too.

## Add a color

Add the `color(...)` entry with both defaults, and the field to `ColorConfig`,
`ColorConfig::dark`, `ColorConfig::light`, `ColorConfig::validate` and
`Theme::from_config`, and the slot to the `color_slots!` list in
`config/mod.rs`, which gives `Theme` its typed accessor. Use the accessor in
rendering code; never a hardcoded color in a widget. Name it for its purpose, such as `modal_border_active`.

## Check the change

```bash,repo
scripts/dev/test.sh cli
scripts/dev/test.sh unit config::
scripts/dev/test.sh integration config
```
