## What changed

<!-- One or two sentences, and the issue it closes (Closes #N). -->

## What ran locally

<!-- The exact commands and their results, e.g. `./scripts/dev/test.sh preflight`
and the scoped tests for what you changed. See docs/for-developers/contributing.md. -->

## Checklist

- [ ] Keys changed: the key registry (`crates/datui-cli/src/keys.rs`) is updated
- [ ] Flags, settings or keys changed: `cargo run -p datui-cli --bin gen_docs -- write` was run
- [ ] User-visible: the docs page and `release-notes/v<next>.md` are updated
