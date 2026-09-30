# Glyph Coverage Audit

```bash
scripts/code/audit_glyphs.py             # audit glyphs.rs against installed fonts
scripts/code/audit_glyphs.py --markdown  # the coverage table, for pasting
```

Every Unicode character datui draws is a slot in
`crates/datui-lib/src/glyphs.rs`, and every slot must pass this audit before
it ships. The script parses the `UNICODE` set, reads each font's charset
through fontconfig (`fc-list`, `fc-query`), and exits non-zero on a violation.

## The rules

| Rule | Why |
|---|---|
| Every codepoint exists in JetBrainsMono Nerd Font | Required coverage for the default set |
| No `Emoji=Yes, Emoji_Presentation=No` codepoint missing from any floor font | A terminal whose font lacks one falls back to the **color emoji** font and renders a blank cell or a clipped blob — no user font choice fixes it |
| The ASCII set is pure ASCII | It is the floor a C locale falls back to |

The floor fonts are JetBrainsMono Nerd Font, Liberation Mono and Noto Sans
Mono. A *non-emoji* codepoint missing from the last two is reported but
allowed: fontconfig substitutes another text font, which renders fine in one
color. A wider list (Fira Code, Hack, Cascadia, Iosevka, Menlo, SF Mono, Consolas,
DejaVu Sans Mono) is audited informationally wherever those fonts are
installed.

## Adding a glyph

1. Pick a codepoint and check it: `fc-list "<font>:charset=<hex>"` per floor
   font, or just add it to `glyphs.rs` and run the script.
2. Give it an ASCII twin of a workable width; the width tests in `glyphs.rs`
   say which slots must line up.
3. Run the audit. A failure names the slot, the codepoint and the font.

Users whose fonts carry more than the floor can override any slot with the
`[glyphs]` config section; see
[Configuration](../reference/settings.md#glyphs). The default set never
assumes more than the floor.
