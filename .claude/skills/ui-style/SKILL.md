---
name: ui-style
description: The canon for how datui looks, reads and responds. Load before any change to render/, widgets/, control bars, help-strings, keybinds, or before designing a new modal, form, sidebar or screen. Carries the hard rules, the component patterns, the keybind compatibility contract, and the acceptance checklist every UI PR must pass.
---

# The datui visual style

datui is a terminal tool for looking at data. The data is the interface;
everything else is chrome, and chrome whispers. When a screen feels heavy,
the cause is almost always decoration doing a job that alignment, one accent
color, or a plainer sentence could do.

Two sensibilities exist in the code today. The light one — the home screen,
the data table, the chart options list, the notes pane — is the style. The
boxy one — bordered boxes around every form field, bordered buttons, nested
frames — is legacy, and every migration moves a screen from the second to
the first. Never add new UI in the boxy style.

## Hard rules

These are checkable, and CI or review should treat a violation as a defect:

1. **One border per surface.** A modal, sidebar or overlay gets one rounded
   frame and one title. Nothing inside it gets another border — not fields,
   not lists, not buttons. Structure inside comes from alignment, section
   rules, and the accent.
2. **No bordered buttons.** An action is a key, and the footer names it
   (`Enter Apply   Esc Cancel`). There is nothing to Tab onto and "press".
3. **Every color is a `ColorConfig` slot.** No `Color::` literals in widgets
   or render code. The current row uses the theme's `highlight_style` helper;
   `Modifier::REVERSED` never appears outside it.
4. **Every glyph is a `glyphs.rs` slot with an ASCII twin.** `LANG=C` must
   render every screen legibly. No emoji, no Nerd Font characters. Slots
   already exist for checkboxes, radios, dots, score marks, sort marks,
   ellipsis, middot, spinner, scrollbar; add a slot before adding a symbol.
5. **Esc discards, Enter applies.** Esc closes a surface and its staged
   edits die with it; reopening shows applied state, never leftovers. Enter
   applies from anywhere in a form (a multiline field is the one exception,
   where Enter types and the footer says what applies).
6. **State is visible.** Anything that changes what the table shows leaves a
   mark: sort direction on the header, `Rows: 417 of 1,000` under a filter
   or query, the drill-down breadcrumb, the reshape chip. If a feature
   mutates the view invisibly, the feature is unfinished.
7. **The keybind contract (below) is frozen.** Everything else about a
   screen may change.

## Color

- One accent (`accent`, `accent_bright`): keys in chips, focused titles and
  labels, the selection rail, the active tab. If two things on one screen
  compete for the accent, one of them is wrong.
- Three chrome tiers a few shades apart: `controls_bg`, `table_header_bg`,
  `alternate_row_color`. Backgrounds never carry meaning beyond these.
- Column names take their type's color; nulls are `∅` in `dimmed`.
- `gradient_start`/`gradient_end` color the wordmark only.
- Errors use the error slots; warnings `warning`; everything informational
  is plain or `dimmed`. No new semantic colors without a config slot and a
  dark/light default pair.

## Text

- Titles: unpadded Title Case (`Sort & Filter`, `Pivot & Melt`). Never
  SCREAMING, never key hints inside a title.
- Field labels: sentence case with a colon (`X axis:`, `Log scale:`).
  Spelled out — no `Col`/`Op`/`Val` abbreviations.
- Chip labels: Title Case, short (`Open all`, `Sort & Filter`).
- Key spelling: `^X` in chips; `Ctrl+X` in help text and prose. Arrows are
  glyphs (`↑↓`, with ASCII twins), never the words "Up/Down".
- American English. No filler ("Please…", "Note that…"). A label is a noun
  or a verb, not a sentence; a status line is one sentence, not a paragraph.
- Ellipsis in status lines is ASCII `...`; the `ellipsis` glyph is only a
  truncation marker inside content.

## Components

Build these once in `widgets/ui/` and reuse them everywhere; a screen that
hand-rolls one of these is a migration target.

**Surface** — the one border. Rounded, Title Case title on the frame,
optional one-line footer of chips. Confirm/Success/Error, every modal, every
sidebar, the help overlay: all Surfaces.

**FormRow** — `label  value` on one line inside a Surface, behind a reserved
one-column rail gutter. The focused row carries the `▎` rail and its label in
the accent plus the cursor; unfocused rows stay plain, and the chosen value
is always echoed so nothing is ambiguous when focus is elsewhere. The gutter
is always there, so focus arriving moves nothing — Tab walks the rail down
the rows, which is what tells a first session the rows are walkable.
Variants: text, number (`←/→` or `+/-` adjust), toggle (checkbox glyph), and
picker (opens a Picker; the row shows the current choice).

```
╭Pivot & Melt─────────────────────────────╮
│ Pivot │ Melt                            │
│                                         │
│  Index        department                │
│  Columns      job_title                 │
│ ▎Values       salary                    │   ← focused row: rail, accent
│  Aggregate    avg                       │      label, value under edit
│                                         │
│ department × job_title → avg(salary)    │   ← the spec, echoed in full
│ Enter Apply   Tab Next   Esc Cancel     │   ← HintBar, chips
╰─────────────────────────────────────────╯
```

**Picker** — the type-to-narrow list (the Sort tab already has the right
one; extract it). Type filters, `↑↓` move, Enter chooses and returns to the
row. Used for columns, operators, formats, aggregations, sheets. A radio
group is a short Picker, not a grid: arrow-navigation across a 4×2 grid is
how the wrong aggregation gets exported. The selection carries the rail and
the tint while the list is focused, and only the accent when it is not:
inside one Surface the rail means focus, and Tab visibly moves it.

**HintBar** — the chip row. One renderer shared by the global control bar
and every Surface footer. Primary action first, Esc last; only keys that
work right now; nothing a first session needs may live only in `?`.

**Section rule** — the home screen's `TITLE ── count` line. The way to
divide space inside a Surface without borders.

## Shapes

Four shapes, chosen by what the user needs to keep seeing:

- **Sidebar** (right, data stays visible): iterative controls whose effect
  you watch — Sort & Filter, chart options, templates.
- **Centered dialog** (small): a commitment — confirm, export, errors.
- **Takeover** (full screen): a different way of looking — analysis, chart
  canvas.
- **Strip** (bottom): text entry — query, go-to-line.

A feature gets one shape. Hints, tabs, focus and footers work identically
across shapes.

## Focus

One signal: the focused element carries the accent (title, label, or rail).
Tab moves focus forward through a Surface's rows, Shift+Tab back; `←/→`
switch tabs whenever a tab bar exists, from anywhere in the Surface. Focus
never silently jumps (the analysis screen's jump-to-results is a defect, not
a pattern). Selection that is not focused stays visible, dimmed.

## Every terminal, every size

datui runs on a truecolor desktop terminal, over SSH with a C locale and a
bitmap font, maximized on an ultrawide, and in a 60-column strip parked in
the corner of someone's screen. A screen is not done until it works in all
four, and neither extreme is the one that suffers.

**Capability tiers.** Four, degrading independently: UTF-8 → ASCII (the
glyphs.rs twin sets), and truecolor → 256 → 16-color ANSI (ColorConfig does
the mapping). Every screen must be legible at the floor of both — check
with `LANG=C` and with `TERM` forced to a 16-color terminal. Meaning may
never live only in a glyph or only in a color: the ASCII twin carries the
same distinction, and a 16-color palette still separates accent, dimmed and
error. Never assume the font: no Nerd Font glyphs, and new Unicode comes
from blocks any UTF-8 font covers, chosen Neutral width (not Ambiguous, or
East Asian locales render it double-wide and columns shear).

**Small windows.** The baseline is full usability at 80×24, and graceful
loss down to roughly 60×20. When width or height runs out, elements
yield in reverse order of importance: branding first (the home wordmark
already steps down to the one-line title on short or narrow terminals),
then conveniences, then primary actions; the way out (Esc/quit chip) goes
last, the data never — which is why the control bar is
built most-important-leftmost and cut from the right. Sidebars cap their
share of the width and collapse before the table does; overlays scroll
inside a capped frame rather than growing past the screen; nothing ever
wraps a table row. When height runs out, footers and headers stay, content
scrolls, and partial items are counted ("… 3 more") rather than half-drawn.

**Ultrawides.** The failure mode is distance, not space. Facts a decision
needs must sit next to the thing decided about — the locality marker lives
beside the row's name, not only in a details pane a foot to the right, for
exactly this reason. Reading surfaces (help, notes, detail panes) cap their
line length at a comfortable measure instead of stretching; tables may use
the width, prose may not. Centered dialogs stay compact rather than scaling
with the terminal.

**No jitter.** Labels arriving asynchronously, spinner frames, and count
updates must not move anything around them: equal-width frames, reserved
columns, and same-width glyph pairs are the rule everywhere something
updates in place.

## Keybind compatibility contract

Frozen — users may be retrained on form internals, never on moving and
leaving:

- Arrows and `h/j/k/l`; `PgUp/PgDn` (and `Ctrl+F/B`, `Ctrl+D/U` at the
  table); `Home/End` (`G`); `:` go-to-line.
- Esc's layered back-out; `q`, `Q`, `Ctrl+Q`, `Ctrl+C` to quit; `Ctrl+O`
  home. (An approved evolution of `q` to "pop to home when home is in the
  stack" is tracked in the navigation issue; until it lands, `q` quits.)
- `?` and F1 for help, including home's empty-filter `?`.
- The feature keys: `/ s c a p e i t T r R N F D H`, Enter-to-drill.
- Text fields keep their readline bindings.
- Home's type-to-filter: every printable except `?` (empty filter only) and
  `~` goes to the filter. Never assign a letter key on the home screen.

Any other key may move, with the help string, the docs key table and a
release-notes line updated in the same PR.

## Acceptance checklist for a migrated screen

- [ ] One border on the surface; zero inside it; no bordered buttons.
- [ ] Focused element accented; selected-but-unfocused still visible.
- [ ] All local keys in a HintBar footer, chip grammar, primary first.
- [ ] No `Color::` literals; no `REVERSED` outside the theme helper; every
      glyph from `glyphs.rs`; `LANG=C` screenshot is clean.
- [ ] Usable at 80×24 and degrades sanely to ~60×20; reading surfaces cap
      their measure on wide terminals; nothing jitters as labels arrive.
- [ ] Esc discards; Enter applies; reopening shows applied state.
- [ ] View-changing state visible on the main screen after the surface
      closes.
- [ ] Contract keybinds identical before and after.
- [ ] Help string, docs page and keyboard-shortcuts.md updated; buffer test
      for the layout; integration test for the keys.
- [ ] Before/after screenshots in the PR.

## Anti-pattern gallery (what the migrations delete)

- A bordered box per form field, label as box title (the old template form).
- Bordered `Save`/`Cancel` buttons reached by Tab.
- Key hints inside a title (`ACCESS PLAN — Esc Close`).
- A 4×2 radio grid navigated by arrows.
- A filter box that silently filters other lists than the one it sits on.
- `Modifier::REVERSED` as a tab highlight in one screen and BOLD in another.
- A dataset mutated with nothing on screen saying so.
