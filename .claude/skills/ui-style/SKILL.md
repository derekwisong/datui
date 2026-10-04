---
name: ui-style
description: The canon for how datui looks, reads and responds. Load before any change to render/, widgets/, control bars, the key registry, keybinds, or before designing a new modal, form, sidebar or screen. Carries the hard rules, the component patterns, the keybind compatibility contract, and the acceptance checklist every UI PR must pass.
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
  `table_alternate_row`. Backgrounds never carry meaning beyond these.
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
optional one-line footer of chips, with a blank row between it and the
content so a focused field's accent never reads as part of a chip (#650). Confirm/Success/Error, every modal, every
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

**Plot axes** — `widgets/axes.rs`, shared by the chart view and the
Distribution plots. Ticks at round values (1-2-5 steps, calendar boundaries
on a time axis), about one label per 15 columns and one per 4 rows; labels
never touch, and a crowded axis takes a coarser step before a shorter form.
Tick marks and the grid are `PlotMarks` slots with ASCII twins. The grid is
off until asked for, sits under the series in `chart_grid` (a shade under
`dimmed`, and a color rather than a grey so 16 colors keep it off the
background), and never takes a cell a series drew in; a legend names two or
more series from the emptiest corner. The XY crosshair (`x`, or a click) takes
the keys from the options: its line is in the accent, under the series, and
its readout sits under the plot.

## Shapes

Four shapes, chosen by what the user needs to keep seeing:

- **Sidebar** (right, data stays visible): iterative controls whose effect
  you watch — Sort & Filter, chart options, views.
- **Centered dialog** (small): a commitment — confirm, export, errors.
- **Takeover** (full screen): a different way of looking — analysis, chart
  canvas.
- **Strip** (bottom): text entry — query, go-to-line.

A feature gets one shape. Hints, tabs, focus and footers work identically
across shapes.

## Focus

One signal: the focused element carries the accent (title, label, or rail).
Focus never silently jumps (the analysis screen's jump-to-results is a
defect, not a pattern). Selection that is not focused stays visible, dimmed.

### Forms: one set of keys

Every form, dialog and option sidebar is a `crate::form::Form`: an ordered
list of fields, each a `FieldKind` (Text, MultilineText, Choice, Checkbox,
Picker, Button). `form::key` decides what a key means and moves focus;
`form::picker_key` does the same for an open picker. The modal only says
what a submit, a step or an action does. Never hand-roll focus enums'
next/prev or per-modal arrow handling.

| Key | Does |
|---|---|
| Tab / Shift+Tab, ↓ / ↑ | Next / previous field, wrapping, from the moment the form opens |
| ← / → | Step a Choice (or a pick-one Picker's value); flip a Checkbox; cursor in Text |
| Space | Checkbox: toggle. Choice: next value, wrapping. Picker: open. Button: do it |
| Enter | Submit from any field (MultilineText types it; Ctrl+J submits). Picker: choose |
| Esc | Close an open picker, otherwise cancel the form |
| Ctrl+P / Ctrl+N | History in a Text field; ↑ ↓ never recall in a form |

- `h j k l` are the arrows on a field that does not type; letters never
  open a picker (Space does, then typing narrows).
- A hidden or disabled field is left out of `fields()`, so focus skips it;
  `settle_focus` after a change that hides the focused one.
- A tab bar is the form's first field, a Choice: ←/→ switch tabs there, and
  only there. A viewer with no fields (the Info panel) switches tabs with
  ←/→ and Tab from anywhere instead.
- Short enumerations are Choices; long lists (columns) are Pickers.
- A form that applies live (the chart options) treats Enter as Space.
- Mouse: a click focuses through `Form::focus(field)`, then acts as Space.

## Feedback

Every message earns its interruption level. The channel is chosen by what
the user must do about the information, never by which feature sent it:

1. **State gets a mark, not a message.** Anything that stays true while it
   holds — sort direction, the row count, the reshape chip, the drill
   breadcrumb — lives on screen as state (hard rule 6) and is never
   re-announced in a message.
2. **Completions get a flash.** An action that finished and needs no
   decision — a copy, an export, a saved view — shows one plain sentence in
   the control bar's status region, without a spinner, cleared after about
   two seconds or on the next keypress, whichever comes first. Appearing
   and expiring move nothing around it: the flash renders exactly where the
   busy status message renders.
3. **Validation stays inline.** A form that cannot apply says why on its
   own status line inside the Surface, `warning` at most. Enter on an
   invalid form re-accents that line; it never raises a modal.
4. **Only a failure that stops the user gets a modal** — a load that
   failed, an export that could not write, an auth that was refused.
   Acknowledge to continue. The modal is a Surface like any other.

One flash component serves every screen: the home status line is the same
component, not a sibling, so duration, clearing and styling cannot drift.
The success modal is retired — any use of it that fits rung 2 becomes a
flash. Non-critical background errors (cache, history) keep logging and
continuing; silence is the right channel for them.

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
error. Never assume the font: no Nerd Font glyphs, and a new Unicode
character must pass the coverage audit (`scripts/code/audit_glyphs.py`,
documented in `docs/for-developers/glyph-audit.md`) — present in
JetBrainsMono Nerd Font, Liberation Mono and Noto Sans Mono, and never an
`Emoji=Yes, Emoji_Presentation=No` codepoint those fonts don't all carry,
because a missing one falls back to the color emoji font and renders a
blank cell (#325 has the full rule and history). Richer glyphs are the
user's `[glyphs]` config overrides, never the defaults.

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
  table); `Home/End` (`G`); `:` go-to-line. At the table `h/l` and `←/→`
  move the column cursor, and the columns scroll only when it would leave
  the screen; `[ ] { }` and `g` move it too (#574 made the change; before,
  `h/l` scrolled the view a column).
- Esc's layered back-out; `q` pops to home when the dataset was opened
  from it and quits otherwise (the control bar says which); `Ctrl+Q` quits
  and `Ctrl+C` from anywhere, a text field included (#649; a field copies
  with `Alt+W`); `Q` quits
  at the table and during a load, and is an ordinary key inside other
  surfaces (chart deliberately has no quit key); `Ctrl+O` home. (#320
  landed the approved `q` evolution.)
- `?` and F1 for help, including home's empty-filter `?`.
- The feature keys: `/ f n N s c a p e i v V r R # F , D H L + -`,
  Enter-to-drill (where there is no group to drill into, Enter inspects the
  row, as Space does; #542).
  (#317 moved the views list from `t`/`T` to `v`/`V` with the rename;
  #559 moved row numbers from `N` to `#` for find's `f`, `n` and `N`;
  #558 gave `F` to value counts and moved digit grouping to `,`;
  #737 gave `H`/`L` to moving the cursor's column and `+`/`-` to filtering
  on its cell, and moved the CSV header toggle to `H` on the Info panel's
  Schema tab.)
- Text fields keep their readline bindings.
- Home's type-to-filter: every printable except `?` and Space (empty filter
  only) and `~` goes to the filter. Never assign a letter key on the home
  screen.

Any other key may move, with its key registry entry and a release-notes line
updated in the same PR. The docs' key tables are generated from the help
strings: run `cargo run -p datui-cli --bin gen_docs -- write`.

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
- [ ] Help string and docs page updated, `gen_docs write` run
      (keyboard-shortcuts.md is generated from the key registry); buffer test
      for the layout; integration test for the keys.
- [ ] Every code block in the docs page is runnable and passes
      `scripts/docs/doc_examples.py`, or is labeled `,template` with
      `<PLACEHOLDER>`s; the page leads with the key, then a table.
- [ ] Before/after screenshots in the PR.

## Anti-pattern gallery (what the migrations delete)

- A bordered box per form field, label as box title (the old template form).
- Bordered `Save`/`Cancel` buttons reached by Tab.
- Key hints inside a title (`ACCESS PLAN — Esc Close`).
- A 4×2 radio grid navigated by arrows.
- A filter box that silently filters other lists than the one it sits on.
- `Modifier::REVERSED` as a tab highlight in one screen and BOLD in another.
- A dataset mutated with nothing on screen saying so.
