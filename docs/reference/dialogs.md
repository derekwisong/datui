# Dialogs

Every dialog and sidebar with fields takes the same keys: [Export](../user-guide/exporting-data.md),
[Copy](../user-guide/copying.md), [Sort & Filter](../user-guide/filtering-sorting.md),
[Pivot & Melt](../user-guide/reshaping.md), the [chart](../user-guide/charting.md)'s
options and its export, [Save View](../user-guide/views.md), and the analysis
forms (Sample, Expected, a column's intent).

| Key | Does |
|---|---|
| <kbd>↓</kbd> <kbd>↑</kbd> or <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd> | Next or previous field, wrapping. They work from the moment the dialog opens |
| <kbd>←</kbd> <kbd>→</kbd> | Change a choice (a format, a compression, a tab); move the cursor in a text field |
| <kbd>Space</kbd> | Act on the field: toggle a checkbox, take a choice's next value (wrapping), open a picker |
| <kbd>Enter</kbd> | Apply, from any field; in an open picker, choose |
| <kbd>Esc</kbd> | Close an open picker; otherwise close the dialog without applying |
| <kbd>Ctrl</kbd>+<kbd>P</kbd> <kbd>Ctrl</kbd>+<kbd>N</kbd> | Earlier or later entries of a text field's history (the export paths) |

- A field the dialog does not offer right now (a delimiter for Parquet, a
  pattern for a melt by type) is skipped.
- <kbd>h</kbd> <kbd>j</kbd> <kbd>k</kbd> <kbd>l</kbd> work as the arrows on a
  field that does not take typing.
- In a multiline field (a view's description), <kbd>Enter</kbd> types a
  newline, and <kbd>↑</kbd> <kbd>↓</kbd> move between its lines, leaving the
  field from the first or last line. <kbd>Ctrl</kbd>+<kbd>J</kbd> applies from
  there.
- In a picker, typing narrows the list, <kbd>↑</kbd> <kbd>↓</kbd> move,
  <kbd>Enter</kbd> chooses, and <kbd>Tab</kbd> chooses and moves to the next
  field. <kbd>Space</kbd> toggles an item where several can be chosen.
  Elsewhere it chooses until you start typing; after that it types a space, so
  you can narrow by a name of several words.
- The chart's options apply as they change, so <kbd>Enter</kbd> there acts as
  <kbd>Space</kbd> does.
- If a dialog cannot do what <kbd>Enter</kbd> asked, it says why on its last
  line, such as a blank path or an export that could not write. The dialog
  stays as you left it, so you can fix it and press <kbd>Enter</kbd> again.

Every question (overwrite a file, delete a view, run a full scan, read every
row) is one dialog with two named choices:

| Key | Does |
|---|---|
| <kbd>←</kbd> <kbd>→</kbd> (<kbd>h</kbd> <kbd>l</kbd>) or <kbd>Tab</kbd> | Pick a choice. One that destroys something starts on **No** |
| <kbd>Enter</kbd> | Confirm the one picked |
| <kbd>Esc</kbd> | Decline |
| <kbd>↑</kbd> <kbd>↓</kbd> (<kbd>k</kbd> <kbd>j</kbd>) | Scroll a long question |

An error says what went wrong; <kbd>Enter</kbd> or <kbd>Esc</kbd> closes it.

The [Info panel](../user-guide/dataset-info.md) is a viewer, not a dialog:
<kbd>←</kbd> <kbd>→</kbd> and <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd>
switch its tabs.
