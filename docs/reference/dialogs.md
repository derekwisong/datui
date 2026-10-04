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
- A multiline field (a view's description) types <kbd>Enter</kbd>, and
  <kbd>↑</kbd> <kbd>↓</kbd> move between its lines, leaving it from the first or
  last; <kbd>Ctrl</kbd>+<kbd>J</kbd> applies from there.
- In a picker, typing narrows the list, <kbd>↑</kbd> <kbd>↓</kbd> move,
  <kbd>Space</kbd> chooses (or toggles, where several can be chosen) and
  <kbd>Tab</kbd> chooses and moves to the next field.
- The chart's options apply as they change, so <kbd>Enter</kbd> there acts as
  <kbd>Space</kbd> does.

The [Info panel](../user-guide/dataset-info.md) is a viewer, not a dialog:
<kbd>←</kbd> <kbd>→</kbd> and <kbd>Tab</kbd> <kbd>Shift</kbd>+<kbd>Tab</kbd>
switch its tabs.
