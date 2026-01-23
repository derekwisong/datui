# Reshaping: Pivot and Melt

Datui supports reshaping tabular data between **long** and **wide** formats via the **Pivot/Melt** modal. Open it with **`p`** from the main view (when data is loaded).

- **Pivot** (long → wide): Turn rows with a key column into new columns. Example: `id`, `date`, `key`, `value` → `id`, `date`, `key_A`, `key_B`, `key_C`.
- **Melt** (wide → long): Collapse many columns into `variable` and `value` columns. Example: `id`, `Q1`, `Q2`, `Q3` → `id`, `variable`, `value`.

Both operations run on the **current** table—i.e. the result of any filters, sorts, or queries you’ve applied. You can filter or sort first, then pivot or melt that view.

## Pivot (long → wide)

1. **Index columns**: Group columns that stay on the left (e.g. `id`, `date`). Use the filter to search, Space to toggle selection. Order matters.
2. **Pivot column**: The column whose distinct values become new column headers (e.g. `key` → `A`, `B`, `C`). Use ↑/↓ to select.
3. **Value column**: The column whose values fill the new cells. Use ↑/↓ to select.
4. **Aggregation**: How to combine multiple values per group: **last**, **first**, **min**, **max**, **avg**, **med**, **std**, **count**. Default is **last**. If the value column is string-typed, only **first** and **last** are available.
5. **Sort new columns**: Toggle to sort new column names alphabetically.

**Apply** runs the pivot and closes the modal. **Cancel** or **Esc** closes without changing the table. **Clear** resets the form.

## Melt (wide → long)

1. **Index columns**: Columns to keep as identifiers (e.g. `id`, `date`). Same multi-select pattern as Pivot.
2. **Value-column strategy**:
   - **All except index**: Melt every column not in the index. Good default when you want to unpivot all measure columns.
   - **By pattern**: Regex over column names (e.g. `Q[1-4]_2024`, `metric_.*`). Type the pattern in the **Pattern** field.
   - **By type**: Melt all **Numeric**, **String**, **Datetime**, or **Boolean** columns (excluding index).
   - **Explicit list**: Manually pick value columns with Space to toggle.
3. **Variable name** / **Value name**: Output column names for the melted dimension and values. Defaults: `variable`, `value`.

**Apply** runs the melt and closes the modal. **Cancel** or **Esc** closes without applying. **Clear** resets the form.

## Keyboard shortcuts in the modal

- **Tab / Shift+Tab**: Move focus (tab bar → form fields → Apply → Cancel → Clear → tab bar).
- **Left / Right**: On the tab bar, switch between **Pivot** and **Melt**. In text fields (filter, pattern, variable/value names), move the cursor.
- **↑ / ↓**: Move selection in lists (index, pivot, value, aggregation, strategy, type, explicit list).
- **Space**: Toggle selection in index and explicit value lists; toggle “Sort new columns” in Pivot.
- **Enter**: Activate focused control (Apply, Cancel, Clear).
- **Esc**: Close modal without applying.
- **Ctrl+h**: Show help.

## Examples

- **Long sales data → wide by product**: Index = `id`, `date`; Pivot = `product`; Value = `revenue`; Aggregation = **last** or **sum** (if you pre-aggregate).
- **Wide sensor table → long**: Index = `timestamp`; Strategy = **All except index**; Variable = `sensor`, Value = `reading`.
