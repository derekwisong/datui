# TODO - Pre-Beta Release

## 🔴 Critical (Fix Before Beta)

### Clippy Warnings (8 warnings)

- [ ] **Fix `.get(0)` unsafe access** (3 occurrences)
  - `src/widgets/datatable.rs:199` - Use `col.first()` or proper error handling
  - `src/statistics.rs:203, 552` - Use safer indexing methods

- [ ] **Refactor function with too many arguments**
  - `src/widgets/analysis.rs:32` - 14 arguments (max 7 recommended)
  - Extract into struct (e.g., `RenderConfig`, `DisplayOptions`)

- [ ] **Remove `as_ref.map()` doing nothing** (4 occurrences)
  - `src/widgets/analysis.rs:353, 371, 511, 529`
  - Change `opt.as_ref().map(|x| x)` → `opt.as_ref()`

## 🟡 Medium Priority (Polish)

### Clippy Warnings (~16 warnings)

- [ ] **Remove redundant closures** (2)
  - `src/widgets/datatable.rs:636, 660`
  - Change `.map(|x| col(x))` → `.map(col)`

- [ ] **Remove unnecessary type casts** (5)
  - `src/widgets/datatable.rs:1012, 1024, 1028, 1096, 1099`
  - Remove `usize as usize` casts

- [ ] **Use `filter_map` with identity → `.flatten()`** (2)
  - `src/statistics.rs:353, 369`

- [ ] **Replace clamp-like patterns with `clamp()`** (3)
  - `src/widgets/analysis.rs:1288`, `src/statistics.rs:838, 1120`

- [ ] **Simplify pattern matching** (3+)
  - `src/statistics.rs:204` - Collapse nested `if let`
  - `src/widgets/analysis.rs:492` - Use `is_some()` instead of pattern match
  - Multiple `else { if ... }` → `else if`

- [ ] **Fix loop variable only used for indexing**
  - `src/widgets/analysis.rs:416` - Use `iter().enumerate()` instead

### Code Quality

- [ ] **Extract common initialization patterns**
  - `src/lib.rs:177-207` - CacheManager, ConfigManager, TemplateManager fallback logic
  - Extract helper: `init_with_fallback<F, T>(name: &str, init: F) -> T`

- [ ] **Standardize naming conventions**
  - Document difference between `::new()` vs `::with_dir()` patterns

- [ ] **Add public API documentation**
  - Add `///` docs for public structs: `App`, `DataTableState`, `OpenOptions`
  - Document public methods on `App`, `DataTableState`
  - Module-level docs for `query`, `statistics`, `template`

## 🟢 Low Priority (Style/Best Practices)

### Clippy Warnings (~54 warnings)

- [ ] **Add `Default` implementations** (6 structs)
  - `DataTable`, `Controls`, `TemplateModal`, `SortModal`, `FilterModal`, `AnalysisModal`
  - Consider if `::new()` logic allows `Default::default()`

- [ ] **Use `#[derive(...)]` where possible** (4)
  - `src/widgets/debug.rs:23`, `src/widgets/template_modal.rs:11, 28, 47`

- [ ] **Fix formatting/styling** (miscellaneous)
  - Empty line after doc comment (`src/widgets/info.rs:10`)
  - Useless type conversion (`src/widgets/datatable.rs:324`)
  - Unnecessary references/derefs (8 occurrences)
  - Useless `vec!` usage (5 occurrences)
  - Single-char string literals in `insert_str()` (2)
  - Manual prefix stripping → `strip_prefix()` (2)
  - `match` for equality → `if` (2)
  - Complex type → extract `type` alias (1)
  - `char::is_digit(10)` → `char::is_ascii_digit()` (1)

### Code Quality

- [ ] **Type aliases for common patterns**
  - `type ColumnName = String;`
  - `type ColumnNames = Vec<ColumnName>;`

- [ ] **Test improvements**
  - Replace `unwrap()` with `expect()` in tests (descriptive messages)
  - More descriptive test names

## Summary

- **Critical**: 3 tasks (8 clippy warnings)
- **Medium**: 9 tasks (~16 clippy warnings + 3 code quality items)
- **Low**: ~24 tasks (~54 clippy warnings + 2 code quality items)

**Recommendation**: Address Critical items before beta. Medium and Low can be done incrementally.
