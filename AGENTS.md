# Datui - Agent and Developer Documentation

This document provides comprehensive context for both LLM agents and human developers working on datui.

## Project Overview

Datui is a terminal user interface (TUI) for exploring and analyzing tabular data files. Built with Rust, Polars, and Ratatui, it provides an interactive environment for data exploration, querying, statistical analysis, and visualization within the terminal.

**Core Philosophy**: Memory-efficient data exploration using lazy evaluation, providing quick insights into datasets without requiring external tools or heavy infrastructure.

## Technology Stack

### Core Technologies

- **Rust**: Primary language for application core
- **Polars (0.52.0)**: Lazy API for all dataframe operations
  - Features: `lazy`, `parquet`, `json`, `polars-io`, `timezones`, `strings`, `pivot`
  - All operations use lazy evaluation by default
  - Data is collected only when needed for display
- **Ratatui (0.30.0)**: TUI framework for rendering
  - Uses `DefaultTerminal` with crossterm backend
  - Widget-based rendering system
- **Crossterm**: Terminal event handling and input

**NOTE: Agents are not permitted to downgrade package versions without express permission**

### Supporting Tools

- **Clap**: Command-line argument parsing
- **Color-eyre**: Error handling and reporting
- **Serde/Serde-json**: Template serialization (JSON format)
- **Dirs**: Standard cache/config directory management
- **FS2**: File locking for cache operations
- **Regex**: Pattern matching for queries

### Development Tools

- **Python + Polars**: Ad-hoc scripting and test data generation (`scripts/`)
- **Cargo**: Build system and testing
- **Rustfmt**: Code formatting (`cargo fmt`)
- **Clippy**: Linting and code quality checks (`cargo clippy`)

## Architecture Overview

### Application Structure

- **crates/datui-cli**: Shared CLI definitions (`Args`, `CompressionFormat`). Used by the main app, `build.rs` (manpage), and `gen_docs` binary (command-line-options markdown for mdbook).
- **src/bin/gen_docs.rs**: Emits `docs/reference/command-line-options.md` from Clap; run via `scripts/docs/generate_command_line_options.py` before mdbook.

```
src/
├── main.rs              # Entry point, CLI parsing, event loop
├── lib.rs               # App struct, event handling, main logic
├── widgets/             # UI widget implementations
│   ├── datatable.rs    # DataTable widget and state management
│   ├── controls.rs     # Control panel widget
│   ├── info.rs         # Info panel widget
│   ├── schema.rs       # Schema display widget
│   ├── debug.rs        # Debug overlay widget
│   ├── analysis.rs     # Analysis modal rendering
│   ├── chart.rs        # Chart view widget (sidebar + chart area)
│   ├── pivot_melt.rs   # Pivot & Melt modal rendering
│   └── template_modal.rs # Template management UI
├── query.rs            # Query parser and executor
├── chart_modal.rs      # Chart view state (type, x/y columns, options, export)
├── chart_data.rs       # Chart data preparation (LazyFrame → series points)
├── pivot_melt_modal.rs # Pivot & Melt modal state and specs
├── statistics.rs       # Statistical computation module
├── analysis_modal.rs   # Analysis modal state management
├── sort_filter_modal.rs # Sort & Filter tabbed modal state
├── sort_modal.rs       # Sort/column order (used by sort_filter)
├── filter_modal.rs     # Filter state (used by sort_filter)
├── template.rs         # Template storage and management
├── cache.rs            # Cache manager (query history, etc.)
└── config.rs           # Configuration manager (templates, etc.)
```

### Core Data Flow

1. **File Loading**: `main.rs` parses CLI args → `AppEvent::Open` → `App::event()` → `DataTableState::from_*()` → Creates `LazyFrame`
2. **User Input**: Terminal events → `AppEvent::Key` → `App::key()` → State mutation → `AppEvent` emission → Re-render
3. **Query Processing**: User types query → `query::parse_query()` → Polars expressions → `LazyFrame` transformations
4. **Display**: `App::render()` → Ratatui widgets → `terminal.draw()` → Terminal output

### Event System

**AppEvent Enum**: Central event type for all application actions
- `Key(KeyEvent)`: Keyboard input
- `Open(PathBuf, OpenOptions)`: File loading
- `Search(String)`: Query execution
- `Filter(Vec<FilterStatement>)`: Filter application
- `Sort(Vec<String>, bool)`: Sort application
- `ColumnOrder(Vec<String>, usize)`: Column reordering/locking
- `Pivot(PivotSpec)`: Pivot (long → wide) reshape
- `Melt(MeltSpec)`: Melt (wide → long) reshape
- `Collect`, `Update`, `Reset`: State management events
- `Exit`, `Crash(String)`: Lifecycle events

**Event Loop**: `main.rs` uses `mpsc::channel` for bidirectional communication
- Main thread: Polls terminal events, sends to channel
- App thread: Receives events, processes, may emit new events

### Configuration System

**Architecture**: 3-layer configuration with TOML format

**Layers** (priority order):
1. **Default Values**: Hardcoded in `Default` trait implementations
2. **User Config**: `~/.config/datui/config.toml` (XDG compliant)
3. **CLI Arguments**: Command-line flags (highest priority)

**Key Components**:
- `AppConfig`: Main configuration struct with all settings
- `ConfigManager`: Manages config directory and file operations
- `ColorParser`: Parses color strings with terminal capability detection
- `Theme`: Pre-parsed colors ready for rendering

**Color Formats**:
- Named: `"cyan"`, `"bright_red"`, `"dark_gray"`
- Hex: `"#ff0000"`, `"#00bfff"`
- Indexed: `"indexed(236)"` for xterm 256-color palette

**Configuration Flow**:
```
main.rs: AppConfig::load(APP_NAME)
  ↓
Merge: Default → User Config
  ↓
Validate: Version, ranges, colors
  ↓
Theme::from_config() → Parse all colors
  ↓
App::new_with_config(theme, config)
  ↓
OpenOptions::from_args_and_config() → Merge CLI args
  ↓
Settings applied throughout app
```

**Configurable Settings**:
- File loading: Delimiter, headers, skip lines/rows, compression
- Display: Row numbers, page buffers, row start index
- Performance: Sampling threshold, event poll interval
- Theme: 22 customizable colors
- Query: History limits, caching
- Templates: Auto-apply behavior
- Debug: Overlay settings

**Important**: The hardcoded `SAMPLING_THRESHOLD` constant in `statistics.rs` is now a fallback. The app uses `app.sampling_threshold` from config.

## Key Design Patterns

### 1. Lazy Evaluation

**Principle**: Never collect data unless absolutely necessary for display.

- All data operations work on `LazyFrame` until final render
- `DataTableState::collect()` is called only when:
  - Table needs to be displayed
  - Statistics need to be computed
  - Scrolling/slicing requires materialized data
- Large datasets are sampled when needed (threshold: 10,000 rows)

**Example**:
```rust
let lf: LazyFrame = /* query, filter, sort */;
// No collection yet - still lazy
let df: DataFrame = lf.collect()?; // Only now is data materialized
```

### 2. State Management

**DataTableState**: Central state for data operations
- `lf: LazyFrame`: Current query/filter/sort state
- `original_lf: LazyFrame`: Backup of original data
- `df: Option<DataFrame>`: Materialized data for display (collected lazily)
- `locked_df: Option<DataFrame>`: Locked columns (separate for fixed column display)
- Transformation state: `filters`, `sort_columns`, `active_query`, `column_order`

**App State**: UI and modal state
- `input_mode`: `Normal`, `Editing`, `SortFilter`, `PivotMelt`
- Modal states: `sort_filter_modal` (Sort + Filter tabs), `template_modal`, `analysis_modal`, `pivot_melt_modal`
- Focus management: Each modal tracks its own focus state

### 3. Modal Pattern

All major UI operations use modal dialogs:
- **Sort & Filter** (`s`): Tabbed modal; Sort tab (column order, sort, lock, visibility) and Filter tab (filter creation). `InputMode::SortFilter`
- **Template Modal**: Template listing and creation
- **Analysis Modal**: Statistical analysis with drill-down views
- **Pivot & Melt** (`p`): Reshape long↔wide

Each modal:
- Has its own state struct with `active: bool`
- Manages its own focus state
- Handles keyboard input when active
- Emits `AppEvent` on completion

### 4. Query System

**Supported Operations**:
- `select columns`: Column selection
- `where condition`: Filtering
- `by columns`: Grouping with aggregation
- Expressions: Arithmetic, comparisons, functions

**Implementation**: `query.rs` contains recursive descent parser
- Tokenization → Parsing → Polars expression generation
- Type-aware expression building
- Error reporting with position information

### 5. Sampling Strategy

**Threshold**: `SAMPLING_THRESHOLD = 10,000` rows (defined in `statistics.rs`)

**Behavior**:
- Datasets < 10,000 rows: Use full data
- Datasets >= 10,000 rows: Sample deterministically (respects `random_seed`)
- Sampling used for: Statistics computation, Q-Q plots, histograms, correlation matrices

**Unified Approach**: All analysis tools use the same sampling decision to ensure consistency

## Module Details

### DataTableState (`widgets/datatable.rs`)

**Purpose**: Manages data loading, transformation, and display state

**Key Methods**:
- `from_csv()`, `from_parquet()`, `from_json()`: File loading
- `query()`: Apply query
- `filter()`: Apply filter expressions
- `sort()`: Sort by columns
- `set_column_order()`: Reorder/lock columns
- `collect()`: Materialize `LazyFrame` to `DataFrame` for display
- `slide_table()`: Handle scrolling (lazy slicing)

**Important Details**:
- Maintains separate `locked_df` for fixed columns
- Uses `TableState` from Ratatui for row/column selection
- Handles grouped data with drill-down capability
- Caches collected dataframes until transformation occurs

### Statistics Module (`statistics.rs`)

**Purpose**: Statistical computation for analysis features

**Key Structures**:
- `ColumnStatistics`: Per-column stats (numeric and categorical)
- `DistributionAnalysis`: Distribution inference with Q-Q plot data
- `CorrelationMatrix`: Correlation computation between numeric columns
- `OutlierAnalysis`: Outlier detection with context data

**Computation Flow**:
1. Sample data if needed (based on `SAMPLING_THRESHOLD`)
2. Compute basic statistics (mean, std, percentiles, etc.)
3. Infer distribution type (Normal, LogNormal, Uniform, PowerLaw, Exponential)
4. Calculate fit quality and confidence scores
5. Detect outliers using IQR and Z-score methods

**Distribution Detection**: Uses statistical tests (Shapiro-Wilk for normality, chi-square for uniformity, etc.)

### Query Parser (`query.rs`)

**Purpose**: Parse and execute queries

**Parser Architecture**:
- Tokenizer: Converts input string to tokens
- Recursive descent parser: Builds AST from tokens
- Expression generator: Converts AST to Polars expressions

**Supported Syntax**:
- Column references: `col`, `column_name`
- Literals: Numbers, strings (with escape sequences)
- Operators: `+`, `-`, `*`, `/`, `%`, `==`, `!=`, `<`, `>`, `<=`, `>=`
- Functions: `len()`, `sum()`, `avg()`, `min()`, `max()`, etc.
- Clauses: `select`, `where`, `by` (group by)

### Template System (`template.rs`)

**Purpose**: Save and restore data view configurations

**Template Contents**:
- Query string
- Filter statements
- Sort columns and direction
- Column order
- Locked columns count

**Relevance Scoring**: Multi-factor algorithm
1. Exact path match: 1000 points (absolute) or 950 points (relative)
2. Exact schema match: 900 points
3. Pattern matching: 50 points (path pattern) or 30 points (filename)
4. Usage statistics: Frequency and recency bonuses

**Storage**: JSON files in `~/.config/datui/templates/`

### Cache System (`cache.rs`)

**Purpose**: Manage application cache (query history, etc.)

**Cache Directory**: Platform-specific
- Linux: `~/.cache/datui/`
- macOS: `~/Library/Caches/datui/`
- Windows: `%LOCALAPPDATA%\datui\cache\`

**Current Cache Files**:
- `query_history.txt`: Query history (1000 entry limit)

**Operations**: Uses file locking (fs2) for thread-safe access

## UI Patterns

### Keyboard Navigation

**Vim-like Binds** (where appropriate):
- `j`/`k`: Down/up navigation
- `h`/`l`: Left/right navigation
- `/`: Open query input
- `Esc`: Close modal/cancel

**Mode-based Input**:
- `InputMode::Normal`: Main view, modal shortcuts
- `InputMode::Editing`: Query input (with history navigation)
- `InputMode::SortFilter`: Sort & Filter modal active (tabs: Sort, Filter)

### Color Scheme

- **Cyan/Yellow**: Keybind hints and labels
- **Green**: Success states, normal distributions
- **Yellow**: Warnings, skewed distributions
- **Red**: Errors, outliers
- **Dark Gray**: Dimmed/hidden elements

### Widget Rendering

**Layout System**: Ratatui layout constraints
- Vertical splits: `Direction::Vertical`
- Horizontal splits: `Direction::Horizontal`
- Constraints: `Length`, `Percentage`, `Min`, `Max`

**Widget Types**:
- `Table`: Data table display with stateful scrolling
- `Paragraph`: Text display
- `List`: Item lists (templates, filters)
- `BarChart`: Histograms
- `Chart`: Q-Q plots (scatter plots)

## Development Workflow

### Python Utilities

- Make a local virtual env `python -m venv .venv` in the repo directory
- Use it to install the `requirements.txt` in the `scripts/` directory
- Use it to run the python scripts in the `scripts/` directory
- Use it to install the `pre-commit` hooks

### Building

```bash
cargo build              # Debug build
cargo build --release    # Release build
```

### Testing

```bash
cargo test              # All tests
cargo test --lib        # Unit tests only
cargo test --test integration_test  # Integration tests
```

### Code Quality Checks

**Pre-commit Hooks**:
- Using the [pre-commit](https://pre-commit.com/) framework
- For details see config file `.pre-commit-config.yaml`
- Runs checks for formatting (`cargo fmt`) and linter (`cargo clippy`) warnings before commit
  - Prevents the need for spurious format commits since all code is formatted before commit
  - CI builds run these checks, using the pre-commit hooks will prevent their failure

```bash
cargo fmt --check       # Check formatting (without modifying files)
cargo fmt               # Format code
cargo clippy            # Run linter (should produce zero warnings)
cargo clippy --fix      # Auto-fix clippy suggestions where possible
```

**Test Structure**:
- `tests/integration_test.rs`: End-to-end application tests
- `tests/statistics_test.rs`: Statistics computation tests
- `tests/template_test.rs`: Template system tests
- `tests/common/mod.rs`: Shared test utilities
- Unit tests are throught the codebase

### Debug Mode

Enable debug overlay: `datui --debug <file>`

Shows:
- Current LazyFrame query
- Transformation state
- Performance metrics

### Cache Management

```bash
datui --clear-cache     # Clear all cache data
```

## Important Implementation Details

### Memory Management

- **LazyFrame**: Zero-copy until collection
- **Sampling**: Automatic for large datasets
- **Column Ordering**: Minimal data copying (references when possible)
- **Cache**: File-based, not in-memory

### Error Handling

- **Graceful Degradation**: App continues on non-critical errors (e.g., cache failures)
- **User-Friendly Messages**: Errors shown in modals, not crashes
- **Error Suppression**: Query errors suppressed in input mode (shown after execution)

### Thread Safety

- **Single-threaded**: Main event loop runs on single thread
- **Channel Communication**: `mpsc::channel` for event passing (thread-safe)
- **File Locking**: `fs2` for cache file access

### Performance Considerations

- **Lazy Collection**: Only collect visible data
- **Slicing**: Use `LazyFrame::slice()` for pagination
- **Caching**: Materialized dataframes cached until transformation
- **Sampling**: 10,000 row default threshold for statistics

## Current Features

### Data Operations

- **File Loading**: CSV, Parquet, JSON, NDJSON with customizable options
- **Queries**: Select, filter, group, aggregate
- **Filtering**: Column-based filtering with logical operators
- **Sorting**: Multi-column sorting with ascending/descending
- **Column Management**: Reorder, lock (freeze), hide/show columns
- **Pivot** (long → wide): Index, pivot column, value column, aggregation (last/first/min/max/avg/med/std/count). Uses `DataTableState::pivot`, `pivot_stable` for first/last.
- **Melt** (wide → long): Index, value-column strategy (all except index, by pattern, by type, explicit), variable/value names. Uses `LazyFrame::unpivot`.

### Analysis Features

- **Statistical Analysis**: Column-level and overall statistics
  - Numeric: Mean, median, std dev, percentiles, skewness, kurtosis
  - Categorical: Unique count, mode, frequency distributions
- **Distribution Analysis**: Distribution type inference (Normal, LogNormal, Uniform, PowerLaw, Exponential)
  - Q-Q plots for visual comparison
  - Histograms with theoretical distributions
  - Outlier detection (IQR and Z-score methods)
- **Correlation Matrix**: Pairwise correlations between numeric columns

### UI Features

- **Responsive Layout**: Adapts to terminal size
- **Scrollable Tables**: Horizontal and vertical scrolling
- **Modal Dialogs**: Sort, filter, template, analysis, **Pivot & Melt** modals
- **Help System**: Context-sensitive help (`?` key)
- **Query History**: Navigate past queries with Up/Down arrows

### Templates

- **Save Configurations**: Save queries, filters, sorts, column orders
- **Auto-Apply**: `T` key applies most relevant template
- **Template Management**: `t` key opens template modal
- **Relevance Matching**: File path, schema, pattern-based matching

## Planned Features (from plans/)

See `plans/` directory for detailed implementation plans:

- **Analysis Improvements**: Enhanced distribution analysis, better Q-Q plots
- **Histogram Fixes**: Distribution-specific theoretical histograms
- **Info Panel** (`plans/info-panel-plan.md`): Tabbed Schema + Resources sidebar (known vs inferred schema, Parquet compression, file size, memory, format-specific details)
- **Partitioned Data**: Hive-style partitioning via directory/glob and `--hive`; Parquet (and CSV/NDJSON where supported); partition predicate pushdown; Partitioned data tab in Info panel
- **Polars Upgrades**: Version compatibility and API migration
- **UI Enhancements**: Improved sort modal, better visualizations

## Development Guidelines

### Code Style

- **Rust Conventions**: Follow standard Rust formatting (`rustfmt`)
- **Code Formatting**: All code should be formatted with `cargo fmt` before committing
- **Linting**: Code should compile with zero clippy warnings (`cargo clippy` should be clean)
  - Fix clippy warnings as they are introduced
  - Prefer clean clippy runs for all code changes
- **Error Handling**: Use `Result<T>` for fallible operations
- **Naming**: Use descriptive names, avoid abbreviations where unclear
- **Comments**: Document complex logic, especially Polars operations

### Testing Strategy

- **Unit Tests**: Test individual functions and modules
- **Integration Tests**: Test complete workflows (file load → query → display)
- **Edge Cases**: Empty datasets, single rows, very wide tables
- **Performance**: Test with large datasets (verify sampling works)

### Adding New Features

1. **Plan First**: Create plan document in `plans/` for complex features
2. **Follow Patterns**: Use existing modal/widget patterns
3. **Lazy First**: Prefer `LazyFrame` operations over `DataFrame`
4. **Test**: Add tests for new functionality
5. **Document**: Update help text and this document

### Modifying Existing Features

1. **Understand Current**: Read existing implementation thoroughly
2. **Preserve Behavior**: Maintain backward compatibility where possible
3. **Update Tests**: Ensure tests still pass, add new tests if needed
4. **Update Docs**: Update help text and documentation

## Common Patterns and Idioms

### Adding a New Modal

1. Create state struct in `src/{feature}_modal.rs`
2. Add to `App` struct: `pub {feature}_modal: {Feature}Modal`
3. Add `InputMode` variant if needed
4. Add keyboard handler in `App::key()`
5. Create widget in `src/widgets/{feature}.rs`
6. Render in `App::render()` when active

### Working with LazyFrame

```rust
// Start with LazyFrame
let lf: LazyFrame = /* load file */;

// Apply transformations (all lazy)
let lf = lf.filter(/* condition */);
let lf = lf.sort(/* columns */);
let lf = lf.select(/* columns */);

// Only collect when needed
let df: DataFrame = lf.collect()?; // Materialize here
```

### Adding Statistics

1. Compute on sampled data (if needed)
2. Use Polars expressions when possible (efficient)
3. Fall back to collected Series for complex computations
4. Store results in appropriate struct (`ColumnStatistics`, `DistributionAnalysis`, etc.)

### Error Messages

- **User-Facing**: Show in error modal with clear message
- **Debug**: Use `eprintln!()` for internal errors
- **Non-Critical**: Continue execution, log warning

## Key Constants

- `APP_NAME`: `"datui"` - Used for cache/config directories
- `SAMPLING_THRESHOLD`: `10_000` - Rows threshold for sampling

## Resources

- **Polars Docs**: https://docs.pola.rs/api/rust/
- **Ratatui Docs**: https://ratatui.rs/
- **Plans Directory**: Detailed implementation plans for features

## Notes for LLM Agents

When working on this codebase:

1. **Always prefer lazy evaluation**: Work with `LazyFrame` until display is needed
2. **Check existing patterns**: Look for similar features before implementing
3. **Respect sampling threshold**: Use `SAMPLING_THRESHOLD` for large datasets
4. **Follow modal pattern**: Use existing modal infrastructure for new dialogs
5. **Update help text**: When adding features, update help system
6. **Test edge cases**: Empty data, single rows, large datasets
7. **Preserve performance**: Avoid unnecessary data collection
8. **Read plans/**: Check for existing plans before implementing features
9. **Format code**: Run `cargo fmt` before submitting changes
10. **Clean clippy**: Ensure `cargo clippy` runs with zero warnings - fix all clippy warnings as part of code changes
