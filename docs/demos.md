# Demo Gallery

This page showcases interactive demonstrations of datui's key features. All demos are generated from real usage and show the application in action.

## Navigation Demo

![Basic Navigation Demo](../demos/01-basic-navigation.gif)

**What it shows:**
- Loading a Parquet file (`people.parquet`)
- Initial data display with headers
- Vertical navigation using `j`/`k` keys (scroll down/up)
- Horizontal navigation using `h`/`l` keys (scroll left/right)

**Key takeaway:** datui provides smooth, keyboard-driven navigation for exploring your data tables.

See [Loading Data](user-guide/loading-data.md) for more information about file formats and options.

## Querying Demo

![Querying Demo](../demos/02-querying.gif)

**What it shows:**
- Opening the query input with `/`
- Typing a query: `select first_name, last_name, city, salary where salary > 80000`
- Executing the query and seeing filtered results
- Clearing the query

**Key takeaway:** datui supports SQL-like queries for selecting columns and filtering data on the fly.

See [Querying Data](user-guide/querying-data.md) for detailed query syntax and examples.

## More Demos Coming Soon

Additional demos will be added for:
- Filtering data with the filter modal
- Sorting and column management
- Statistical analysis features
- Template management
- And more!

---

**Note:** All demos use sample data from `tests/sample-data/`. See the [Demos README](../demos/README.md) for information about generating your own demos.
