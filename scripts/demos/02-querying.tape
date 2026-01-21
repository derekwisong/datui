Output demos/02-querying.gif
Set FontSize 14
Set Width 1000
Set Height 600
Set TypingSpeed 50ms

Type "datui tests/sample-data/people.parquet"
Enter

# Wait for TUI to load
Sleep 0.5s

# Open query input (press '/')
Type "/"
Sleep 0.5s

# Type a query to select and filter data
Type "select first_name, last_name, city, salary where salary > 80000"
Sleep 0.1s

# Execute query (Enter)
Enter

# Show query results
Sleep 3s

# Quit
Type "q"
