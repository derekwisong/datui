Output demos/02-querying.gif
Set FontSize 18
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE
Hide

Type "datui tests/sample-data/people.parquet"
Enter

# Wait for TUI to load
Sleep 0.5s

Show

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
