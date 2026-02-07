Output demos/11-overview.gif
Set FontSize 18
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui tests/sample-data/charting_demo.parquet"
Enter
Sleep 0.5s
Show

# TUI already loaded: Parquet table view
Sleep 2s

# Scroll down through the table (j = down)
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.08s
Type "j"
Sleep 0.5s

# Scroll back up (k = up)
Type "k"
Sleep 0.08s
Type "k"
Sleep 0.08s
Type "k"
Sleep 0.08s
Type "k"
Sleep 0.08s
Type "k"
Sleep 0.08s
Sleep 0.5s

# Open chart (c)
Type "c"
Sleep 2.5s

# Tab to X axis filter, type "date", select date column
Tab
Sleep 0.2s
Tab
Sleep 0.2s
Type "date"
Sleep 0.4s
Tab
Sleep 0.2s
Space
Sleep 0.4s

# Tab to Y axis filter, type "stock", select stock_market
Tab
Sleep 0.2s
Tab
Sleep 0.2s
Type "stock"
Sleep 0.4s
Tab
Sleep 0.2s
Space
Sleep 1.5s

# View the line chart (date vs stock_market)
Sleep 5s

# Back to main table
Escape
Sleep 0.8s

# Open analysis (a)
Type "a"
Sleep 1.5s

# Enter to run Describe (first tool, already focused in sidebar)
Enter
Sleep 0.5s

# View Describe
Sleep 2.5s

# Tab to re-focus sidebar, Down to Distribution Analysis, Enter to run
Tab
Sleep 0.3s
Down
Sleep 0.2s
Enter
Sleep 0.5s

# View Distribution Analysis
Sleep 2.5s

# Tab to sidebar, Down to Correlation Matrix, Enter to run
Tab
Sleep 0.3s
Down
Sleep 0.2s
Enter
Sleep 0.5s

# View Correlation Matrix
Sleep 2.5s

# Back to main table
Escape
Sleep 0.8s

# Open query (/), run a simple query
Type "/"
Sleep 0.5s
Type "select date, stock_market, high_temp where stock_market > 1200"
Sleep 0.3s
Enter
Sleep 1s

# View query results
Sleep 4s

# Quit
Type "q"
