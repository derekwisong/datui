Output demos/10-charting.gif
Set FontSize 18
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui demo/data/bitcoin_daily.parquet"
Enter
Sleep 0.8s
Show
Sleep 2s

# Open charting.
Type "c"
Sleep 3s

# Tab past the plot style to the X axis filter; pick date.
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Type "date"
Sleep 1s
Tab
Sleep 0.5s
Space
Sleep 0.5s

# Tab to the Y axis filter; pick tx_count.
Tab
Sleep 0.5s
Type "tx_count"
Sleep 0.5s
Tab
Sleep 1s
Space
Sleep 1s

# Transactions per day, 2009 to now.
Sleep 3s

# Export the chart as a PNG.
Type "e"
Sleep 1s
Type "/tmp/datui_chart.png"
Sleep 1s
Enter
Sleep 2s
Enter
Sleep 1s

Escape
Sleep 2s
