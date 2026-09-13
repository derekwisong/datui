Output demos/05-melt.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
# Central Park daily weather since 1869, one column per measurement.
Type "datui demo/data/central_park_weather.parquet"
Enter
Sleep 0.8s
Show
Sleep 1s

# Open Pivot & Melt, switch to the Melt tab.
Type "p"
Sleep 1s
Right
Sleep 0.5s

# Tab past the filter to the index column list; keep date.
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Type " "
Sleep 0.8s

# Tab through the strategy, pattern, type, list and name fields to Apply.
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Sleep 1.5s

Enter
Sleep 0.5s

# Every measurement is now a row: date, variable, value.
Sleep 5s

Sleep 1s
Type "q"
