Output demos/06-sorting.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
# Every magnitude 6+ earthquake since 1900, from the USGS catalogue.
Type "datui demo/data/earthquakes_m6.parquet"
Enter
Sleep 0.8s
Show
Sleep 1s

# Open Sort & Filter.
Type "s"
Sleep 1s

# Tab past the filter to the column list.
Tab
Sleep 0.5s
Tab
Sleep 0.5s

# Down to magnitude.
Down
Sleep 0.4s
Down
Sleep 0.4s
Down
Sleep 0.4s
Down
Sleep 0.4s
Down
Sleep 0.4s

# Add it to the sort.
Type " "
Sleep 0.8s

# Tab to the order and flip it to descending.
Tab
Sleep 0.5s
Type " "
Sleep 0.8s

# Tab to Apply.
Tab
Sleep 0.5s
Sleep 1.5s

Enter
Sleep 0.5s

# The biggest earthquakes since 1900: Chile 1960, Alaska 1964, Sumatra 2004, Tohoku 2011.
Sleep 5s

Sleep 1s
Type "q"
