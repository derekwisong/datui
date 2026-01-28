Output demos/03-info.gif
Set FontSize 18
Set Width 100
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE
Hide
Type "datui tests/sample-data/large_dataset.parquet"
Enter
Sleep 0.5s
Show
Sleep 1s

# open info panel
Type "i"
Sleep 1s

# scroll the schema table
Type "j"
Sleep 0.1s
Type "j"
Sleep 0.1s
Type "j"
Sleep 0.1s
Type "j"
Sleep 0.1s
Type "k"
Sleep 0.1s
Type "k"
Sleep 0.1s
Type "k"
Sleep 0.1s
Type "k"
Sleep 1s

# select tab bar and move to the resources tab
Tab
Sleep 1s
Type "l"
# wait a moment and exit
Sleep 5s
# exit
Escape
# sleep here helps the gif look like a good loop
Sleep 1s