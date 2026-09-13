Output demos/07-filtering.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
# Every meteorite in the Meteoritical Society's catalogue, via NASA: 45,716 rows.
Type "datui demo/data/meteorites.parquet"
Enter
Sleep 0.8s
Show
Sleep 1s

# Open Sort & Filter, switch to the Filter tab.
Type "s"
Sleep 1s
Type "l"
Sleep 0.5s

# Tab to the column selector and cycle to mass_g.
Tab
Sleep 0.5s
Type "l"
Sleep 0.4s
Type "l"
Sleep 0.4s
Type "l"
Sleep 0.4s
Type "l"
Sleep 0.6s

# Tab to the operator; cycle to >.
Tab
Sleep 0.5s
Type "l"
Sleep 0.3s
Type "l"
Sleep 0.6s

# Tab to the value.
Tab
Sleep 0.5s
Type "1000000"
Sleep 0.6s

# Tab past the logical operator to Add, and add the filter.
Tab
Sleep 0.5s
Tab
Sleep 0.5s
Enter
Sleep 0.8s

# Tab to Apply.
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
Sleep 1.5s

Enter
Sleep 0.5s

# The 52 meteorites heavier than a tonne.
Sleep 4s

Sleep 1s
Type "q"
