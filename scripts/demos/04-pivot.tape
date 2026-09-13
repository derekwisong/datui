Output demos/04-pivot.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
# Gapminder: life expectancy, population and GDP per capita, one row per country and year.
Type "datui demo/data/gapminder.parquet"
Enter
Sleep 0.8s
Show
Sleep 1s

# Open Pivot & Melt.
Type "p"
Sleep 1s

# Tab past the filter to the index column list.
Tab
Sleep 0.5s
Tab
Sleep 0.5s

# Index on country and continent. Space toggles a column.
Type " "
Sleep 0.5s
Down
Sleep 0.5s
Type " "
Sleep 0.8s

# Pivot column defaults to year and value column to life_exp: one column per year.
# Tab through them to Apply.
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

# One row per country, one column per year.
Sleep 5s

Sleep 1s
Type "q"
