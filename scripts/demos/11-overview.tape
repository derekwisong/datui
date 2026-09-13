Output demos/11-overview.gif
Set FontSize 18
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
# Central Park daily weather since 1869: 57,596 days.
Type "datui demo/data/central_park_weather.parquet"
Enter
Sleep 0.8s
Show
Sleep 2s

# Scroll down.
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

# And back up.
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

# Query down to the last decade.
Type "/"
Sleep 0.5s
Type "select date, tmax_c, tmin_c, precip_mm, snow_mm where date >= 2016.01.01"
Sleep 0.3s
Enter
Sleep 2.5s

# Chart it: date on X, tmax_c on Y.
Type "c"
Sleep 2.5s
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
Tab
Sleep 0.2s
Tab
Sleep 0.2s
Type "tmax"
Sleep 0.4s
Tab
Sleep 0.2s
Space
Sleep 1.5s

# Ten summers and ten winters.
Sleep 5s

Escape
Sleep 0.8s

# Analysis: describe, distribution, correlation.
Type "a"
Sleep 1.5s
Enter
Sleep 0.5s
Sleep 2.5s

Tab
Sleep 0.3s
Down
Sleep 0.2s
Enter
Sleep 0.5s
Sleep 2.5s

Tab
Sleep 0.3s
Down
Sleep 0.2s
Enter
Sleep 0.5s
Sleep 2.5s

Escape
Sleep 2s

Type "q"
