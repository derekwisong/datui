Output demos/14-cloud-home.gif
Set FontSize 14
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

# Headline tour: home, public cloud data, a remote table, then a local recent.
Hide
Type "cd /tmp/datui-demo/data"
Enter
Type "clear"
Enter
Show

Type "datui"
Enter
Sleep 3s

# The cloud source is filterable alongside local files and recents.
Type "public"
Sleep 1.5s
Enter
Sleep 2s

# Browse a publisher-maintained public dataset without credentials.
Type "bigquery"
Sleep 1.5s
Enter
Sleep 6s
Type "states"
Sleep 1.2s
Enter
Sleep 5s
Type "parquet"
Sleep 1.2s
Enter
Sleep 8s

# The small remote Parquet file opens as a normal table.
Sleep 3s
Ctrl+O
Sleep 2s
Ctrl+U
Sleep 0.8s
Escape
Sleep 0.8s
Escape
Sleep 0.8s
Escape
Sleep 2s

# Local files and cloud objects share the same home screen.
Type "penguins"
Sleep 1.5s
Enter
Sleep 4s
Down
Down
Down
Sleep 2s

Type "q"
