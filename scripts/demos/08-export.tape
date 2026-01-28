Output demos/08-export.gif
Set FontSize 18
Set Width 1500
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

Hide
Type "datui tests/sample-data/people.parquet"
Enter
Sleep 0.5s
Show
Sleep 1s

# Open export (e); focus starts on Path input, format defaults to Parquet for .parquet
Type "e"
Sleep 1s

# Type export path in /tmp
Type "/tmp/people_export.parquet"
Sleep 0.5s

# Tab to Export button (PathInput -> ExportButton for Parquet)
Tab
Sleep 0.5s

# Pause before pressing Export
Sleep 1.5s

# Press Export
Enter
Sleep 2s
Type "q"
