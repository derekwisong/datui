Output demos/12-home-screen.gif
Set FontSize 14
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

# Isolated fixture and environment from generate_demos.py.
Hide
Type "cd /tmp/datui-demo/data"
Enter
Type "clear"
Enter
Show

Type "datui"
Sleep 0.3s
Enter
Sleep 2.5s

# Details follow the cursor.
Down
Sleep 1.2s
Down
Sleep 1.2s
Down
Sleep 1.8s

# Fuzzy filter.
Type "park"
Sleep 2.5s
Type "_weather"
Sleep 2.5s

# Column search.
Ctrl+U
Sleep 0.8s
Type "species"
Sleep 2.5s

# Open one.
Enter
Sleep 3s

Type "q"
Sleep 1s
