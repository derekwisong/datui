Output demos/12-home-screen.gif
Set FontSize 14
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

# Isolated fixture from make-home-fixture.py. vhs 0.11 has no Set Env.
Hide
Type "export HOME=/tmp/datui-demo/home DATUI_CACHE_DIR=/tmp/datui-demo/cache XDG_CONFIG_HOME=/tmp/datui-demo/config"
Enter
Type "cd /tmp/datui-demo/data/quant-research"
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
Type "fact"
Sleep 2.5s
Type "or"
Sleep 2.5s

# Column search.
Ctrl+U
Sleep 0.8s
Type "ticker"
Sleep 2.5s

# Open one.
Ctrl+U
Sleep 0.8s
Type "returns"
Sleep 1.5s
Enter
Sleep 3s

Type "q"
Sleep 1s
