Output demos/13-light-theme.gif
Set FontSize 14
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

# The light set: the same hues, darkened for a white background. The terminal is
# Tokyo Night Day, and COLORFGBG tells datui the background is light, the way a
# terminal that sets it would.
Set Theme { "name": "Tokyo Night Day", "background": "#e1e2e7", "foreground": "#3760bf", "cursor": "#3760bf", "black": "#b4b5b9", "red": "#f52a65", "green": "#587539", "yellow": "#8c6c3e", "blue": "#2e7de9", "magenta": "#9854f1", "cyan": "#007197", "white": "#6172b0", "brightBlack": "#a1a6c5", "brightRed": "#f52a65", "brightGreen": "#587539", "brightYellow": "#8c6c3e", "brightBlue": "#2e7de9", "brightMagenta": "#9854f1", "brightCyan": "#007197", "brightWhite": "#3760bf" }

# Same fixture as the home-screen demo, for the same reason: nothing from the
# recording machine ends up on screen -- HOME included, so no cloud credentials
# are found and no bucket is listed.
Hide
Type "export HOME=/tmp/datui-demo/home COLORFGBG='0;15' DATUI_CACHE_DIR=/tmp/datui-demo/cache XDG_CONFIG_HOME=/tmp/datui-demo/config"
Enter
Type "cd /tmp/datui-demo/quant-research"
Enter
Type "clear"
Enter
Show

Type "datui"
Sleep 0.3s
Enter
Sleep 2.5s

Down
Sleep 0.8s
Down
Sleep 1.2s

# Open it: the table in the light set.
Enter
Sleep 2.5s

Type "j"
Sleep 0.2s
Type "j"
Sleep 0.2s
Type "j"
Sleep 1.5s

# And the sort & filter sidebar.
Type "s"
Sleep 2s
Escape
Sleep 1s

Type "q"
Sleep 0.5s
