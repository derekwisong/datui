Output demos/12-home-screen.gif
Set FontSize 14
Set Width 1200
Set Height 800
Set TypingSpeed 25ms
# DO NOT DELETE - HEADER ABOVE

# Everything the home screen shows comes from the machine it runs on, so the demo
# gets its own cache, config and workspace. Built by make-home-fixture.py, which the
# generator runs first. Set here rather than with `Set Env`, which vhs 0.11 lacks.
Hide
Type "export DATUI_CACHE_DIR=/tmp/datui-demo/cache XDG_CONFIG_HOME=/tmp/datui-demo/config"
Enter
Type "cd /tmp/datui-demo/quant-research"
Enter
Type "clear"
Enter
Show

# Open with no arguments: the home screen is what datui is when you have not told it
# what to look at yet.
Type "datui"
Sleep 0.3s
Enter
Sleep 2.5s

# Move down the list. The details pane follows the cursor: source filesystem, rows,
# columns, what it weighs on disk and once open, and how it is partitioned.
Down
Sleep 1.2s
Down
Sleep 1.2s
Down
Sleep 1.8s

# Type to filter. Matching is fuzzy and the matched characters are underlined.
Type "fact"
Sleep 2.5s
Type "or"
Sleep 2.5s

# Column search: a dataset matches on what is inside it, not just its name.
Ctrl+U
Sleep 0.8s
Type "ticker"
Sleep 2.5s

# Clear and open one.
Ctrl+U
Sleep 0.8s
Type "prices"
Sleep 1.5s
Enter
Sleep 3s

Type "q"
Sleep 1s
