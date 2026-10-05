# datui — Omarchy theme template
#
# Rendered by `omarchy theme set <name>` to:
#   ~/.local/state/omarchy/current/theme/datui.toml
#
# datui picks it up via its own config:
#   # ~/.config/datui/config.toml
#   import = ["~/.local/state/omarchy/current/theme/datui.toml"]
#
# Anything you set in ~/.config/datui/config.toml overrides what is generated
# here. Caveat: a colour set to the same string as datui's own default cannot
# override an imported value — use an explicit form (e.g. "#ff0000" rather than
# "red") when pinning a colour datui also uses by default.
#
# Derived shades use `{{ mix background foreground N% }}` rather than a fixed
# colour so they track the theme's own contrast direction and stay correct on
# light themes (catppuccin-latte, flexoki-light) as well as dark ones.

# Follow the theme's own light/dark polarity. This picks datui's base palette, so
# any colour slot this template does not set still lands on the right side of the
# light/dark divide -- including slots added by a future datui release.
[theme]
mode = "{{ mode }}"

[theme.colors]

# --- Surfaces --------------------------------------------------------------
background   = "{{ background }}"
surface      = "{{ lighter_background }}"
controls_bg  = "{{ dark_background }}"

# --- Text ------------------------------------------------------------------
text_primary   = "{{ foreground }}"
text_secondary = "{{ mix background foreground 65% }}"
text_inverse   = "{{ background }}"
dimmed         = "{{ muted }}"

# --- Chrome ----------------------------------------------------------------
accent           = "{{ accent }}"
accent_bright    = "{{ mix accent foreground 30% }}"
gradient_start   = "{{ blue }}"
gradient_end     = "{{ magenta }}"
chip_key         = "{{ accent }}"
chip_label       = "{{ light_foreground }}"
throbber         = "{{ accent }}"
sidebar_border   = "{{ mix background foreground 25% }}"

# --- Table -----------------------------------------------------------------
table_header           = "{{ bright_foreground }}"
table_header_bg        = "{{ mix background foreground 12% }}"
table_alternate_row    = "{{ mix background foreground 6% }}"
table_row_numbers      = "{{ muted }}"
table_column_separator = "{{ mix background foreground 25% }}"
# A tint under the current row. "reversed" swaps fg/bg instead.
table_selected         = "{{ mix background accent 30% }}"
find_match             = "{{ yellow }}"
# The column cursor's cells, and its header and the current cell.
table_column_cursor    = "{{ mix background foreground 9% }}"
table_cell_cursor      = "{{ mix background foreground 22% }}"

# --- Text caret / modals ---------------------------------------------------
input_cursor        = "{{ accent }}"
# Text under the caret block; the background reads on an accent caret.
input_cursor_text   = "{{ background }}"
modal_border_active = "{{ accent }}"
modal_border_error  = "{{ red }}"

# --- Status ----------------------------------------------------------------
success = "{{ green }}"
error   = "{{ red }}"
warning = "{{ yellow }}"

# --- Column types ----------------------------------------------------------
# These must stay mutually distinguishable: they are how you read a schema at a
# glance. Mapped to the theme's six hues rather than derived shades.
type_str      = "{{ green }}"
type_int      = "{{ cyan }}"
type_float    = "{{ blue }}"
type_bool     = "{{ yellow }}"
type_temporal = "{{ magenta }}"
type_binary   = "{{ muted }}"

# --- Distribution / outliers ----------------------------------------------
distribution_normal = "{{ green }}"
distribution_skewed = "{{ yellow }}"
distribution_other  = "{{ foreground }}"
outlier_marker      = "{{ red }}"

# --- Charts ----------------------------------------------------------------
# chart_1 is also histogram bars and Q-Q points; overlays take `dimmed`.
chart_1 = "{{ accent }}"
chart_2 = "{{ magenta }}"
chart_3 = "{{ green }}"
chart_4 = "{{ yellow }}"
chart_5 = "{{ blue }}"
chart_6 = "{{ red }}"
chart_7 = "{{ orange }}"
# Cyan, then lighter shades of two hues above. A slot that comes out the same
# color as an earlier one (cyan as the accent) is skipped, never drawn twice.
chart_8  = "{{ cyan }}"
chart_9  = "{{ mix magenta foreground 45% }}"
chart_10 = "{{ mix yellow foreground 45% }}"
# The grid sits a shade under muted, so it never competes with a series.
chart_grid = "{{ mix background foreground 30% }}"

# --- Hex view --------------------------------------------------------------
# Bytes by class, as hexyl colors them.
hex_null       = "{{ muted }}"
hex_printable  = "{{ cyan }}"
hex_whitespace = "{{ green }}"
hex_control    = "{{ magenta }}"
hex_high       = "{{ yellow }}"
hex_ff         = "{{ red }}"
