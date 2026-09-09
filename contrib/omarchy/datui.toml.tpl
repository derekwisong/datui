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
keybind_hints    = "{{ accent }}"
keybind_labels   = "{{ light_foreground }}"
throbber         = "{{ accent }}"
sidebar_border   = "{{ mix background foreground 25% }}"
column_separator = "{{ mix background foreground 25% }}"

# --- Table -----------------------------------------------------------------
table_header        = "{{ bright_foreground }}"
table_header_bg     = "{{ mix background foreground 12% }}"
alternate_row_color = "{{ mix background foreground 6% }}"
row_numbers         = "{{ muted }}"
# "reversed" swaps fg/bg at render time, so it follows any theme for free.
table_selected      = "reversed"

# --- Cursor / modals -------------------------------------------------------
cursor_focused      = "{{ accent }}"
cursor_dimmed       = "{{ muted }}"
modal_border_active = "{{ accent }}"
modal_border_error  = "{{ red }}"

# --- Status ----------------------------------------------------------------
success = "{{ green }}"
error   = "{{ red }}"
warning = "{{ yellow }}"

# --- Column types ----------------------------------------------------------
# These must stay mutually distinguishable: they are how you read a schema at a
# glance. Mapped to the theme's six hues rather than derived shades.
str_col      = "{{ green }}"
int_col      = "{{ cyan }}"
float_col    = "{{ blue }}"
bool_col     = "{{ yellow }}"
temporal_col = "{{ magenta }}"
binary_col   = "{{ muted }}"

# --- Distribution / outliers ----------------------------------------------
distribution_normal = "{{ green }}"
distribution_skewed = "{{ yellow }}"
distribution_other  = "{{ foreground }}"
outlier_marker      = "{{ red }}"

# --- Charts ----------------------------------------------------------------
primary_chart_series_color   = "{{ accent }}"
secondary_chart_series_color = "{{ muted }}"
chart_series_color_1 = "{{ blue }}"
chart_series_color_2 = "{{ green }}"
chart_series_color_3 = "{{ yellow }}"
chart_series_color_4 = "{{ magenta }}"
chart_series_color_5 = "{{ cyan }}"
chart_series_color_6 = "{{ orange }}"
chart_series_color_7 = "{{ red }}"
