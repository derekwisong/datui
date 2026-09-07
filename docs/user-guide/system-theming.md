# Theming datui from your system

datui can follow a theme that something else on your machine generates — a desktop
theme manager, chezmoi, home-manager, or a dotfiles repo. It does this with one
generic mechanism, [`import`](configuration.md#importing-other-config-files), and
knows nothing about any particular system.

The pattern is the same one Alacritty uses (`general.import`) and btop uses
(`color_theme = "current"`): the app exposes an include, and the theme system
points it at a generated file.

## Light and dark terminals

Before anything system-specific, one setting matters on its own.

Some of datui's colours — header fills, alternating row stripes, borders, dim text
— need to sit *near* the terminal background without matching it. No ANSI colour
means "slightly off from the background", so those slots resolve to fixed shades,
and a set tuned for a dark terminal is unreadable on a light one.

```toml
[theme]
mode = "auto"   # "auto" (default), "dark", or "light"
```

`auto` reads the `COLORFGBG` environment variable and falls back to `dark`.
Alacritty, Kitty and Ghostty do not set that variable, so **if you use a light
terminal colour scheme in one of those, set `mode = "light"` explicitly.**

Individual colours always override whichever set is chosen, so `mode` only decides
the starting point.

## Omarchy

[Omarchy](https://omarchy.org/) renders per-app theme files from templates whenever
you run `omarchy theme set`. datui ships a template for it.

### Setup

1. Install the template:

   ```bash
   mkdir -p ~/.config/omarchy/themed
   cp contrib/omarchy/datui.toml.tpl ~/.config/omarchy/themed/
   ```

2. Point datui at the file Omarchy will generate:

   ```toml
   # ~/.config/datui/config.toml
   import = ["~/.local/state/omarchy/current/theme/datui.toml"]
   ```

3. Apply a theme:

   ```bash
   omarchy theme set tokyo-night
   ```

`omarchy theme set` now restyles datui along with everything else. The template
also sets `theme.mode` from the theme's own light/dark polarity, so light themes
get light chrome automatically.

A theme switch takes effect the next time datui starts; a running instance keeps
the theme it launched with.

### Customising

Your own `config.toml` wins over anything imported, so the simplest override is a
normal config entry:

```toml
import = ["~/.local/state/omarchy/current/theme/datui.toml"]

[theme.colors]
int_col = "#ff8800"     # applies on every theme
```

Watch out for one trap: a colour set to *the same string as datui's default* is
indistinguishable from one that was never set, so it will not override an import.
See [the caveat in the configuration
guide](configuration.md#a-caveat-when-overriding-an-imported-color).

#### Per-theme overrides

To change colours for one theme only, put an extra file in that theme's directory
and import it after the generated one:

```toml
# ~/.config/datui/config.toml
import = [
  "~/.local/state/omarchy/current/theme/datui.toml",
  "~/.local/state/omarchy/current/theme/datui.override.toml",
]
```

```toml
# ~/.config/omarchy/themes/osaka-jade/datui.override.toml
[theme.colors]
int_col   = "#ff8800"
float_col = "#ff00ff"
```

Omarchy copies your theme directory into the theme state directory before it
renders templates, so the file arrives untouched. Themes without an override file
simply lack it, and datui skips a missing import with a warning.

> **Name it `datui.override.toml`, not `datui.toml`.** A file named `datui.toml`
> *replaces* the generated one instead of layering on top of it — the renderer skips
> any output that already exists — so you would lose every colour you did not
> restate.

### What to expect

The chrome — backgrounds, borders, header fills, row striping — follows the theme
closely, and this is where nearly all the visible difference comes from.

Column-type colours will look much the same as they do untheme'd. datui's defaults
for those are ANSI names, and your terminal's ANSI palette is themed from the same
source, so they already tracked the theme before you installed anything. On
near-monochrome themes several column types will look alike; that is the theme's
palette, not a datui setting, and the per-theme override above is the way out.

## Other systems

Nothing above is Omarchy-specific except the paths. Any tool that can write a TOML
file works:

```toml
# chezmoi, home-manager, stow, a Makefile, anything
import = ["~/.local/share/mytheme/datui.toml"]
```

The imported file uses datui's own config format, so it may set any section, not
just `[theme.colors]`.

## See Also

- [Configuration](configuration.md) — the full config format, including `import`
  precedence rules
