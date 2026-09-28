#!/usr/bin/env python3
"""Audit the default Unicode glyph set against the fonts terminals actually ship.

Every glyph in `crates/datui-lib/src/glyphs.rs` must be covered by the floor
fonts, because a codepoint the terminal font lacks is worse than absent: an
`Emoji=Yes, Emoji_Presentation=No` codepoint falls back to the *color emoji*
font and renders a blank cell or a clipped blob, and no font the user picks
fixes that (issue #325).

The rules this script enforces:

1. Every codepoint in the UNICODE set exists in JetBrainsMono Nerd Font.
2. No text-presentation emoji codepoint (Emoji=Yes, Emoji_Presentation=No)
   is missing from any installed floor font. Absence anywhere means the
   emoji fallback path, which no user-side font choice can repair.
3. A non-emoji codepoint missing from Liberation Mono or Noto Sans Mono is
   reported as a benign fallback (fontconfig substitutes another *text*
   font), not a failure.
4. The ASCII set contains only ASCII.

Extended fonts (Fira Code, Hack, ...) are reported informationally when
installed. Requires fontconfig (`fc-list`, `fc-query`), so Linux/BSD; on
other platforms run it where fontconfig can see the same font files.

Usage:
    scripts/code/audit_glyphs.py             # audit, human-readable
    scripts/code/audit_glyphs.py --markdown  # emit the coverage table as markdown
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

GLYPHS_RS = Path(__file__).resolve().parents[2] / "crates/datui-lib/src/glyphs.rs"

# The coverage floor: the default set must be complete in the first, and may
# never put an emoji-class codepoint where any of the three lack it. The trio
# spans the common cases: the most popular TUI font, and the two mono fonts
# stock RHEL-alikes and Debian-alikes fall back to.
FLOOR_FONTS = [
    "JetBrainsMono Nerd Font",
    "Liberation Mono",
    "Noto Sans Mono",
]

# Audited when installed; gaps here are reported, not enforced.
EXTENDED_FONTS = [
    "JetBrains Mono",
    "Fira Code",
    "Hack",
    "Cascadia Code",
    "Cascadia Mono",
    "Iosevka",
    "Menlo",
    "SF Mono",
    "Consolas",
    "DejaVu Sans Mono",
]

# BMP ranges of Emoji=Yes and Emoji_Presentation=Yes, from Unicode 15.1
# emoji-data.txt. Text-presentation emoji = EMOJI minus EMOJI_PRESENTATION:
# the codepoints that render as text in a covering font but route to the
# color emoji font when the main font lacks them. Every datui glyph is BMP;
# the parser asserts that, so supplementary-plane ranges are not needed.
EMOJI = [
    (0x00A9, 0x00A9), (0x00AE, 0x00AE), (0x203C, 0x203C), (0x2049, 0x2049),
    (0x2122, 0x2122), (0x2139, 0x2139), (0x2194, 0x2199), (0x21A9, 0x21AA),
    (0x231A, 0x231B), (0x2328, 0x2328), (0x23CF, 0x23CF), (0x23E9, 0x23F3),
    (0x23F8, 0x23FA), (0x24C2, 0x24C2), (0x25AA, 0x25AB), (0x25B6, 0x25B6),
    (0x25C0, 0x25C0), (0x25FB, 0x25FE), (0x2600, 0x2604), (0x260E, 0x260E),
    (0x2611, 0x2611), (0x2614, 0x2615), (0x2618, 0x2618), (0x261D, 0x261D),
    (0x2620, 0x2620), (0x2622, 0x2623), (0x2626, 0x2626), (0x262A, 0x262A),
    (0x262E, 0x262F), (0x2638, 0x263A), (0x2640, 0x2640), (0x2642, 0x2642),
    (0x2648, 0x2653), (0x265F, 0x2660), (0x2663, 0x2663), (0x2665, 0x2666),
    (0x2668, 0x2668), (0x267B, 0x267B), (0x267E, 0x267F), (0x2692, 0x2697),
    (0x2699, 0x2699), (0x269B, 0x269C), (0x26A0, 0x26A1), (0x26A7, 0x26A7),
    (0x26AA, 0x26AB), (0x26B0, 0x26B1), (0x26BD, 0x26BE), (0x26C4, 0x26C5),
    (0x26C8, 0x26C8), (0x26CE, 0x26CF), (0x26D1, 0x26D1), (0x26D3, 0x26D4),
    (0x26E9, 0x26EA), (0x26F0, 0x26F5), (0x26F7, 0x26FA), (0x26FD, 0x26FD),
    (0x2702, 0x2702), (0x2705, 0x2705), (0x2708, 0x270D), (0x270F, 0x270F),
    (0x2712, 0x2712), (0x2714, 0x2714), (0x2716, 0x2716), (0x271D, 0x271D),
    (0x2721, 0x2721), (0x2728, 0x2728), (0x2733, 0x2734), (0x2744, 0x2744),
    (0x2747, 0x2747), (0x274C, 0x274C), (0x274E, 0x274E), (0x2753, 0x2755),
    (0x2757, 0x2757), (0x2763, 0x2764), (0x2795, 0x2797), (0x27A1, 0x27A1),
    (0x27B0, 0x27B0), (0x27BF, 0x27BF), (0x2934, 0x2935), (0x2B05, 0x2B07),
    (0x2B1B, 0x2B1C), (0x2B50, 0x2B50), (0x2B55, 0x2B55), (0x3030, 0x3030),
    (0x303D, 0x303D), (0x3297, 0x3297), (0x3299, 0x3299),
]
EMOJI_PRESENTATION = [
    (0x231A, 0x231B), (0x23E9, 0x23EC), (0x23F0, 0x23F0), (0x23F3, 0x23F3),
    (0x25FD, 0x25FE), (0x2614, 0x2615), (0x2648, 0x2653), (0x267F, 0x267F),
    (0x2693, 0x2693), (0x26A1, 0x26A1), (0x26AA, 0x26AB), (0x26BD, 0x26BE),
    (0x26C4, 0x26C5), (0x26CE, 0x26CE), (0x26D4, 0x26D4), (0x26EA, 0x26EA),
    (0x26F2, 0x26F3), (0x26F5, 0x26F5), (0x26FA, 0x26FA), (0x26FD, 0x26FD),
    (0x2705, 0x2705), (0x270A, 0x270B), (0x2728, 0x2728), (0x274C, 0x274C),
    (0x274E, 0x274E), (0x2753, 0x2755), (0x2757, 0x2757), (0x2795, 0x2797),
    (0x27B0, 0x27B0), (0x27BF, 0x27BF), (0x2B1B, 0x2B1C), (0x2B50, 0x2B50),
    (0x2B55, 0x2B55),
]


def in_ranges(cp: int, ranges: list[tuple[int, int]]) -> bool:
    return any(lo <= cp <= hi for lo, hi in ranges)


def is_text_emoji(cp: int) -> bool:
    return in_ranges(cp, EMOJI) and not in_ranges(cp, EMOJI_PRESENTATION)


def extract_set(source: str, name: str) -> dict[str, str]:
    """Slot -> literal text for one `const NAME: Glyphs = Glyphs { ... };` block."""
    block = re.search(
        rf"const {name}: Glyphs = Glyphs \{{(.*?)\n\}};", source, re.DOTALL
    )
    if not block:
        sys.exit(f"could not find the {name} set in {GLYPHS_RS}")
    slots: dict[str, str] = {}
    # Field values are plain string literals, slices of them, or the wordmark's
    # Some(&[...]); comments in the block never contain a double-quoted string.
    for field in re.finditer(
        r"^\s*(\w+): (.+?),?$", block.group(1), re.MULTILINE
    ):
        slot, value = field.group(1), field.group(2)
        literals = re.findall(r'"((?:[^"\\]|\\.)*)"', value)
        if literals:
            text = "".join(literals)
            slots[slot] = text.replace('\\\\', '\\').replace('\\"', '"')
    return slots


def codepoints_by_slot(slots: dict[str, str]) -> dict[int, set[str]]:
    """Non-ASCII codepoint -> the slots that use it."""
    out: dict[int, set[str]] = {}
    for slot, text in slots.items():
        for ch in text:
            if ord(ch) > 0x7F:
                out.setdefault(ord(ch), set()).add(slot)
    return out


def font_files(family: str) -> list[str]:
    result = subprocess.run(
        ["fc-list", family, "file"], capture_output=True, text=True, check=True
    )
    return [
        line.strip().rstrip(":")
        for line in result.stdout.splitlines()
        if line.strip()
    ]


def font_charset(family: str) -> set[int] | None:
    """Union of the family's variants' charsets, or None when not installed."""
    files = font_files(family)
    if not files:
        return None
    charset: set[int] = set()
    for file in files:
        result = subprocess.run(
            ["fc-query", "--format", "%{charset}\n", file],
            capture_output=True,
            text=True,
            check=True,
        )
        for token in result.stdout.split():
            lo, _, hi = token.partition("-")
            try:
                start = int(lo, 16)
                end = int(hi, 16) if hi else start
            except ValueError:
                continue
            charset.update(range(start, end + 1))
    return charset


def char_name(cp: int) -> str:
    return unicodedata.name(chr(cp), f"U+{cp:04X}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--markdown", action="store_true", help="emit the coverage table as markdown"
    )
    args = parser.parse_args()

    source = GLYPHS_RS.read_text(encoding="utf-8")

    ascii_set = extract_set(source, "ASCII")
    ascii_offenders = {
        slot: text
        for slot, text in ascii_set.items()
        if any(ord(ch) > 0x7F for ch in text)
    }

    unicode_set = extract_set(source, "UNICODE")
    points = codepoints_by_slot(unicode_set)
    if not points:
        sys.exit("parsed no non-ASCII codepoints from the UNICODE set; parser broken?")
    for cp in points:
        assert cp <= 0xFFFF, f"U+{cp:04X} is outside the BMP; extend the emoji tables"

    charsets: dict[str, set[int] | None] = {}
    for family in FLOOR_FONTS + EXTENDED_FONTS:
        charsets[family] = font_charset(family)

    installed = [f for f in FLOOR_FONTS + EXTENDED_FONTS if charsets[f] is not None]
    missing_floor = [f for f in FLOOR_FONTS if charsets[f] is None]

    # The table.
    def mark(family: str, cp: int) -> str:
        cs = charsets[family]
        if cs is None:
            return "–"
        return "✓" if cp in cs else "✗"

    short = {f: f.split()[0] if f != "SF Mono" else "SF" for f in installed}
    rows = []
    for cp in sorted(points):
        rows.append(
            [
                chr(cp),
                f"U+{cp:04X}",
                "emoji" if is_text_emoji(cp) else "",
                " ".join(sorted(points[cp])),
            ]
            + [mark(f, cp) for f in installed]
        )
    header = ["", "cp", "class", "slots"] + [short[f] for f in installed]
    if args.markdown:
        print("| " + " | ".join(header) + " |")
        print("|" + "---|" * len(header))
        for row in rows:
            print("| " + " | ".join(row) + " |")
    else:
        widths = [
            max(len(str(r[i])) for r in [header] + rows) for i in range(len(header))
        ]
        for row in [header] + rows:
            print("  ".join(str(c).ljust(w) for c, w in zip(row, widths)))

    # The verdicts.
    failures: list[str] = []
    benign: dict[str, list[int]] = {}

    for slot, text in ascii_offenders.items():
        failures.append(f"ASCII set slot `{slot}` contains non-ASCII: {text!r}")

    primary = FLOOR_FONTS[0]
    for cp in sorted(points):
        slots = ", ".join(sorted(points[cp]))
        ch = chr(cp)
        cs = charsets[primary]
        if cs is not None and cp not in cs:
            failures.append(
                f"{ch} U+{cp:04X} ({slots}) missing from {primary}: "
                f"{char_name(cp)}"
            )
        for family in FLOOR_FONTS[1:]:
            cs = charsets[family]
            if cs is None or cp in cs:
                continue
            if is_text_emoji(cp):
                failures.append(
                    f"{ch} U+{cp:04X} ({slots}) is a text-presentation emoji "
                    f"missing from {family}: the color-emoji fallback renders "
                    f"a blank or clipped cell there"
                )
            else:
                benign.setdefault(family, []).append(cp)

    print()
    if missing_floor:
        print(f"floor fonts not installed, audit incomplete: {', '.join(missing_floor)}")
    for family, cps in benign.items():
        chars = " ".join(chr(cp) for cp in cps)
        print(
            f"note: {family} lacks {len(cps)} non-emoji codepoints "
            f"(benign text-font fallback): {chars}"
        )
    for failure in failures:
        print(f"FAIL: {failure}")
    if failures or missing_floor:
        return 1
    print(f"ok: {len(points)} codepoints audited against {len(installed)} fonts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
