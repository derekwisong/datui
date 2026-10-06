import contextlib
import io
import tempfile
import unittest
from pathlib import Path

import capture


def a_take(tape_name: str = "teaser", look: capture.Look = capture.NIGHT_MARKET) -> capture.Take:
    root = Path("/scratch/takes") / tape_name
    return capture.Take(capture.TAPES[tape_name], look, root, Path("/scratch/out"))


class ArgumentTest(unittest.TestCase):
    def test_no_tapes_means_every_tape(self) -> None:
        args = capture.parse_args([])
        self.assertEqual(args.tapes, list(capture.TAPES))
        self.assertFalse(args.publish)
        self.assertFalse(args.dry_run)

    def test_tapes_are_picked_by_name(self) -> None:
        args = capture.parse_args(["teaser", "shots-food", "--dry-run"])
        self.assertEqual(args.tapes, ["teaser", "shots-food"])
        self.assertTrue(args.dry_run)

    def test_an_unknown_tape_is_refused(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            capture.parse_args(["no-such-tape"])

    def test_the_default_output_is_scratch_not_the_docs(self) -> None:
        out = capture.parse_args([]).out
        self.assertEqual(out, capture.DEFAULT_OUT.resolve())
        self.assertFalse(out.is_relative_to(capture.REPO))

    def test_out_is_made_absolute(self) -> None:
        args = capture.parse_args(["--out", "rel/dir"])
        self.assertTrue(args.out.is_absolute())

    def test_every_tape_has_a_file(self) -> None:
        for name in capture.TAPES:
            self.assertTrue((capture.DEMOS_DIR / f"{name}.tape").is_file(), name)


class IsolationTest(unittest.TestCase):
    def test_personal_settings_and_credentials_are_dropped(self) -> None:
        source = {
            "PATH": "/usr/bin",
            "HOME": "/home/someone",
            "LANG": "en_US.UTF-8",
            "AWS_PROFILE": "personal",
            "AWS_ACCESS_KEY_ID": "AKIA",
            "AZURE_CLIENT_ID": "secret",
            "GOOGLE_APPLICATION_CREDENTIALS": "/private/key.json",
            "CLOUDSDK_CONFIG": "/private/gcloud",
            "DATUI_LOG": "debug",
            "XDG_CONFIG_HOME": "/home/someone/.config",
            "XDG_RUNTIME_DIR": "/run/user/1000",
            "HISTFILE": "/home/someone/.bash_history",
            "NO_COLOR": "1",
        }
        take = a_take()
        env = capture.take_env(source, take)
        self.assertFalse([k for k in env if k.startswith(("AWS_", "AZURE_", "GOOGLE_", "CLOUDSDK_"))])
        self.assertNotIn("NO_COLOR", env)
        self.assertNotIn("DATUI_LOG", env)
        self.assertEqual(env["HOME"], str(take.home))
        self.assertEqual(env["XDG_CONFIG_HOME"], str(take.root / "config"))
        self.assertEqual(env["XDG_RUNTIME_DIR"], "/run/user/1000")
        self.assertEqual(env["DATUI_CONFIG_DIR"], str(take.config_dir))
        self.assertEqual(env["DATUI_CACHE_DIR"], str(take.cache_dir))
        self.assertEqual(env["HISTFILE"], "/dev/null")
        self.assertEqual(env["PS1"], "$ ")
        self.assertTrue(env["PATH"].startswith(str(take.bin_dir)))
        self.assertEqual(env["LANG"], "en_US.UTF-8")
        for value in env.values():
            self.assertNotIn("/home/someone", value)

    def test_a_shared_cache_replaces_the_fresh_one(self) -> None:
        env = capture.take_env({"PATH": "/usr/bin"}, a_take(), Path("/warm/cache"))
        self.assertEqual(env["DATUI_CACHE_DIR"], "/warm/cache")

    def test_a_non_utf8_locale_becomes_utf8(self) -> None:
        env = capture.take_env({"PATH": "/usr/bin", "LANG": "C", "LC_ALL": "C"}, a_take())
        self.assertEqual(env["LANG"], "C.UTF-8")
        self.assertNotIn("LC_ALL", env)

    def test_the_wrapper_turns_off_cloud_discovery_and_sets_the_theme(self) -> None:
        script = capture.wrapper_script(Path("/build/datui"), capture.GALLERY[2], Path("/take/tmp"))
        self.assertIn("TMPDIR=/take/tmp exec /build/datui -c cloud.discover=false", script)
        self.assertIn("theme.dark=gruvbox-dark", script)
        self.assertTrue(script.rstrip().endswith('"$@"'))

    def test_the_fixture_starts_empty_with_only_the_takes_theme(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            take = capture.Take(capture.TAPES["theme-gallery"], capture.GALLERY[2], root / "take", root / "out")
            capture.build_fixture(take, Path("/build/datui"))
            self.assertEqual(list(take.work.iterdir()), [])
            self.assertEqual(list(take.home.iterdir()), [take.work])
            self.assertEqual([p.name for p in (take.config_dir / "themes").iterdir()], ["gruvbox-dark.toml"])
            self.assertTrue((take.bin_dir / "datui").stat().st_mode & 0o111)


class TapeTest(unittest.TestCase):
    def test_outputs_and_screenshots_are_absolute_and_header_comes_first(self) -> None:
        take = a_take("shots-food")
        text, outputs = capture.prepare_tape('Type "datui"\nScreenshot food-sidebar.png\n', "Set FontSize 18", take)
        self.assertTrue(text.startswith('Output "/scratch/out/review/shots-food.webm"\nSet FontSize 18'))
        self.assertIn('Screenshot "/scratch/out/screenshots/food-sidebar.png"', text)
        self.assertIn(Path("/scratch/out/screenshots/food-sidebar.png"), outputs)

    def test_a_gif_tape_writes_gif_and_webm(self) -> None:
        _, outputs = capture.prepare_tape('Type "datui"\n', "", a_take("teaser"))
        self.assertEqual(outputs[:2], [Path("/scratch/out/teaser.gif"), Path("/scratch/out/teaser.webm")])

    def test_a_gallery_shot_is_named_for_its_theme(self) -> None:
        take = a_take("theme-gallery", capture.GALLERY[1])
        text, outputs = capture.prepare_tape("Screenshot theme.png\n", "", take)
        self.assertIn(Path("/scratch/out/themes/day-market.png"), outputs)
        self.assertIn('"name": "Tokyo Night Day"', text)

    def test_a_tape_may_not_set_its_own_settings_or_outputs(self) -> None:
        for line in ("Set FontSize 30", "Output x.gif", "Source other.tape"):
            with self.assertRaises(ValueError):
                capture.prepare_tape(line + "\n", "", a_take("teaser"))

    def test_the_gallery_runs_once_per_theme(self) -> None:
        takes = capture.plan(["theme-gallery"], Path("/o"), Path("/s"))
        self.assertEqual([t.look.name for t in takes], [look.name for look in capture.GALLERY])
        self.assertEqual(len({t.root for t in takes}), len(takes))

    def test_every_gallery_theme_file_exists(self) -> None:
        for look in capture.GALLERY:
            if look.theme_file:
                self.assertTrue((capture.CONTRIB_THEMES / look.theme_file).is_file(), look.theme_file)

    def test_the_tapes_parse(self) -> None:
        for name, tape in capture.TAPES.items():
            looks = capture.GALLERY if tape.kind == "gallery" else (capture.NIGHT_MARKET,)
            body = (capture.DEMOS_DIR / f"{name}.tape").read_text()
            capture.prepare_tape(body, capture.HEADER.read_text(), a_take(name, looks[0]))


class PublishTest(unittest.TestCase):
    def test_publish_maps_into_demos(self) -> None:
        pairs = dict(capture.publish_pairs(["teaser", "theme-gallery"], Path("/o")))
        self.assertEqual(pairs[Path("/o/teaser.gif")], capture.PUBLISH_DIR / "teaser.gif")
        self.assertEqual(pairs[Path("/o/teaser.png")], capture.PUBLISH_DIR / "teaser.png")
        self.assertEqual(pairs[Path("/o/theme-gallery.png")], capture.PUBLISH_DIR / "theme-gallery.png")

    def test_publish_copies_nothing_when_an_output_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(capture.publish(["teaser"], Path(tmp), dry_run=False), 1)


if __name__ == "__main__":
    unittest.main()
