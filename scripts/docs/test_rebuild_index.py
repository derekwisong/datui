"""Check navigation for the build layouts the landing page supports."""
import contextlib
import io
import os
import re
import tempfile
import unittest
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch

import rebuild_index


class Page(HTMLParser):
    def __init__(self, content):
        super().__init__()
        self.links = []
        self.refreshes = []
        self.feed(content)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a":
            self.links.append(attrs.get("href", ""))
        if tag == "meta" and attrs.get("http-equiv", "").lower() == "refresh":
            self.refreshes.append(attrs)


class LandingPageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.book = self.root / "book"
        self.book.mkdir()

    def build(self, name, demo=False):
        path = self.book / name
        path.mkdir(parents=True)
        (path / "index.html").write_text("<h1>Book</h1>")
        if demo:
            (path / "demos").mkdir()
            (path / "demos/teaser.gif").write_bytes(b"GIF89a")
            (path / "demos/teaser.png").write_bytes(b"\x89PNG")
        return path

    def render(self):
        cwd = Path.cwd()
        try:
            with patch.object(rebuild_index, "get_repo_root", return_value=self.root), \
                 patch.object(rebuild_index, "check_git_ref_exists", return_value=False), \
                 contextlib.redirect_stdout(io.StringIO()):
                rebuild_index.main()
        finally:
            os.chdir(cwd)
        content = (self.book / "index.html").read_text()
        page = Page(content)
        self.assertEqual(page.refreshes, [])
        self.assertIn('id="install"', content)
        self.assertIn('id="versions"', content)
        self.assertNotIn('href="/datui/', content)
        return content, page

    def test_no_books_links_to_source_docs(self):
        content, page = self.render()
        self.assertIn("https://github.com/derekwisong/datui/tree/main/docs/", page.links)
        self.assertNotIn('id="teaser"', content)
        self.assertIn("CAPTURE PLACEHOLDER", content)
        self.assertFalse(any(link.startswith("latest/") for link in page.links))

    def test_tag_without_alias_links_to_the_existing_tag(self):
        self.build("v0.3.2", demo=True)
        content, page = self.render()
        self.assertIn("v0.3.2/", page.links)
        self.assertIn("v0.3.2/getting-started/quick-start.html", page.links)
        self.assertIn('data-src="v0.3.2/demos/teaser.gif"', content)
        self.assertIn('src="v0.3.2/demos/teaser.png"', content)
        self.assertFalse(any(link.startswith("latest/") for link in page.links))

    def test_latest_alias_is_primary_but_not_a_duplicate_version(self):
        self.build("v0.3.2")
        self.build("latest", demo=True)
        content, page = self.render()
        self.assertIn("latest/getting-started/installation.html", page.links)
        self.assertIn("v0.3.2/", page.links)
        self.assertNotIn(">latest</a>", content)

    def test_development_only_including_nested_branch_names(self):
        self.build("docs/revision")
        _, page = self.render()
        self.assertIn("docs/revision/getting-started/quick-start.html", page.links)
        self.assertIn("docs/revision/", page.links)
        self.build("main")
        _, page = self.render()
        self.assertIn("main/getting-started/quick-start.html", page.links)

    def test_release_order_and_older_versions(self):
        for version in ["v0.2.8", "v0.2.9", "v0.2.10", "v0.3.0", "v0.3.1", "v0.3.2"]:
            self.build(version)
        recent, older, latest = rebuild_index.collect_versions(self.book)
        self.assertEqual(latest, "v0.3.2")
        self.assertEqual([v["name"] for v in recent], ["v0.3.2", "v0.3.1", "v0.3.0", "v0.2.10", "v0.2.9"])
        self.assertEqual([v["name"] for v in older], ["v0.2.8"])
        content, _ = self.render()
        self.assertIn("Older releases", content)

    def test_incomplete_builds_assets_and_prereleases_are_not_latest(self):
        self.build("v0.3.2")
        self.build("v0.4.0-rc.1")
        for name in ["v99.0.0", "demos", "assets"]:
            (self.book / name).mkdir()
        recent, _, latest = rebuild_index.collect_versions(self.book)
        self.assertEqual(latest, "v0.3.2")
        self.assertEqual({v["name"] for v in recent}, {"v0.3.2", "v0.4.0-rc.1"})

    def test_nested_release_name_is_a_branch_and_chapter_indexes_are_ignored(self):
        self.build("docs/v1.0.0")
        self.build("docs/v1.0.0/examples")
        recent, _, latest = rebuild_index.collect_versions(self.book)
        self.assertIsNone(latest)
        self.assertEqual([v["path"] for v in recent], ["docs/v1.0.0"])
        _, page = self.render()
        self.assertIn("docs/v1.0.0/", page.links)

    def test_version_names_are_html_escaped(self):
        self.build('docs-"example"')
        content, page = self.render()
        self.assertIn('docs-"example"/', page.links)
        self.assertNotIn('href="docs-"example"', content)

    def test_teaser_is_opt_in_and_absent_when_not_built(self):
        path = self.build("main", demo=True)
        content, _ = self.render()
        self.assertIn('id="teaser"', content)
        self.assertIn('data-src="main/demos/teaser.gif"', content)
        self.assertNotIn("CAPTURE PLACEHOLDER", content)
        (path / "demos/teaser.png").unlink()
        content, _ = self.render()
        self.assertNotIn('id="teaser"', content)

    def test_generated_parts_are_filled_and_every_command_is_labelled(self):
        content, _ = self.render()
        self.assertRegex(content, r"\d+ formats")
        self.assertNotIn("{{", content)
        for channel in ["script", "winget", "brew", "pip", "cargo", "aur", "apt", "binaries"]:
            self.assertIn(f'id="install-{channel}"', content)
        pres = re.findall(r"<pre\b[^>]*>", content)
        self.assertTrue(pres)
        for pre in pres:
            self.assertIn("data-example=", pre)
        for phrase in ["opens anything", "any data file", "any format"]:
            self.assertNotIn(phrase, content.lower())

if __name__ == "__main__":
    unittest.main()
