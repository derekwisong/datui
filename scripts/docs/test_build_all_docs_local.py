"""A failed book must not become the latest release or a cache hit."""
import contextlib
import io
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import build_all_docs_local as builder


class BuildTests(unittest.TestCase):
    def exercise(self, root, fail=False):
        def build(command, **kwargs):
            if not fail:
                output = root / 'book/v1.0.0'
                output.mkdir(parents=True, exist_ok=True)
                (output / 'index.html').write_text('built book')
            return subprocess.CompletedProcess(command, 1 if fail else 0)

        with patch.object(builder, 'get_repo_root', return_value=root), \
             patch.object(builder, 'get_version_tags', return_value=['v1.0.0']), \
             patch.object(builder, 'get_tag_sha', return_value='abc123'), \
             patch.object(builder, 'run', return_value=subprocess.CompletedProcess([], 0)), \
             patch.object(builder.subprocess, 'run', side_effect=build) as builds, \
             patch.object(builder.sys.stdin, 'isatty', return_value=False), \
             contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            status = builder.main()
        return status, builds.call_count

    def test_failed_build_preserves_latest_and_is_not_cached(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            latest = root / 'book/latest'
            latest.mkdir(parents=True)
            (latest / 'index.html').write_text('previous release')
            status, _ = self.exercise(root, fail=True)
            self.assertEqual(status, 1)
            self.assertFalse((root / 'book/v1.0.0/.built_sha').exists())
            self.assertEqual((latest / 'index.html').read_text(), 'previous release')

    def test_successful_build_is_reused_only_while_book_exists(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertEqual(self.exercise(root), (0, 1))
            self.assertEqual((root / 'book/latest/index.html').read_text(), 'built book')
            self.assertEqual(self.exercise(root), (0, 0))
            (root / 'book/v1.0.0/index.html').unlink()
            self.assertEqual(self.exercise(root), (0, 1))


if __name__ == '__main__':
    unittest.main()
