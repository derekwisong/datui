# Release notes

Optional hand-written notes, one file per release, named `v<version>.md` to
match the git tag.

`release.yml` composes the GitHub release body before creating the release. It
uses the file for the tag when there is one, and otherwise generates a body from
the commit subjects since the previous tag. So the body is never empty, and
nothing here is ever required.

## Why the timing matters

`publish-packages.yml` starts about a minute after the release is created, and
komac copies the release body into the winget manifest as `ReleaseNotes`. The
body has to be right at that moment.

Editing the release page afterwards fixes what people read on GitHub and nothing
else. Winget already has whatever the body said. Version 0.3.1 went to winget
with no `ReleaseNotes` at all, because the notes were written six minutes after
komac had run.

That is the reason the body is composed in the workflow rather than typed in
later, and the reason a file here has to be committed with the release rather
than added afterwards.

## Writing them

```bash
python scripts/bump_version.py notes    # scaffolds the file from the commit log
```

Then edit it and commit it with the release, which `bump_version.py release
--commit` does for you. Write for someone deciding whether to upgrade.

Skipping this is fine. A release with no file here gets the generated body,
which is plainer but accurate and on time. The release commands only stop you
for a file that exists but still has its `TODO` line, or one you wrote and
forgot to commit before tagging.
