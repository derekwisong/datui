//! Checks on files in the repository rather than on the app: the desktop entry,
//! the release notes' wiring, and retired words. Filter by module, as in
//! `scripts/dev/test.sh integration repo wording::`.

mod desktop_entry;
mod release_notes;
mod wording;
