//! The component kit: the pieces every surface is built from.
//!
//! One Surface per modal or sidebar, FormRows inside it, a Picker for pick-one
//! lists, a HintBar for the keys, a SectionRule to divide space without borders,
//! Working for a wait said where its result will appear.
//! The canon these implement is the `ui-style` skill; a screen that hand-rolls
//! one of these is a migration target.

mod form_row;
mod hintbar;
mod picker;
mod section_rule;
mod surface;
mod working;

pub use form_row::{FormRow, FormValue};
pub use hintbar::HintBar;
pub use picker::{Clicks, Picker, PickerState};
pub use section_rule::SectionRule;
pub use surface::Surface;
pub use working::Working;

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    /// Source files outside the kit that may still draw a frame of their own,
    /// relative to `src/`. Each needs a comment at the frame saying why a
    /// Surface cannot express it.
    const ALLOWED: &[&str] = &[];

    /// What a hand-built frame looks like in source. `Borders::NONE` draws
    /// nothing and is not one.
    const FRAMES: &[&str] = &[
        "Borders::",
        "Block::bordered",
        ".border_set(",
        ".border_type(",
    ];

    fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("source dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// Line number and text of every line in `source` that builds a frame.
    fn frames_in(source: &str) -> Vec<(usize, &str)> {
        source
            .lines()
            .enumerate()
            .filter(|(_, line)| {
                let code = line.trim_start().replace("Borders::NONE", "");
                !code.starts_with("//") && FRAMES.iter().any(|frame| code.contains(frame))
            })
            .map(|(i, line)| (i + 1, line.trim()))
            .collect()
    }

    /// Hard rule 1 of the canon: a surface gets one frame, and only Surface
    /// draws it. #368, #369 and #370 moved every hand-drawn frame onto it;
    /// this keeps new ones from coming back.
    #[test]
    fn only_the_surface_draws_a_frame() {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let kit = src.join("widgets").join("ui");
        let mut files = Vec::new();
        rust_files(&src, &mut files);
        for dir in ["render", "widgets"] {
            assert!(
                files.iter().any(|path| path.starts_with(src.join(dir))),
                "the scan reads src/{dir}"
            );
        }
        let mut found = Vec::new();
        for path in files.iter().filter(|path| !path.starts_with(&kit)) {
            let name = path
                .strip_prefix(&src)
                .expect("under src")
                .to_string_lossy()
                .replace('\\', "/");
            let source = std::fs::read_to_string(path).expect("source file");
            let frames = frames_in(&source);
            if ALLOWED.contains(&name.as_str()) {
                assert!(
                    !frames.is_empty(),
                    "{name} is allowed a frame it no longer draws"
                );
                continue;
            }
            found.extend(
                frames
                    .into_iter()
                    .map(|(line, code)| format!("  {name}:{line}: {code}")),
            );
        }
        assert!(
            found.is_empty(),
            "draw these frames with widgets::ui::Surface:\n{}",
            found.join("\n")
        );
    }

    /// The scan sees a frame, and not a borderless block or a comment.
    #[test]
    fn the_frame_scan_reads_code_only() {
        let source = [
            "let a = Block::default().borders(Borders::ALL);",
            "let b = Block::bordered();",
            "let c = Block::default().borders(Borders::NONE);",
            "// Borders::ALL was here once.",
            "let d = Block::default().borders(Borders::TOP | Borders::BOTTOM);",
        ]
        .join("\n");
        let lines: Vec<usize> = frames_in(&source).into_iter().map(|(n, _)| n).collect();
        assert_eq!(lines, [1, 2, 5]);
    }
}
