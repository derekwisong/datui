//! `datui cache`, `datui views` and `datui man`: commands that open no data.

use datui_cli::{CacheAction, ViewsAction};

use crate::cache::CacheManager;
use crate::config::ConfigManager;
use crate::view::{MatchCriteria, ViewManager};

/// What `datui cache ACTION` prints, and its exit code.
pub fn cache(cache: Option<&CacheManager>, action: &CacheAction) -> (String, i32) {
    let CacheAction::Clear { recents } = action;
    let Some(cache) = cache else {
        return ("No cache to clear\n".into(), 0);
    };
    if *recents {
        cache.clear_recents();
        return ("Recently opened datasets forgotten\n".into(), 0);
    }
    match cache.clear_all() {
        Ok(()) => (format!("Cleared {}\n", cache.cache_dir().display()), 0),
        Err(e) => (format!("{e}\n"), 1),
    }
}

/// What `datui views ACTION` prints, and its exit code.
pub fn views(config: &ConfigManager, action: &ViewsAction) -> (String, i32) {
    let mut views = match ViewManager::new(config) {
        Ok(views) => views,
        Err(e) => return (format!("{e}\n"), 1),
    };
    match action {
        ViewsAction::List => {
            let all = views.all_views();
            if all.is_empty() {
                return ("No saved views\n".into(), 0);
            }
            let width = all
                .iter()
                .map(|v| v.name.chars().count())
                .max()
                .unwrap_or(0);
            let mut out = String::new();
            for view in all {
                out.push_str(&format!(
                    "{:<width$}  {}\n",
                    view.name,
                    matches(&view.match_criteria)
                ));
            }
            (out, 0)
        }
        ViewsAction::Rm { name } => {
            let Some(id) = views.get_view_by_name(name).map(|v| v.id.clone()) else {
                return (
                    format!("No saved view named \"{name}\"; `datui views list` lists them\n"),
                    1,
                );
            };
            match views.delete_view(&id) {
                Ok(()) => (format!("Removed {name}\n"), 0),
                Err(e) => (format!("{e}\n"), 1),
            }
        }
        ViewsAction::Clear => {
            let count = views.all_views().len();
            match views.remove_all_views() {
                Ok(()) => (format!("Removed {count} saved views\n"), 0),
                Err(e) => (format!("{e}\n"), 1),
            }
        }
    }
}

/// What files a view is matched to, in a few words.
fn matches(criteria: &MatchCriteria) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(path) = &criteria.exact_path {
        parts.push(path.display().to_string());
    } else if let Some(path) = &criteria.relative_path {
        parts.push(path.clone());
    }
    if let Some(pattern) = criteria
        .path_pattern
        .as_ref()
        .or(criteria.filename_pattern.as_ref())
    {
        parts.push(pattern.clone());
    }
    if let Some(columns) = &criteria.schema_columns {
        parts.push(format!("{} columns", columns.len()));
    }
    if parts.is_empty() {
        "any file".into()
    } else {
        parts.join(", ")
    }
}

/// What `datui man` prints, and its exit code: the page list (`list`), every page
/// written under `dir`, or one page. On a terminal (`terminal`) a page is shown with
/// `man`, as `git help` does; where there is no `man` (Windows, a minimal container),
/// as plain text, through a pager ($PAGER, less, more). For a pipe the page's roff is
/// printed, for `man -l -` or a file.
pub fn man(
    page: Option<&str>,
    list: bool,
    dir: Option<&std::path::Path>,
    terminal: bool,
) -> (String, i32) {
    use datui_cli::man::{PAGES, find};
    if list {
        let width = PAGES.iter().map(|p| p.title().len()).max().unwrap_or(0);
        let text = PAGES
            .iter()
            .map(|p| format!("{:width$}  {}\n", p.title(), p.summary()))
            .collect();
        return (text, 0);
    }
    if let Some(dir) = dir {
        let written: std::io::Result<()> = PAGES.iter().try_for_each(|p| {
            let section = dir.join(format!("man{}", p.section));
            std::fs::create_dir_all(&section)?;
            std::fs::write(section.join(p.file_name()), p.roff())
        });
        return match written {
            Ok(()) => (
                format!("Wrote {} pages under {}\n", PAGES.len(), dir.display()),
                0,
            ),
            Err(e) => (format!("{}: {e}\n", dir.display()), 1),
        };
    }
    let name = page.unwrap_or("datui");
    let Some(page) = find(name) else {
        return (
            format!("No manual page {name}; `datui man --list` lists them\n"),
            1,
        );
    };
    if terminal {
        if let Some(code) = show_with_man(page) {
            return (String::new(), code);
        }
        let width = crossterm::terminal::size().map_or(80, |(cols, _)| usize::from(cols));
        let text = page.plain(width.clamp(40, 100));
        if show_with_pager(&text) {
            return (String::new(), 0);
        }
        return (text, 0);
    }
    (page.roff(), 0)
}

/// Page `text` through the first pager that runs. False when none does.
fn show_with_pager(text: &str) -> bool {
    pagers()
        .into_iter()
        .any(|mut pager| page_through(&mut pager, text))
}

/// The pagers to try, in order: `$PAGER`, run by the shell as git runs it so quoted
/// paths and arguments work, then `less` and `more`.
fn pagers() -> Vec<std::process::Command> {
    use std::process::Command;
    let mut out = Vec::new();
    if let Some(pager) = std::env::var("PAGER").ok().filter(|p| !p.trim().is_empty()) {
        if cfg!(unix) {
            let mut sh = Command::new("sh");
            sh.arg("-c").arg(&pager);
            out.push(sh);
        } else {
            let mut words = pager.split_whitespace();
            if let Some(program) = words.next() {
                let mut command = Command::new(program);
                command.args(words);
                out.push(command);
            }
        }
    }
    let mut less = Command::new("less");
    // As git does: quit when the page fits, keep the screen, pass colors.
    if std::env::var_os("LESS").is_none() {
        less.env("LESS", "FRX");
    }
    out.push(less);
    out.push(Command::new("more"));
    out
}

/// Run `pager` with `text` on its input. False when it cannot be started.
fn page_through(pager: &mut std::process::Command, text: &str) -> bool {
    use std::io::Write;
    let Ok(mut child) = pager.stdin(std::process::Stdio::piped()).spawn() else {
        return false;
    };
    if let Some(mut stdin) = child.stdin.take() {
        // A pager quit before the end closes the pipe; that is not a failure.
        let _ = stdin.write_all(text.as_bytes());
    }
    let _ = child.wait();
    true
}

/// Show `page` with `man`, from a temporary file named as the page, so its title and
/// section are its own. `None` when `man` cannot be run.
fn show_with_man(page: &datui_cli::man::Page) -> Option<i32> {
    let dir = tempfile::Builder::new()
        .prefix("datui-man-")
        .tempdir()
        .ok()?;
    let path = dir.path().join(page.file_name());
    std::fs::write(&path, page.roff()).ok()?;
    // A path with a slash is a file to man-db, mandoc and macOS's man alike.
    let status = std::process::Command::new("man").arg(&path).status().ok()?;
    Some(status.code().unwrap_or(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn views_are_listed_removed_by_name_and_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let config = ConfigManager::with_dir(dir.path().to_path_buf());
        let mut manager = ViewManager::new(&config).unwrap();
        for name in ["daily", "weekly"] {
            let criteria = MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: Some(format!("{name}_*.csv")),
                schema_columns: None,
                schema_types: None,
                table: None,
            };
            let settings = crate::view::ViewSettings {
                sample: None,
                query: None,
                sql_query: None,
                fuzzy_query: None,
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_descending: Vec::new(),
                sort_ascending: true,
                column_order: Vec::new(),
                locked_columns_count: 0,
                pivot: None,
                melt: None,
                reshape_source: None,
                columns: Vec::new(),
            };
            manager
                .create_view(name.to_string(), None, criteria, settings)
                .unwrap();
        }
        let (text, code) = views(&config, &ViewsAction::List);
        assert_eq!(code, 0);
        assert!(
            text.contains("daily") && text.contains("weekly_*.csv"),
            "{text}"
        );
        let (text, code) = views(
            &config,
            &ViewsAction::Rm {
                name: "nope".into(),
            },
        );
        assert_eq!(code, 1, "{text}");
        assert_eq!(
            views(
                &config,
                &ViewsAction::Rm {
                    name: "daily".into()
                }
            )
            .1,
            0
        );
        let (text, _) = views(&config, &ViewsAction::List);
        assert!(!text.contains("daily") && text.contains("weekly"), "{text}");
        let (text, code) = views(&config, &ViewsAction::Clear);
        assert_eq!((text.as_str(), code), ("Removed 1 saved views\n", 0));
        assert_eq!(views(&config, &ViewsAction::List).0, "No saved views\n");
    }

    #[test]
    fn cache_clear_says_what_it_did() {
        let (text, code) = cache(None, &CacheAction::Clear { recents: false });
        assert_eq!((text.as_str(), code), ("No cache to clear\n", 0));
    }

    #[test]
    fn man_lists_writes_and_prints_pages() {
        let (list, code) = man(None, true, None, false);
        assert_eq!(code, 0);
        assert!(list.contains("datui-config(5)"), "{list}");
        let dir = tempfile::tempdir().unwrap();
        let (_, code) = man(None, false, Some(dir.path()), false);
        assert_eq!(code, 0);
        assert!(dir.path().join("man1/datui.1").exists());
        assert!(dir.path().join("man5/datui-config.5").exists());
        assert!(dir.path().join("man7/datui-keys.7").exists());
        let (page, code) = man(Some("keys"), false, None, false);
        assert_eq!(code, 0);
        assert!(page.contains(".TH DATUI\\-KEYS 7"), "{page}");
        // Without man(1), the page reads as text.
        let text = datui_cli::man::find("keys").unwrap().plain(80);
        assert!(text.starts_with("DATUI-KEYS(7)"), "{text}");
        assert!(text.contains("Ctrl+O"), "{text}");
        let (said, code) = man(Some("nope"), false, None, false);
        assert_eq!(code, 1);
        assert!(said.contains("datui man --list"), "{said}");
    }
}
