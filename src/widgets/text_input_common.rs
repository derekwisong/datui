use color_eyre::Result;
use std::fs;
use std::io::{BufRead, BufReader, Write};

use crate::cache::CacheManager;

/// Shared utilities for text input widgets
/// Load history from a cache file
pub fn load_history_impl(cache: &CacheManager, history_id: &str) -> Result<Vec<String>> {
    let history_file = cache.cache_file(&format!("{}_history.txt", history_id));

    if !history_file.exists() {
        return Ok(Vec::new());
    }

    let file = fs::File::open(&history_file)?;
    let reader = BufReader::new(file);
    let mut history = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            history.push(line);
        }
    }

    Ok(history)
}

/// Save history to a cache file
pub fn save_history_impl(
    cache: &CacheManager,
    history_id: &str,
    history: &[String],
    limit: usize,
) -> Result<()> {
    cache.ensure_cache_dir()?;
    let history_file = cache.cache_file(&format!("{}_history.txt", history_id));

    let mut file = fs::File::create(&history_file)?;

    // Write history entries (oldest first, but we keep the most recent `limit` entries)
    let start = history.len().saturating_sub(limit);
    for entry in history.iter().skip(start) {
        writeln!(file, "{}", entry)?;
    }

    Ok(())
}

/// Add entry to history with deduplication
/// Only consecutive duplicate entries are skipped
pub fn add_to_history(history: &mut Vec<String>, entry: String) {
    // Only skip if the new entry matches the last entry (consecutive duplicate)
    if let Some(last) = history.last() {
        if last == &entry {
            return; // Skip consecutive duplicate
        }
    }
    history.push(entry);
}

/// Convert character position to byte position in a UTF-8 string
pub fn char_to_byte_pos(text: &str, char_pos: usize) -> usize {
    text.chars().take(char_pos).map(|c| c.len_utf8()).sum()
}

/// Convert byte position to character position in a UTF-8 string
pub fn byte_to_char_pos(text: &str, byte_pos: usize) -> usize {
    let mut char_pos = 0;
    let mut byte_count = 0;

    for ch in text.chars() {
        if byte_count >= byte_pos {
            break;
        }
        byte_count += ch.len_utf8();
        char_pos += 1;
    }

    char_pos
}

/// Get the character at a given character position
pub fn char_at(text: &str, char_pos: usize) -> Option<char> {
    text.chars().nth(char_pos)
}

/// Get the byte range for a character at a given character position
pub fn char_byte_range(text: &str, char_pos: usize) -> Option<(usize, usize)> {
    let mut byte_start = 0;

    for (char_count, ch) in text.chars().enumerate() {
        if char_count == char_pos {
            return Some((byte_start, byte_start + ch.len_utf8()));
        }
        byte_start += ch.len_utf8();
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_char_to_byte_pos() {
        assert_eq!(char_to_byte_pos("hello", 0), 0);
        assert_eq!(char_to_byte_pos("hello", 5), 5);
        assert_eq!(char_to_byte_pos("café", 3), 3); // 'é' is 2 bytes
        assert_eq!(char_to_byte_pos("café", 4), 5);
        assert_eq!(char_to_byte_pos("🚀", 0), 0);
        assert_eq!(char_to_byte_pos("🚀", 1), 4); // Emoji is 4 bytes
    }

    #[test]
    fn test_byte_to_char_pos() {
        assert_eq!(byte_to_char_pos("hello", 0), 0);
        assert_eq!(byte_to_char_pos("hello", 5), 5);
        assert_eq!(byte_to_char_pos("café", 3), 3);
        assert_eq!(byte_to_char_pos("café", 5), 4);
        assert_eq!(byte_to_char_pos("🚀", 0), 0);
        assert_eq!(byte_to_char_pos("🚀", 4), 1);
    }

    #[test]
    fn test_char_at() {
        assert_eq!(char_at("hello", 0), Some('h'));
        assert_eq!(char_at("hello", 4), Some('o'));
        assert_eq!(char_at("café", 3), Some('é'));
        assert_eq!(char_at("🚀", 0), Some('🚀'));
        assert_eq!(char_at("hello", 10), None);
    }

    #[test]
    fn test_char_byte_range() {
        assert_eq!(char_byte_range("hello", 0), Some((0, 1)));
        assert_eq!(char_byte_range("hello", 4), Some((4, 5)));
        assert_eq!(char_byte_range("café", 3), Some((3, 5))); // 'é' is 2 bytes
        assert_eq!(char_byte_range("🚀", 0), Some((0, 4))); // Emoji is 4 bytes
        assert_eq!(char_byte_range("hello", 10), None);
    }

    #[test]
    fn test_add_to_history() {
        let mut history = Vec::new();

        // Add first entry
        add_to_history(&mut history, "query1".to_string());
        assert_eq!(history.len(), 1);

        // Add different entry
        add_to_history(&mut history, "query2".to_string());
        assert_eq!(history.len(), 2);

        // Add consecutive duplicate (should be skipped)
        add_to_history(&mut history, "query2".to_string());
        assert_eq!(history.len(), 2);

        // Add non-consecutive duplicate (should be preserved)
        add_to_history(&mut history, "query1".to_string());
        assert_eq!(history.len(), 3);
        assert_eq!(history[0], "query1");
        assert_eq!(history[1], "query2");
        assert_eq!(history[2], "query1");
    }
}
