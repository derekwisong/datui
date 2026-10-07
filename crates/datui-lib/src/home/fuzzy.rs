//! Fuzzy matching scored as fzf scores it, so ranking matches the finder people already
//! know (`fzf`, `fzf-lua`, Telescope's fzf-native and `snacks.picker`, a port of
//! `fzf/src/algo/algo.go`, all agree). The scoring constants are fzf's:
//!
//! - a match right after `/` or `_` beats one mid-word
//! - consecutive characters beat scattered ones
//! - a match in the file name beats one in a directory
//! - the best alignment wins, not the first found (`revdetail` lands on `revenue`, not
//!   the `re` in `warehouse`)

/// Character classes, which is how fzf decides what counts as a boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Class {
    White = 0,
    NonWord = 1,
    Delimiter = 2,
    Lower = 3,
    Upper = 4,
    Number = 5,
}

// fzf's constants, unchanged. They are a calibrated set rather than independent
// knobs: the consecutive bonus is exactly what cancels a one-character gap, so
// "abc" and "a-b-c" differ by the boundary bonuses alone.
const SCORE_MATCH: i32 = 16;
const SCORE_GAP_START: i32 = -3;
const SCORE_GAP_EXTENSION: i32 = -1;
const BONUS_BOUNDARY: i32 = SCORE_MATCH / 2;
const BONUS_NON_WORD: i32 = SCORE_MATCH / 2;
const BONUS_CAMEL_123: i32 = BONUS_BOUNDARY - 1;
const BONUS_CONSECUTIVE: i32 = -(SCORE_GAP_START + SCORE_GAP_EXTENSION);
const BONUS_FIRST_CHAR_MULTIPLIER: i32 = 2;
/// Start-of-string and whitespace boundaries: fzf's path scheme value, not its
/// default's `+ 2`, so a match after `/` outranks one at the start (every haystack here
/// is a path; `report` should prefer `archive/old/report.csv`).
const BONUS_BOUNDARY_WHITE: i32 = BONUS_BOUNDARY;
const BONUS_BOUNDARY_DELIMITER: i32 = BONUS_BOUNDARY + 1;

/// Awarded when no path separator follows the match start (it landed in the file
/// name): fzf's `--scheme=path`, so `sales` finds `archive/old/sales.csv` over
/// `sales/2024/report.csv`.
const BONUS_FILENAME: i32 = BONUS_BOUNDARY - 2;

/// Added when the needle is the whole haystack, ignoring case. Not fzf's: it ranks an
/// exact name above the other matches, as no alignment of a name-length needle gains
/// this much over another.
const EXACT_BONUS: i32 = 1_000;

/// Where a needle sits in a name, for a list narrowed by substring: the whole name
/// (0), its start (1), or inside it (2). `None` when the name does not contain it.
/// Case-insensitive; an empty needle is inside every name.
pub fn substring_rank(needle: &str, haystack: &str) -> Option<u8> {
    let needle = needle.to_lowercase();
    let hay = haystack.to_lowercase();
    if needle.is_empty() {
        Some(2)
    } else if hay == needle {
        Some(0)
    } else if hay.starts_with(&needle) {
        Some(1)
    } else {
        hay.contains(&needle).then_some(2)
    }
}

fn class_of(c: char) -> Class {
    if c.is_whitespace() {
        Class::White
    } else if matches!(c, '/' | '\\' | ',' | ':' | ';' | '|') {
        Class::Delimiter
    } else if c.is_ascii_digit() {
        Class::Number
    } else if c.is_uppercase() {
        Class::Upper
    } else if c.is_lowercase() || c.is_alphabetic() {
        Class::Lower
    } else {
        Class::NonWord
    }
}

/// The bonus for matching a character of class `curr` when the one before it was
/// `prev`. This is where "start of a word" is expressed.
fn bonus_for(prev: Class, curr: Class) -> i32 {
    if curr > Class::NonWord {
        match prev {
            Class::White => return BONUS_BOUNDARY_WHITE,
            Class::Delimiter => return BONUS_BOUNDARY_DELIMITER,
            Class::NonWord => return BONUS_BOUNDARY,
            _ => {}
        }
    }
    // camelCase, and the digit that starts a run of them.
    if (prev == Class::Lower && curr == Class::Upper)
        || (prev != Class::Number && curr == Class::Number)
    {
        return BONUS_CAMEL_123;
    }
    match curr {
        Class::NonWord | Class::Delimiter => BONUS_NON_WORD,
        Class::White => BONUS_BOUNDARY_WHITE,
        _ => 0,
    }
}

/// A match: its score and the characters that made it, from the same alignment, so
/// highlights show what was scored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    /// Higher is better, as in fzf.
    pub score: i32,
    /// Character indices into the haystack, ascending.
    pub positions: Vec<usize>,
}

/// Score one alignment: the greedy forward walk starting at `start`.
///
/// Returns `None` when the needle does not fit in what remains of the haystack.
fn score_from(hay: &[char], lower: &[char], needle: &[char], start: usize) -> Option<Match> {
    if lower[start] != needle[0] {
        return None;
    }

    let mut positions = Vec::with_capacity(needle.len());
    let mut score = 0i32;
    let mut consecutive = 0usize;
    let mut first_bonus = 0i32;

    // A match sitting in the file name rather than in a directory along the way.
    if !hay[start..].iter().any(|c| *c == '/' || *c == '\\') {
        score += BONUS_FILENAME;
    }

    let mut prev_class = if start == 0 {
        Class::White
    } else {
        class_of(hay[start - 1])
    };
    let mut previous: Option<usize> = None;

    for (n, needle_char) in needle.iter().enumerate() {
        // Find this needle character at or after where the last one landed.
        let from = previous.map(|p| p + 1).unwrap_or(start);
        let pos = (from..lower.len()).find(|i| lower[*i] == *needle_char)?;

        let class = class_of(hay[pos]);
        let gap = previous.map(|p| pos - p - 1).unwrap_or(0);

        let bonus = if gap > 0 {
            prev_class = class_of(hay[pos - 1]);
            let b = bonus_for(prev_class, class);
            score += SCORE_GAP_START + (gap as i32 - 1) * SCORE_GAP_EXTENSION;
            consecutive = 0;
            first_bonus = 0;
            b
        } else {
            let b = bonus_for(prev_class, class);
            if consecutive == 0 {
                first_bonus = b;
                b
            } else {
                // A run that begins mid-word but crosses a boundary is credited for
                // the boundary: "sales" in "my_sales" should not be penalised for
                // having started one character early.
                if b >= BONUS_BOUNDARY && b > first_bonus {
                    first_bonus = b;
                }
                b.max(first_bonus).max(BONUS_CONSECUTIVE)
            }
        };

        // The first character of the needle is where the match is anchored, so its
        // bonus counts double.
        score += SCORE_MATCH
            + if n == 0 {
                bonus * BONUS_FIRST_CHAR_MULTIPLIER
            } else {
                bonus
            };

        consecutive += 1;
        prev_class = class;
        previous = Some(pos);
        positions.push(pos);
    }

    Some(Match { score, positions })
}

/// The best fuzzy match of `needle` in `haystack`, or `None`: every start is tried and
/// the highest alignment wins. Case-insensitive; an empty needle matches with score 0.
pub fn best_match(needle: &str, haystack: &str) -> Option<Match> {
    if needle.is_empty() {
        return Some(Match {
            score: 0,
            positions: Vec::new(),
        });
    }
    // Most names in a long list do not match. For ASCII, which is most names, saying so
    // takes one pass over the bytes and no allocation; the search scores tens of
    // thousands of names per keystroke.
    if needle.is_ascii() && haystack.is_ascii() && !ascii_subsequence(haystack, needle) {
        return None;
    }
    let hay: Vec<char> = haystack.chars().collect();
    let lower: Vec<char> = haystack.to_lowercase().chars().collect();
    let needle: Vec<char> = needle.to_lowercase().chars().collect();
    // Lowercasing can change length (ß, İ). Falling back keeps the indices honest
    // rather than highlighting the wrong characters.
    if lower.len() != hay.len() {
        return simple_match(&hay, &needle);
    }
    if needle.len() > hay.len() {
        return None;
    }
    // A single cheap pass in front of the exhaustive one. Most candidates in a long
    // list do not match at all, and those now cost O(n) instead of a scan from every
    // position the first character happens to sit at.
    if !subsequence(&lower, &needle) {
        return None;
    }

    // The whole name typed is the answer: `hour` must find `hour` before `time_hour`,
    // which the boundary after `_` scores the same.
    if lower == needle {
        let mut m = score_from(&hay, &lower, &needle, 0)?;
        m.score += EXACT_BONUS;
        return Some(m);
    }

    let mut best: Option<Match> = None;
    for start in 0..hay.len() {
        // Only positions where the first needle character actually sits can start an
        // alignment, which is what keeps the exhaustive search cheap in practice.
        if lower[start] != needle[0] {
            continue;
        }
        if let Some(candidate) = score_from(&hay, &lower, &needle, start) {
            if best.as_ref().is_none_or(|b| candidate.score > b.score) {
                best = Some(candidate);
            }
        } else {
            // The needle no longer fits in what remains; no later start will fit
            // either.
            break;
        }
    }
    best
}

/// A plain greedy subsequence walk, for haystacks whose lowercase form has a
/// different length than the original and so cannot be indexed in parallel.
fn simple_match(hay: &[char], needle: &[char]) -> Option<Match> {
    let mut positions = Vec::with_capacity(needle.len());
    let mut hi = 0usize;
    for nc in needle {
        let found =
            (hi..hay.len()).find(|i| hay[*i].to_lowercase().next().is_some_and(|c| c == *nc))?;
        positions.push(found);
        hi = found + 1;
    }
    Some(Match {
        score: (positions.len() as i32) * SCORE_MATCH,
        positions,
    })
}

/// Whether `needle` is a subsequence of `haystack`, ignoring ASCII case. Both ASCII.
fn ascii_subsequence(haystack: &str, needle: &str) -> bool {
    let mut hay = haystack.bytes();
    needle
        .bytes()
        .all(|n| hay.any(|h| h.eq_ignore_ascii_case(&n)))
}

/// Whether `needle` is a subsequence of an already-lowercased `haystack`.
fn subsequence(lower: &[char], needle: &[char]) -> bool {
    let mut hi = 0usize;
    for nc in needle {
        match (hi..lower.len()).find(|i| lower[*i] == *nc) {
            Some(found) => hi = found + 1,
            None => return false,
        }
    }
    true
}

/// Whether `needle` matches at all, without scoring: a one-pass gate before
/// `best_match`, since most candidates fail.
pub fn is_match(needle: &str, haystack: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let mut chars = haystack.chars().flat_map(|c| c.to_lowercase());
    'outer: for nc in needle.chars().flat_map(|c| c.to_lowercase()) {
        for hc in chars.by_ref() {
            if hc == nc {
                continue 'outer;
            }
        }
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exact_name_outranks_the_same_word_after_a_boundary() {
        let exact = best_match("hour", "hour").unwrap().score;
        let inside = best_match("hour", "time_hour").unwrap().score;
        assert!(exact > inside, "{exact} vs {inside}");
        assert!(best_match("HOUR", "hour").unwrap().score > inside);
        assert_eq!(best_match("hour", "hour").unwrap().positions, [0, 1, 2, 3]);
    }

    #[test]
    fn substring_rank_puts_the_name_then_its_start_then_the_rest() {
        assert_eq!(substring_rank("Hour", "hour"), Some(0));
        assert_eq!(substring_rank("hour", "hours"), Some(1));
        assert_eq!(substring_rank("hour", "time_hour"), Some(2));
        assert_eq!(substring_rank("hour", "minute"), None);
        assert_eq!(substring_rank("", "minute"), Some(2));
    }
}
