//! Fuzzy matching, scored the way fzf scores it.
//!
//! The point is not to invent a good matcher, it is to not surprise anyone. A person
//! who reaches for a fuzzy finder has one already calibrated in their fingers, and if
//! datui ranks differently the filter feels broken rather than different.
//!
//! fzf is the shared ancestor: `fzf` itself, `fzf-lua`, Telescope with
//! `telescope-fzf-native`, and `snacks.picker` — which is a direct port of
//! `fzf/src/algo/algo.go` — all agree on the behaviour this implements. The scoring
//! constants below are fzf's.
//!
//! What that buys, concretely:
//!
//! - a match right after `/` or `_` beats one in the middle of a word
//! - consecutive characters beat scattered ones
//! - a match in the file name beats one in a directory along the way
//! - the *best* alignment wins, not the first one found scanning left to right
//!
//! That last property is the one people notice. Matching `revdetail` against
//! `warehouse/2024/q3/revenue_detail.parquet` greedily puts `re` inside *ware*house;
//! every mainstream finder puts it on *rev*enue, because it tries every starting
//! position and keeps the highest score.

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
/// Start-of-string and whitespace boundaries.
///
/// fzf's default scheme sets this to `BONUS_BOUNDARY + 2`, privileging the start of
/// the string. Its *path* scheme drops it back to `BONUS_BOUNDARY`, so that a match
/// after `/` outranks one at the start — and that is the right scheme here, because
/// every haystack on this screen is a file name or a path below the search root.
///
/// Without it, `report` prefers `report/2024/summary.csv` to
/// `archive/old/report.csv` by a single point, which is the wrong answer and the one
/// fzf gives only when told the input is not paths.
const BONUS_BOUNDARY_WHITE: i32 = BONUS_BOUNDARY;
const BONUS_BOUNDARY_DELIMITER: i32 = BONUS_BOUNDARY + 1;

/// Awarded when nothing after the match start is a path separator — that is, when the
/// match landed in the file name rather than in a directory along the way.
///
/// This is fzf's `--scheme=path`, and it is the difference between `sales` finding
/// `archive/old/sales.csv` and finding `sales/2024/report.csv`. Search results here
/// are named by their path below the search root, so it matters more than usual.
const BONUS_FILENAME: i32 = BONUS_BOUNDARY - 2;

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

/// A successful match: how good it is, and exactly which characters made it.
///
/// The positions come out of the same alignment that produced the score, so what gets
/// highlighted is always what was actually scored.
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

/// Best fuzzy match of `needle` in `haystack`, or `None` if it does not match at all.
///
/// Every starting position is tried and the highest-scoring alignment wins. This is
/// what makes `revdetail` land on `revenue_detail` rather than on the `re` in
/// `warehouse`, and it is the behaviour people arrive with.
///
/// Matching is case-insensitive. An empty needle matches everything with score 0.
pub fn best_match(needle: &str, haystack: &str) -> Option<Match> {
    if needle.is_empty() {
        return Some(Match {
            score: 0,
            positions: Vec::new(),
        });
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

/// Whether `needle` matches at all, without paying for the scoring.
///
/// A cheap gate in front of `best_match`, since most candidates in a long list do not
/// match and every one that fails here costs a single pass.
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
