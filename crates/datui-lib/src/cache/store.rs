//! One store for every cache kind: a directory per kind, a file per entry.
//!
//! `<cache>/<kind>/<stable-hash(key)>.<ext>`, each file framed as
//!
//! | bytes | what |
//! |---|---|
//! | 4 | `dtuc` |
//! | 2 | the frame's version |
//! | 2 | the kind's payload version |
//! | 4 + n | the key |
//! | 4 + n | the fingerprint |
//! | 8 + n | the payload |
//! | 8 | CRC-64 of everything above |
//!
//! A file that does not decode, is of another version, holds another key (a hash
//! collision) or another fingerprint is a miss. A hit dates the file, and the
//! modification time is the LRU clock a sweep evicts by once a kind passes its budget.

use super::{CacheManager, atomic_write};
use crate::logging::LogFailure;
use color_eyre::Result;
use std::fs;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const MAGIC: &[u8; 4] = b"dtuc";
const FRAME_VERSION: u16 = 1;

/// A temp file older than this is from a writer that died; a live one renames its
/// temp file within moments of creating it.
const STALE_TEMP: Duration = Duration::from_secs(3_600);

/// What the cache files of one kind hold, and how much of the disk they may take.
pub(crate) trait Kind {
    /// The kind's directory under the cache.
    const DIR: &'static str;
    const EXT: &'static str;
    /// Bumped when the payload's encoding changes: files of another version are misses.
    const VERSION: u16;
    /// Bytes kept, all entries together. The entry just written is always kept.
    const BUDGET: u64;
    type Value;
    fn encode(value: &Self::Value) -> Result<Vec<u8>>;
    fn decode(payload: &[u8]) -> Option<Self::Value>;
}

/// CRC-64/XZ: fixed by its spec, unlike `DefaultHasher`, whose output may change
/// with any Rust release and would orphan every file named by it.
static CRC64: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_XZ);

/// A hash that is the same in every build, for file names and fingerprints.
pub struct StableHasher(crc::Digest<'static, u64>);

impl Default for StableHasher {
    fn default() -> Self {
        Self(CRC64.digest())
    }
}

impl StableHasher {
    /// Length-prefixed, so `("ab", "c")` and `("a", "bc")` differ.
    pub fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.u64(bytes.len() as u64);
        self.0.update(bytes);
        self
    }

    pub fn u64(&mut self, n: u64) -> &mut Self {
        self.0.update(&n.to_le_bytes());
        self
    }

    pub fn finish(self) -> u64 {
        self.0.finalize()
    }
}

/// CRC-64/XZ of one byte string.
pub fn stable_hash(bytes: &[u8]) -> u64 {
    CRC64.checksum(bytes)
}

/// The entries of one kind under a cache directory.
pub(crate) struct Store<K: Kind> {
    cache: CacheManager,
    budget: u64,
    kind: PhantomData<K>,
}

impl<K: Kind> Store<K> {
    pub(crate) fn new(cache: &CacheManager) -> Self {
        Self {
            cache: cache.clone(),
            budget: K::BUDGET,
            kind: PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_budget(mut self, budget: u64) -> Self {
        self.budget = budget;
        self
    }

    pub(crate) fn dir(&self) -> PathBuf {
        self.cache.cache_file(K::DIR)
    }

    pub(crate) fn file(&self, key: &str) -> PathBuf {
        self.dir()
            .join(format!("{:016x}.{}", stable_hash(key.as_bytes()), K::EXT))
    }

    /// The value stored under `key` at `fingerprint`, dating it as used.
    pub(crate) fn get(&self, key: &str, fingerprint: &str) -> Option<K::Value> {
        let file = self.file(key);
        let bytes = fs::read(&file).ok()?;
        let value = self.read(&file, &bytes, key, fingerprint)?;
        touch(&file);
        Some(value)
    }

    /// Date `key`'s entry as used, if there is one.
    pub(crate) fn touch(&self, key: &str) {
        let file = self.file(key);
        if file.exists() {
            touch(&file);
        }
    }

    /// Every entry that reads, by key, without dating any.
    pub(crate) fn scan(&self) -> Vec<(String, K::Value)> {
        let Ok(entries) = fs::read_dir(self.dir()) else {
            return Vec::new();
        };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == K::EXT))
            .filter_map(|file| {
                let bytes = fs::read(&file).ok()?;
                let frame = unframe(&bytes, K::VERSION).or_else(|| {
                    damaged::<K>(&file);
                    None
                })?;
                let value = K::decode(frame.payload).or_else(|| {
                    damaged::<K>(&file);
                    None
                })?;
                Some((frame.key.to_string(), value))
            })
            .collect()
    }

    /// Entries on disk.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        fs::read_dir(self.dir()).map_or(0, |entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == K::EXT))
                .count()
        })
    }

    pub(crate) fn put(&self, key: &str, fingerprint: &str, value: &K::Value) {
        self.put_all([(key, fingerprint, value)]);
    }

    /// Store each entry, then sweep once.
    pub(crate) fn put_all<'a>(
        &self,
        entries: impl IntoIterator<Item = (&'a str, &'a str, &'a K::Value)>,
    ) where
        K::Value: 'a,
    {
        let mut written = Vec::new();
        let mut bytes = 0u64;
        // Made once a batch, not asked again per entry.
        let mut made_dir = false;
        for (key, fingerprint, value) in entries {
            let file = self.file(key);
            let stored = (|| -> Result<()> {
                let frame = frame(K::VERSION, key, fingerprint, &K::encode(value)?)?;
                // An entry that has not changed is only dated: a measured directory is
                // recorded on every look, and most looks find what the last one did.
                if fs::metadata(&file).is_ok_and(|m| m.len() == frame.len() as u64)
                    && fs::read(&file).is_ok_and(|old| old == frame)
                {
                    touch(&file);
                    return Ok(());
                }
                if !made_dir {
                    fs::create_dir_all(self.dir())?;
                    made_dir = true;
                }
                atomic_write(&file, &frame)?;
                bytes += frame.len() as u64;
                Ok(())
            })();
            match stored {
                Ok(()) => written.push(file),
                Err(e) => log::warn!(target: "datui", "save a {} entry: {e:#}", K::DIR),
            }
        }
        if !written.is_empty() && self.may_pass_budget(bytes) {
            self.sweep(&written);
        }
    }

    /// Whether the kind may now be past its budget: unknown until this session's first
    /// sweep, then what that sweep found plus everything written since. Sweeping reads
    /// the whole directory, and measuring a directory of thousands writes a batch at a
    /// time; a sweep per batch grew with the cache.
    fn may_pass_budget(&self, written: u64) -> bool {
        let mut swept = self.cache.swept.lock().unwrap_or_else(|e| e.into_inner());
        match swept.get_mut(K::DIR) {
            Some(total) => {
                *total = total.saturating_add(written);
                *total > self.budget
            }
            None => true,
        }
    }

    /// Drop stale temp files and, past the budget, the least recently used entries down
    /// to three quarters of it, never one of `keep`. Under the kind's lock only so two sweeps do not race;
    /// writes land by rename and need none.
    fn sweep(&self, keep: &[PathBuf]) {
        self.cache
            .with_cache_lock(K::DIR, || {
                retire_legacy(&self.cache);
                let Ok(entries) = fs::read_dir(self.dir()) else {
                    return Ok(());
                };
                let now = SystemTime::now();
                let mut kept = Vec::new();
                for entry in entries.flatten() {
                    let path = entry.path();
                    let Ok(meta) = entry.metadata() else { continue };
                    let Ok(modified) = meta.modified() else {
                        continue;
                    };
                    if path.extension().is_some_and(|x| x == "tmp") {
                        if now
                            .duration_since(modified)
                            .is_ok_and(|age| age > STALE_TEMP)
                        {
                            fs::remove_file(&path).or_log("remove a stale cache temp file");
                        }
                    } else if path.extension().is_some_and(|x| x == K::EXT) {
                        kept.push((modified, meta.len(), path));
                    }
                }
                let mut total: u64 = kept.iter().map(|(_, len, _)| len).sum();
                let found = self.cache.swept.clone();
                let note = |total: u64| {
                    let mut swept = found.lock().unwrap_or_else(|e| e.into_inner());
                    swept.insert(K::DIR, total);
                };
                if total <= self.budget {
                    note(total);
                    return Ok(());
                }
                // Down to three quarters, so a cache that has filled its budget, where an LRU
                // cache settles, does not sweep again on the next write.
                let target = self.budget / 4 * 3;
                kept.sort();
                for (_, len, file) in kept {
                    if total <= target {
                        break;
                    }
                    if !keep.contains(&file) && fs::remove_file(&file).is_ok() {
                        total -= len;
                    }
                }
                note(total);
                Ok(())
            })
            .or_log(&format!("sweep the {} cache", K::DIR));
    }

    fn read(&self, file: &Path, bytes: &[u8], key: &str, fingerprint: &str) -> Option<K::Value> {
        let Some(frame) = unframe(bytes, K::VERSION) else {
            damaged::<K>(file);
            return None;
        };
        // Another key is a hash collision and another fingerprint a changed dataset:
        // ordinary misses, not damage.
        if frame.key != key || frame.fingerprint != fingerprint {
            return None;
        }
        K::decode(frame.payload).or_else(|| {
            damaged::<K>(file);
            None
        })
    }
}

/// Files earlier builds wrote, which nothing reads now.
fn retire_legacy(cache: &CacheManager) {
    for name in [
        "datasets.json",
        "dataset_shapes.json",
        "cloud_sources.json",
        "visits.json",
    ] {
        let _ = fs::remove_file(cache.cache_file(name));
    }
    let _ = fs::remove_dir_all(cache.cache_file("dataset_shapes"));
}

/// Log the first damaged file of each kind this process meets; one is news, a
/// thousand is noise.
fn damaged<K: Kind>(file: &Path) {
    static LOGGED: std::sync::Mutex<Vec<&'static str>> = std::sync::Mutex::new(Vec::new());
    let mut logged = LOGGED.lock().unwrap_or_else(|e| e.into_inner());
    if !logged.contains(&K::DIR) {
        logged.push(K::DIR);
        log::warn!(target: "datui", "{} is damaged or from another build; ignoring it", file.display());
    }
}

/// Mark a file as used just now. Best effort: an entry that cannot be re-dated is
/// still an entry that can be used.
fn touch(file: &Path) {
    fs::OpenOptions::new()
        .write(true)
        .open(file)
        .and_then(|f| f.set_modified(SystemTime::now()))
        .or_log("re-date a cache entry");
}

pub(crate) struct Frame<'a> {
    pub(crate) key: &'a str,
    pub(crate) fingerprint: &'a str,
    pub(crate) payload: &'a [u8],
}

pub(crate) fn frame(version: u16, key: &str, fingerprint: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(36 + key.len() + fingerprint.len() + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(&u32::try_from(key.len())?.to_le_bytes());
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(&u32::try_from(fingerprint.len())?.to_le_bytes());
    out.extend_from_slice(fingerprint.as_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(payload);
    out.extend_from_slice(&CRC64.checksum(&out).to_le_bytes());
    Ok(out)
}

pub(crate) fn unframe(bytes: &[u8], version: u16) -> Option<Frame<'_>> {
    let (body, sum) = bytes.split_last_chunk::<8>()?;
    if CRC64.checksum(body) != u64::from_le_bytes(*sum) {
        return None;
    }
    let rest = body.strip_prefix(MAGIC)?;
    let (frame_version, rest) = rest.split_first_chunk::<2>()?;
    let (kind_version, rest) = rest.split_first_chunk::<2>()?;
    if u16::from_le_bytes(*frame_version) != FRAME_VERSION
        || u16::from_le_bytes(*kind_version) != version
    {
        return None;
    }
    let (key, rest) = take_str(rest)?;
    let (fingerprint, rest) = take_str(rest)?;
    let (len, payload) = rest.split_first_chunk::<8>()?;
    (usize::try_from(u64::from_le_bytes(*len)).ok()? == payload.len()).then_some(Frame {
        key,
        fingerprint,
        payload,
    })
}

fn take_str(bytes: &[u8]) -> Option<(&str, &[u8])> {
    let (len, rest) = bytes.split_first_chunk::<4>()?;
    let len = usize::try_from(u32::from_le_bytes(*len)).ok()?;
    let (text, rest) = (rest.get(..len)?, rest.get(len..)?);
    Some((std::str::from_utf8(text).ok()?, rest))
}
