//! An in-process stand-in for an S3 bucket that counts what is asked of it.
//!
//! Enough of the S3 API for datui and Polars to list a prefix and read objects in
//! ranges: `ListObjectsV2` (prefix and delimiter, one page), `HEAD`, and `GET` with
//! or without a `Range`. Signatures are not checked. Every request is counted, and
//! every body byte sent, so a test can say what a run cost on the wire. Nothing
//! leaves the loopback interface.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// What the bucket was asked for, counted as the requests arrive.
#[derive(Debug, Default)]
pub struct Wire {
    lists: AtomicU64,
    heads: AtomicU64,
    gets: AtomicU64,
    /// Body bytes sent in answer to GETs.
    bytes: AtomicU64,
}

/// One reading of [`Wire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WireCount {
    pub lists: u64,
    pub heads: u64,
    pub gets: u64,
    pub bytes: u64,
}

impl WireCount {
    pub fn requests(&self) -> u64 {
        self.lists + self.heads + self.gets
    }

    /// What happened between `earlier` and this reading.
    pub fn since(&self, earlier: &WireCount) -> WireCount {
        WireCount {
            lists: self.lists - earlier.lists,
            heads: self.heads - earlier.heads,
            gets: self.gets - earlier.gets,
            bytes: self.bytes - earlier.bytes,
        }
    }
}

impl Wire {
    pub fn count(&self) -> WireCount {
        WireCount {
            lists: self.lists.load(Ordering::SeqCst),
            heads: self.heads.load(Ordering::SeqCst),
            gets: self.gets.load(Ordering::SeqCst),
            bytes: self.bytes.load(Ordering::SeqCst),
        }
    }
}

/// Each key's bytes and ETag, the tag computed once rather than per request.
type Objects = BTreeMap<String, (Vec<u8>, String)>;

/// A bucket served at `endpoint`: `s3://<bucket>/<key>` with that endpoint reaches it.
pub struct FakeS3 {
    pub endpoint: String,
    pub wire: Arc<Wire>,
}

impl FakeS3 {
    /// Serve `objects` (key to bytes) as `bucket`.
    pub fn serve(bucket: &str, objects: BTreeMap<String, Vec<u8>>) -> FakeS3 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let wire = Arc::new(Wire::default());
        let objects: Objects = objects
            .into_iter()
            .map(|(key, bytes)| {
                let tag = etag(&bytes);
                (key, (bytes, tag))
            })
            .collect();
        let objects = Arc::new(RwLock::new(objects));
        let served = (wire.clone(), objects, bucket.to_string());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let (wire, objects, bucket) =
                    (served.0.clone(), served.1.clone(), served.2.clone());
                std::thread::spawn(move || answer(stream, &bucket, &objects, &wire));
            }
        });
        FakeS3 { endpoint, wire }
    }

    /// The configuration that points datui at this bucket.
    pub fn cloud_config(&self) -> datui::config::CloudConfig {
        datui::config::CloudConfig {
            s3_endpoint_url: Some(self.endpoint.clone()),
            s3_access_key_id: Some("testing".to_string()),
            s3_secret_access_key: Some("testing".to_string()),
            s3_region: Some("us-east-1".to_string()),
            ..Default::default()
        }
    }
}

/// Answer every request a connection carries, in turn.
fn answer(stream: TcpStream, bucket: &str, objects: &RwLock<Objects>, wire: &Wire) {
    let Ok(mut out) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(stream);
    loop {
        let mut request = String::new();
        if reader.read_line(&mut request).unwrap_or(0) == 0 {
            return;
        }
        let mut range = None;
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).unwrap_or(0) == 0 {
                return;
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("range")
            {
                range = Some(value.trim().to_string());
            }
        }
        let mut parts = request.split_whitespace();
        let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or("/"));
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let path = decode(path.trim_start_matches('/'));
        let key = path
            .strip_prefix(bucket)
            .map(|rest| rest.trim_start_matches('/').to_string())
            .unwrap_or_default();
        let objects = objects.read().expect("objects");
        let written = if key.is_empty() && method == "GET" {
            wire.lists.fetch_add(1, Ordering::SeqCst);
            let body = list(bucket, query, &objects);
            respond(&mut out, "200 OK", &[], body.as_bytes())
        } else if let Some((bytes, tag)) = objects.get(&key) {
            let mut headers = vec![
                ("Last-Modified", "Tue, 01 Oct 2024 00:00:00 GMT".to_string()),
                ("ETag", tag.clone()),
                ("Accept-Ranges", "bytes".to_string()),
            ];
            if method == "HEAD" {
                wire.heads.fetch_add(1, Ordering::SeqCst);
                headers.push(("Content-Length", bytes.len().to_string()));
                respond_head(&mut out, &headers)
            } else {
                wire.gets.fetch_add(1, Ordering::SeqCst);
                let (start, end) = byte_range(range.as_deref(), bytes.len());
                let body = &bytes[start..end];
                wire.bytes.fetch_add(body.len() as u64, Ordering::SeqCst);
                if range.is_some() {
                    headers.push((
                        "Content-Range",
                        format!("bytes {start}-{}/{}", end.saturating_sub(1), bytes.len()),
                    ));
                    respond(&mut out, "206 Partial Content", &headers, body)
                } else {
                    respond(&mut out, "200 OK", &headers, body)
                }
            }
        } else if method == "HEAD" {
            respond_head_status(&mut out, "404 Not Found")
        } else {
            let body =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code></Error>";
            respond(&mut out, "404 Not Found", &[], body.as_bytes())
        };
        if !written {
            return;
        }
    }
}

/// `bytes=a-b`, `bytes=a-` or `bytes=-n` over an object of `len` bytes, as a half-open
/// range; the whole object without one.
fn byte_range(range: Option<&str>, len: usize) -> (usize, usize) {
    let Some(spec) = range.and_then(|range| range.strip_prefix("bytes=")) else {
        return (0, len);
    };
    let (from, to) = spec.split_once('-').unwrap_or((spec, ""));
    match (from.parse::<usize>(), to.parse::<usize>()) {
        (Ok(from), Ok(to)) => (from.min(len), (to + 1).min(len)),
        (Ok(from), Err(_)) => (from.min(len), len),
        (Err(_), Ok(suffix)) => (len.saturating_sub(suffix), len),
        _ => (0, len),
    }
}

/// A `ListObjectsV2` page: every key under the prefix, folded at the delimiter.
fn list(bucket: &str, query: &str, objects: &Objects) -> String {
    let param = |name: &str| {
        query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(key, _)| *key == name)
            .map(|(_, value)| decode(value))
    };
    let prefix = param("prefix").unwrap_or_default();
    let delimiter = param("delimiter").filter(|delimiter| !delimiter.is_empty());
    let mut contents = String::new();
    let mut prefixes = std::collections::BTreeSet::new();
    let mut keys = 0;
    for (key, (bytes, tag)) in objects.range(prefix.clone()..) {
        let Some(rest) = key.strip_prefix(&prefix) else {
            break;
        };
        if let Some(delimiter) = &delimiter
            && let Some(at) = rest.find(delimiter.as_str())
        {
            prefixes.insert(format!("{prefix}{}", &rest[..at + delimiter.len()]));
            continue;
        }
        keys += 1;
        contents.push_str(&format!(
            "<Contents><Key>{key}</Key><LastModified>2024-10-01T00:00:00.000Z</LastModified>\
             <ETag>{tag}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            bytes.len()
        ));
    }
    let common: String = prefixes
        .iter()
        .map(|prefix| format!("<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>"))
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{bucket}</Name><Prefix>{prefix}</Prefix><KeyCount>{}</KeyCount>\
         <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}{common}\
         </ListBucketResult>",
        keys + prefixes.len()
    )
}

fn respond(out: &mut TcpStream, status: &str, headers: &[(&str, String)], body: &[u8]) -> bool {
    let mut head = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", body.len());
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    out.write_all(head.as_bytes()).is_ok() && out.write_all(body).is_ok()
}

fn respond_head(out: &mut TcpStream, headers: &[(&str, String)]) -> bool {
    let mut head = "HTTP/1.1 200 OK\r\n".to_string();
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    out.write_all(head.as_bytes()).is_ok()
}

fn respond_head_status(out: &mut TcpStream, status: &str) -> bool {
    out.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes())
        .is_ok()
}

/// A percent-encoded path or query value, decoded.
fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = (bytes[i] == b'%' && i + 2 < bytes.len())
            .then(|| std::str::from_utf8(&bytes[i + 1..i + 3]).ok())
            .flatten()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match hex {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A stable tag for an object's bytes: changed bytes, changed tag.
fn etag(bytes: &[u8]) -> String {
    let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    });
    format!("\"{hash:x}-{}\"", bytes.len())
}
