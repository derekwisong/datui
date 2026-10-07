use super::*;

/// A 16-bit stereo PCM WAV header as a producer writing to a pipe writes it: the
/// sizes 0xFFFFFFFF, and `junk` bytes reserved after `WAVE` when given.
pub(crate) fn streamed_wav(frames: u32, junk: Option<u32>) -> Vec<u8> {
    let mut out = b"RIFF".to_vec();
    out.extend(u32::MAX.to_le_bytes());
    out.extend(b"WAVE");
    if let Some(size) = junk {
        out.extend(b"JUNK");
        out.extend(size.to_le_bytes());
        out.extend(vec![0u8; size as usize]);
    }
    out.extend(b"fmt ");
    out.extend(16u32.to_le_bytes());
    out.extend(1u16.to_le_bytes()); // PCM
    out.extend(2u16.to_le_bytes()); // channels
    out.extend(48_000u32.to_le_bytes());
    out.extend((48_000u32 * 4).to_le_bytes());
    out.extend(4u16.to_le_bytes());
    out.extend(16u16.to_le_bytes());
    out.extend(b"data");
    out.extend(u32::MAX.to_le_bytes());
    for i in 0..frames {
        out.extend((i as i16).to_le_bytes());
        out.extend((-(i as i16)).to_le_bytes());
    }
    out
}

fn file_of(bytes: &[u8]) -> (tempfile::TempDir, File) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("take.wav");
    std::fs::write(&path, bytes).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    (dir, file)
}

#[test]
fn a_streamed_wav_gets_its_sizes() {
    let bytes = streamed_wav(100, None);
    let (_dir, mut file) = file_of(&bytes);
    assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Riff);
    assert_eq!(read_u32(&mut file, 4).unwrap() as usize, bytes.len() - 8);
    assert_eq!(
        read_u32(&mut file, 40).unwrap(),
        400,
        "100 frames of 4 bytes"
    );
    let header =
        crate::formats::audio::read_header(&std::fs::read(_dir.path().join("take.wav")).unwrap());
    assert!(header.is_ok(), "{header:?}");
    assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Nothing, "once");
}

#[test]
fn other_files_are_left_alone() {
    let (_dir, mut file) = file_of(b"a,b\n1,2\n");
    assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Nothing);
    assert_eq!(
        std::fs::read(_dir.path().join("take.wav")).unwrap(),
        b"a,b\n1,2\n"
    );
}

/// Over 4 GB, the JUNK chunk a producer reserved becomes RF64's `ds64`. The file
/// is sparse: only its header is ever read.
#[test]
fn over_four_gigabytes_the_header_becomes_rf64() {
    let bytes = streamed_wav(0, Some(28));
    let (dir, mut file) = file_of(&bytes);
    let len = 5u64 << 30;
    file.set_len(len).unwrap();
    assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::Rf64);
    let head = {
        let mut head = vec![0u8; 80];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut head).unwrap();
        head
    };
    assert_eq!(&head[0..4], b"RF64");
    assert_eq!(&head[12..16], b"ds64");
    let u64_at = |at: usize| u64::from_le_bytes(head[at..at + 8].try_into().unwrap());
    assert_eq!(u64_at(20), len - 8, "the RIFF size");
    let data_at = 12 + 8 + 28 + 8 + 16;
    assert_eq!(&head[data_at..data_at + 4], b"data");
    assert_eq!(u64_at(28), len - (data_at as u64 + 8), "the data size");
    drop(dir);

    // No room reserved: the sizes stay as they were written.
    let (_dir, mut file) = file_of(&streamed_wav(0, None));
    file.set_len(len).unwrap();
    assert_eq!(fix_wav_sizes(&mut file).unwrap(), Fixed::TooLong);
}

/// FILE is never replaced without `--force`.
#[test]
fn an_existing_file_is_refused_without_force() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run1.csv");
    std::fs::write(&path, "keep me").unwrap();
    let refused = create(&path, false).unwrap_err();
    assert!(refused.contains("--force"), "{refused}");
    assert_eq!(std::fs::read(&path).unwrap(), b"keep me");
    create(&path, true).unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"",
        "overwritten with --force"
    );
}
