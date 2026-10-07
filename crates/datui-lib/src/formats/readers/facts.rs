//! What the Info panel's worker reads of a file Polars opened, for its format's tab: an
//! Arrow IPC file's footer, an Avro file's header, an ORC file's tail. Each is a few KB
//! the open read too, and none of the rows. Parquet's is [`crate::formats::parquet_footer`].

use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;

use super::FormatFacts;
use crate::FileFormat;
use crate::formats::model_files::MetaValue;
use crate::formats::text_formats::{Detail, capped_list, count, tab};
use crate::numfmt::group_chrome;

fn facts(detail: Detail) -> FormatFacts {
    FormatFacts {
        detail: Some(Arc::new(detail)),
        footer: None,
    }
}

/// Metadata values as the tab lists them: text where they are text.
fn metadata<'a>(pairs: impl Iterator<Item = (&'a str, &'a [u8])>) -> Vec<(String, MetaValue)> {
    let pairs: Vec<_> = pairs.collect();
    let total = pairs.len();
    capped_list(
        pairs.into_iter().map(|(key, value)| {
            let value = match std::str::from_utf8(value) {
                Ok(text) => text.to_string(),
                Err(_) => format!("{} of bytes", crate::numfmt::bytes(value.len() as u64)),
            };
            (key.to_string(), MetaValue::Text(value))
        }),
        total,
    )
}

/// An Arrow IPC file's footer: its record batches and dictionaries, and the metadata
/// its schema and footer carry (a Hugging Face dataset's features, pandas' index).
pub(crate) fn arrow(path: &Path) -> Result<FormatFacts> {
    let mut file = std::fs::File::open(path)?;
    let meta = polars_arrow::io::ipc::read::read_file_metadata(&mut file)?;
    let middot = crate::glyphs::get().middot;
    let mut first = count(meta.blocks.len() as u64, "record batch", "record batches");
    if let Some(dictionaries) = meta.dictionaries.as_ref().filter(|d| !d.is_empty()) {
        first.push_str(&format!(
            " {middot} {}",
            count(dictionaries.len() as u64, "dictionary", "dictionaries")
        ));
    }
    let lines = vec![
        first,
        format!(
            "{} {middot} {}",
            count(meta.schema.len() as u64, "column", "columns"),
            if meta.ipc_schema.is_little_endian {
                "little-endian"
            } else {
                "big-endian"
            }
        ),
    ];
    let pairs = meta
        .custom_schema_metadata
        .iter()
        .chain(meta.custom_metadata.iter())
        .flat_map(|m| m.iter())
        .map(|(k, v)| (k.as_str(), v.as_bytes()));
    Ok(facts(Detail {
        tab: tab(FileFormat::Arrow),
        lines,
        list_title: "Metadata",
        list: metadata(pairs),
        ..Default::default()
    }))
}

/// An Avro file's header: its record's name and documentation, its codec, and each
/// field's documentation (an export from datui keeps a renamed column's name there).
pub(crate) fn avro(path: &Path) -> Result<FormatFacts> {
    use polars_arrow::io::avro::avro_schema;
    let mut file = std::io::BufReader::new(std::fs::File::open(path)?);
    let meta = avro_schema::read::read_metadata(&mut file)
        .map_err(|e| color_eyre::eyre::eyre!("not an Avro file: {e:?}"))?;
    let record = &meta.record;
    let middot = crate::glyphs::get().middot;
    let name = match &record.namespace {
        Some(ns) if !ns.is_empty() => format!("{ns}.{}", record.name),
        _ => record.name.clone(),
    };
    let codec = match meta.compression {
        None => "uncompressed",
        Some(avro_schema::file::Compression::Deflate) => "deflate",
        Some(avro_schema::file::Compression::Snappy) => "snappy",
    };
    let mut lines = vec![format!(
        "Record {name} {middot} {} {middot} {codec}",
        count(record.fields.len() as u64, "field", "fields")
    )];
    if let Some(doc) = record.doc.as_ref().filter(|d| !d.is_empty()) {
        lines.push(doc.clone());
    }
    let documented: Vec<_> = record
        .fields
        .iter()
        .filter_map(|f| Some((f.name.clone(), MetaValue::Text(f.doc.clone()?))))
        .collect();
    let total = documented.len();
    Ok(facts(Detail {
        tab: tab(FileFormat::Avro),
        lines,
        list_title: "Field docs",
        list: capped_list(documented.into_iter(), total),
        ..Default::default()
    }))
}

/// An ORC file's tail: its stripes and the rows in each, its compression and format
/// version, and the metadata its writer kept.
pub(crate) fn orc(path: &Path) -> Result<FormatFacts> {
    let mut file = std::fs::File::open(path)?;
    let meta = orc_rust::reader::metadata::read_metadata(&mut file)?;
    let middot = crate::glyphs::get().middot;
    let stripes: Vec<u64> = meta
        .stripe_metadatas()
        .iter()
        .map(|s| s.number_of_rows())
        .collect();
    let mut lines = vec![format!(
        "{} in {}",
        count(meta.number_of_rows(), "row", "rows"),
        count(stripes.len() as u64, "stripe", "stripes")
    )];
    if stripes.len() > 1 {
        let least = stripes.iter().min().copied().unwrap_or(0);
        let most = stripes.iter().max().copied().unwrap_or(0);
        lines.push(format!(
            "Rows per stripe: {} to {}",
            group_chrome(least as usize),
            group_chrome(most as usize)
        ));
    }
    let codec = meta.compression().map_or("uncompressed".to_string(), |c| {
        c.compression_type().to_string().to_lowercase()
    });
    lines.push(format!(
        "Format version: {} {middot} {codec}",
        meta.file_format_version()
    ));
    let mut pairs: Vec<(&str, &[u8])> = meta
        .user_custom_metadata()
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_slice()))
        .collect();
    pairs.sort();
    Ok(facts(Detail {
        tab: tab(FileFormat::Orc),
        lines,
        list_title: "Metadata",
        list: metadata(pairs.into_iter()),
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    fn text(detail: &Detail) -> String {
        detail.lines.join("\n")
    }

    /// Each reads the bytes its format puts at an end, and says what they hold.
    #[test]
    fn each_reads_its_header_or_footer() {
        let dir = tempfile::tempdir().unwrap();
        let mut df = df!("a" => [1i64, 2, 3], "b" => ["x", "y", "z"]).unwrap();

        let arrow_path = dir.path().join("t.arrow");
        IpcWriter::new(std::fs::File::create(&arrow_path).unwrap())
            .finish(&mut df)
            .unwrap();
        let detail = arrow(&arrow_path).unwrap().detail.unwrap();
        assert_eq!(detail.tab, "Arrow");
        assert!(
            text(&detail).contains("1 record batch"),
            "{}",
            text(&detail)
        );
        assert!(text(&detail).contains("2 columns"), "{}", text(&detail));

        let avro_path = dir.path().join("t.avro");
        let mut out = std::fs::File::create(&avro_path).unwrap();
        crate::avro_types::write(&mut df, &mut out).unwrap();
        drop(out);
        let detail = avro(&avro_path).unwrap().detail.unwrap();
        assert_eq!(detail.tab, "Avro");
        assert!(text(&detail).contains("2 fields"), "{}", text(&detail));

        // Not the format: an error, which the panel shows as its reason.
        assert!(orc(&arrow_path).is_err());
        assert!(avro(&arrow_path).is_err());
    }
}
