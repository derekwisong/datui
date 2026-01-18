use polars::prelude::*;
use std::fs::File;
use std::path::Path;

pub fn create_large_test_csv() -> &'static Path {
    let path = Path::new("tests/sample-data/large_test.csv");
    if !path.exists() {
        let mut df = df! (
            "a" => (0..100).collect::<Vec<i32>>(),
            "b" => (0..100).map(|i| format!("text_{}", i)).collect::<Vec<String>>(),
            "c" => (0..100).map(|i| i % 3).collect::<Vec<i32>>(),
            "d" => (0..100).map(|i| i % 5).collect::<Vec<i32>>()
        )
        .unwrap();
        let mut file = File::create(path).unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();
    }
    path
}
