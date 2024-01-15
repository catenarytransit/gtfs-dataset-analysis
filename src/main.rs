// 2023-01-15 - Written by Kyler Chin - Catenary Transit Initiatives
// https://github.com/catenarytransit/gtfs-dataset-analysis
// Removing this attribution is not allowed, as covered in APGL.
// Intended for analysing translation patterns of GTFS Schedule data across the globe
use csv::ReaderBuilder;
use gtfs_structures::Agency;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::path::Path;

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct RawTranslationRow {
    table_name: String,
    field_name: String,
    language: String,
    record_id: Option<String>,
    record_sub_id: Option<String>,
    field_value: Option<String>,
}

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct TranslationPivotRow {
    count: u64,
    table_name: String,
    field_name: String,
    language: String,
    has_record_id: bool,
    has_record_sub_id: bool,
    has_field_value: bool,
    feed_id: String,
}

#[derive(Hash, PartialEq, Eq)]
struct TranslationHashKey {
    table_name: String,
    field_name: String,
    language: String,
    has_record_id: bool,
    has_record_sub_id: bool,
    has_field_value: bool,
    feed_id: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let inputpath = arguments::parse(std::env::args())
        .expect("Add an unzipped folder path via --input PATH")
        .get::<String>("input");

    let mut agency_info: HashMap<String, Agency> = HashMap::new();
    let mut translation_pivot: HashMap<TranslationHashKey, i64> = HashMap::new();

    let path = fs::read_dir(inputpath.unwrap().as_str()).unwrap();

    for entry in path {
        let entry = entry.unwrap();
        let feed_name = entry.file_name().into_string().unwrap();
        println!("Reading {}", feed_name);
        if entry.path().is_dir() {
            let feed_dir = fs::read_dir(entry.path()).unwrap();

            //read translations if exist
            let translations = fs::read_to_string(Path::new(
                format!("{}/translations.txt", entry.path().to_str().unwrap()).as_str(),
            ));

            if translations.is_ok() {
                let translations = translations.unwrap();

                println!("Translation found for {:?}", entry.path().file_name());

                let mut reader = csv::Reader::from_reader(translations.as_bytes());

                for result in reader.deserialize() {
                    let row: Result<RawTranslationRow, csv::Error> = result;
                    if let Ok(row) = row {
                        let hash_key = TranslationHashKey {
                            table_name: row.table_name,
                            field_name: row.field_name,
                            language: row.language,
                            has_record_id: row.record_id.is_some(),
                            has_field_value: row.field_value.is_some(),
                            has_record_sub_id: row.record_sub_id.is_some(),
                            feed_id: feed_name.clone(),
                        };

                        translation_pivot
                            .entry(hash_key)
                            .and_modify(|counter| *counter += 1)
                            .or_insert(1);
                    }
                }
            }
        }
    }

    Ok(())
}
