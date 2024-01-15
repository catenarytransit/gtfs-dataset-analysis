// 2023-01-15 - Written by Kyler Chin - Catenary Transit Initiatives
// https://github.com/catenarytransit/gtfs-dataset-analysis
// Removing this attribution is not allowed, as covered in APGL.
// Intended for analysing translation patterns of GTFS Schedule data across the globe
use csv::ReaderBuilder;
use gtfs_structures::Agency;
use std::collections::HashMap;
use std::io::Write;
use std::error::Error;
use std::fs;
use std::path::Path;

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct RawTranslationRow {
    table_name: String,
    language: String,
    field_name: String,
    record_id: Option<String>,
    record_sub_id: Option<String>,
    field_value: Option<String>,
}

#[derive(Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
struct TranslationPivotRow {
    feed_id: String,
    table_name: String,
    language: String,
    field_name: String,
    has_record_id: bool,
    has_record_sub_id: bool,
    has_field_value: bool,
    count: u64,
}

#[derive(Hash, PartialEq, Eq)]
struct TranslationHashKey {
    feed_id: String,
    table_name: String,
    language: String,
    field_name: String,
    has_record_id: bool,
    has_record_sub_id: bool,
    has_field_value: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let inputpath = arguments::parse(std::env::args())
        .expect("Add an unzipped folder path via --input PATH")
        .get::<String>("input");

    let mut agency_info: HashMap<String, Vec<Agency>> = HashMap::new();
    let mut translation_pivot: HashMap<TranslationHashKey, u64> = HashMap::new();

    let path = fs::read_dir(inputpath.unwrap().as_str()).unwrap();

    let mut agency_info_wtr = csv::Writer::from_path("agency_analysis.csv").unwrap();

    let mut translation_pivot_wtr = csv::Writer::from_writer(vec![]);

    agency_info_wtr.write_record(&[
        &"feed_name",
        &"agency_id",
        &"name",
        &"url",
        &"timezone",
        &"lang",
        &"phone",
        &"fare_url",
        &"email",
    ])?;

    for entry in path {
        let entry = entry.unwrap();
        let feed_name = entry.file_name().into_string().unwrap();
        println!("Reading {}", feed_name);
        if entry.path().is_dir() {
            let feed_dir = fs::read_dir(entry.path()).unwrap();

            //read agency data
            let agency_info = fs::read_to_string(Path::new(
                format!("{}/agency.txt", entry.path().to_str().unwrap()).as_str(),
            ));

            if agency_info.is_ok() {
                let agency_info = agency_info.unwrap();

                let mut reader = csv::Reader::from_reader(agency_info.as_bytes());

                for result in reader.deserialize() {
                    let row: Result<Agency, csv::Error> = result;

                    if let Ok(row) = row {
                        agency_info_wtr.write_record(&[
                            &feed_name,
                            &row.id.unwrap_or_default(),
                            &row.name,
                            &row.url,
                            &row.timezone,
                            &row.lang.unwrap_or_default(),
                            &row.phone.unwrap_or_default(),
                            &row.fare_url.unwrap_or_default(),
                            &row.email.unwrap_or_default(),
                        ])?;
                    }
                }
            }

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
                    } else {
                        println!("Error in translation reading {:?}", row.unwrap_err());
                    }
                }
            }
        }
    }

    //write the pivot table

    for (k,v) in translation_pivot {
        translation_pivot_wtr.serialize(TranslationPivotRow {
            table_name: k.table_name,
            count: v,
            field_name: k.field_name,
            language: k.language,
            has_record_id: k.has_record_id,
            has_record_sub_id: k.has_record_sub_id,
            has_field_value: k.has_field_value,
            feed_id: k.feed_id
    })?;
    }

    let translation_csv = String::from_utf8(translation_pivot_wtr.into_inner().unwrap()).unwrap();
    let mut translation_file = std::fs::File::create("./translation_pivot_analysis.csv").unwrap();

    translation_file.write_all(translation_csv.as_bytes()).unwrap();

    Ok(())
}
