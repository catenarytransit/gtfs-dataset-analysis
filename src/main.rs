// 2023-01-15 - Written by Kyler Chin - Catenary Transit Initiatives
// Removing this attribution is not allowed, as covered in APGL.
// Intended for analysing translation patterns of GTFS Schedule data across the globe
use std::fs;
use gtfs_structures::Agency;

struct RawTranslationRow {
    table_name: String,
    field_name: String, 
    language: String, 
    record_id: Option<String>, 
    record_sub_id: Option<String>, 
    field_value: Option<String>
}   

struct TranslationPivotRow {
    count: u64,
    table_name: String,
    field_name: String, 
    language: String, 
    has_record_id: bool, 
    has_record_sub_id: bool, 
    has_field_value: bool
}

#[derive(Hash)]
struct TranslationHashKey {
    table_name: String,
    field_name: String, 
    language: String, 
    has_record_id: bool, 
    has_record_sub_id: bool, 
    has_field_value: bool
}

fn main() {
    let inputpath = arguments::parse(std::env::args())
        .expect("Add an unzipped folder path via --input PATH")
        .get::<String>("input");

    let path = fs::read_dir(inputpath.unwrap().as_str()).unwrap();

    for entry in path {
        let entry = entry.unwrap();
        if entry.path().is_dir() {
            let feed_dir = fs::read_dir(entry.path()).unwrap();
        }
    }

    
}
