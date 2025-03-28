use std::sync::OnceLock;

use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;
use fluvio_smartmodule::{smartmodule, RecordData, Result, SmartModuleRecord};

static INDEX_NAME: OnceLock<String> = OnceLock::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    let index_name = params.get("index_name").unwrap().to_string();
    INDEX_NAME.set(index_name.clone()).unwrap();
    Ok(())
}

#[smartmodule(array_map)]
pub fn array_map(record: &SmartModuleRecord) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    // Deserialize a JSON array with any kind of values inside
    let json = serde_json::from_str::<serde_json::Value>(record.value.to_string().as_str())?;

    let strings = json
        .get("result")
        .unwrap()
        .as_array()
        .unwrap()
        .into_iter()
        .map(|value| value.as_array().unwrap())
        .map(|arr| {
            format!(
                "deribit_rv,index_name={} rv={} {}000000",
                INDEX_NAME.get().unwrap(),
                arr.get(1).unwrap().as_f64().unwrap(),
                arr.get(0).unwrap().as_f64().unwrap()
            )
        })
        .collect::<Vec<String>>();

    // Create one record from each JSON string to send
    let kvs: Vec<(Option<RecordData>, RecordData)> = strings
        .into_iter()
        .map(|s| (None, RecordData::from(s)))
        .collect();
    Ok(kvs)
}
