use std::time::Instant;

use process_mining::{
    convert_dataframe_to_log, convert_log_to_dataframe, event_log::stream_xes::XESOuterLogData,
    export_xes_event_log_to_file_path, import_xes_file, XESImportOptions,
};
use pyo3::{exceptions::PyTypeError, prelude::*};
use pyo3_polars::PyDataFrame;

use crate::ocel::{import_ocel_json_rs, import_ocel_xml_rs};

mod ocel;
mod test;

///
/// Import an XES event log
///
/// Returns a tuple of a Polars [DataFrame] for the event data and a json-encoding of  all log attributes/extensions/classifiers
///
/// * `path` - The filepath of the .xes or .xes.gz file to import
/// * `date_format` - Optional date format to use for parsing <date> tags (See https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
/// * `print_debug` - Optional flag to enable debug print outputs
///
///
#[pyfunction]
#[pyo3(signature = (path, date_format=None, print_debug=None))]
fn import_xes_rs(
    path: String,
    date_format: Option<String>,
    print_debug: Option<bool>,
) -> PyResult<(PyDataFrame, String)> {
    if print_debug.is_some_and(|a| a) {
        println!("Starting XES Import");
    }
    let start_now = Instant::now();
    let mut now = Instant::now();
    let log = import_xes_file(
        &path,
        XESImportOptions {
            date_format,
            ..Default::default()
        },
    )
    .unwrap();
    if print_debug.is_some_and(|a| a) {
        println!("Importing XES Log took {:.2?}", now.elapsed());
    }
    now = Instant::now();
    let other_data = XESOuterLogData {
        log_attributes: log.attributes.clone(),
        extensions: log.extensions.clone().unwrap_or_default().clone(),
        classifiers: log.classifiers.clone().unwrap_or_default().clone(),
        global_trace_attrs: log.global_trace_attrs.clone().unwrap_or_default(),
        global_event_attrs: log.global_event_attrs.clone().unwrap_or_default(),
    };
    let converted_log = convert_log_to_dataframe(&log, print_debug.unwrap_or_default()).unwrap();
    if print_debug.is_some_and(|a| a) {
        println!("Finished Converting Log; Took {:.2?}", now.elapsed());
    }
    if print_debug.is_some_and(|a| a) {
        println!("Total duration: {:.2?}", start_now.elapsed());
    }
    Ok((
        PyDataFrame(converted_log),
        serde_json::to_string(&other_data).unwrap(),
    ))
}

#[pyfunction]
// #[pyo3(signature = (df, path))]
fn export_xes_rs(df: PyDataFrame, path: String) -> PyResult<()> {
    let df: polars::frame::DataFrame = df.into();
    let log = convert_dataframe_to_log(&df)
        .map_err(|e| PyTypeError::new_err(format!("Failed to convert dataframe to log: {e:?}")))?;

    export_xes_event_log_to_file_path(&log, path)
        .map_err(|e| PyTypeError::new_err(format!("Failed to export XES: {e:?}")))
}

/// Python Module
#[pymodule]
fn rustxes(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(import_xes_rs, m)?)?;
    m.add_function(wrap_pyfunction!(export_xes_rs, m)?)?;
    m.add_function(wrap_pyfunction!(import_ocel_xml_rs, m)?)?;
    m.add_function(wrap_pyfunction!(import_ocel_json_rs, m)?)?;
    Ok(())
}
