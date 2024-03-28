use std::{collections::HashSet, time::Instant};

use polars::{prelude::*, series::Series};
use process_mining::{
    event_log::{stream_xes::XESOuterLogData, Attribute, AttributeValue, XESEditableAttribute},
    import_xes_file, EventLog, XESImportOptions,
};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::ocel::{import_ocel_json_rs, import_ocel_xml_rs};

mod ocel;
mod test;

///
/// Prefix to attribute keys for trace-level attributes (e.g., when "flattening" the log to a [DataFrame])
///
pub const TRACE_PREFIX: &str = "case:";

///
/// Convert a attribute ([Attribute]) to an [AnyValue]
///
/// Used for converting values and data types to the DataFrame equivalent
///
/// The UTC timezone argument is used to correctly convert to AnyValue::Datetime with UTC timezone
///
fn attribute_to_any_value<'a>(
    from_option: Option<&Attribute>,
    utc_tz: &'a Option<String>,
) -> AnyValue<'a> {
    match from_option {
        Some(from) => {
            let x = attribute_value_to_any_value(&from.value, utc_tz);
            x
        }
        None => AnyValue::Null,
    }
}

///
/// Convert a attribute ([AttributeValue]) to an [AnyValue]
///
/// Used for converting values and data types to the DataFrame equivalent
///
/// The UTC timezone argument is used to correctly convert to AnyValue::Datetime with UTC timezone
///
fn attribute_value_to_any_value<'a>(
    from: &AttributeValue,
    utc_tz: &'a Option<String>,
) -> AnyValue<'a> {
    match from {
        AttributeValue::String(v) => AnyValue::StringOwned(v.into()),
        AttributeValue::Date(v) => {
            // Fallback for testing:
            // return AnyValue::StringOwned(v.to_string().into());
            return AnyValue::Datetime(
                v.timestamp_nanos_opt().unwrap(),
                polars::prelude::TimeUnit::Nanoseconds,
                utc_tz,
            );
        }
        AttributeValue::Int(v) => AnyValue::Int64(*v),
        AttributeValue::Float(v) => AnyValue::Float64(*v),
        AttributeValue::Boolean(v) => AnyValue::Boolean(*v),
        AttributeValue::ID(v) => {
            let s = v.to_string();
            AnyValue::StringOwned(s.into())
        }
        // TODO: Add proper List/Container support
        AttributeValue::List(l) => AnyValue::StringOwned(format!("{:?}", l).into()),
        AttributeValue::Container(c) => AnyValue::StringOwned(format!("{:?}", c).into()),
        AttributeValue::None() => AnyValue::Null,
    }
}

fn attribute_to_dtype(from: &AttributeValue, _utc_tz: &Option<String>) -> DataType {
    match from {
        AttributeValue::String(_) => DataType::String,
        AttributeValue::Date(_) => DataType::String, //DataType::Datetime(TimeUnit::Nanoseconds, utc_tz.clone()),
        AttributeValue::Int(_) => DataType::Int64,
        AttributeValue::Float(_) => DataType::Float64,
        AttributeValue::Boolean(_) => DataType::Boolean,
        AttributeValue::ID(_) => DataType::String,
        AttributeValue::List(_) => todo!(),
        AttributeValue::Container(_) => todo!(),
        AttributeValue::None() => DataType::Unknown,
    }
}
///
/// Convert an [EventLog] to a Polars [DataFrame]
///
/// Flattens event log and adds trace-level attributes to events with prefixed attribute key (see [TRACE_PREFIX])
///
pub fn convert_log_to_df(
    log: &EventLog,
    print_debug: Option<bool>,
) -> Result<DataFrame, PolarsError> {
    if print_debug.is_some_and(|a| a) {
        println!("Starting converting log to DataFrame");
    }
    let mut now = Instant::now();
    let all_attributes: HashSet<String> = log
        .traces
        .par_iter()
        .flat_map(|t| {
            let trace_attrs: HashSet<String> = t
                .attributes
                .iter()
                .map(|a| TRACE_PREFIX.to_string() + a.key.as_str())
                .collect();
            let m: HashSet<String> = t
                .events
                .iter()
                .flat_map(|e| {
                    e.attributes
                        .iter()
                        .map(|a| a.key.clone())
                        .collect::<Vec<String>>()
                })
                .collect();
            [trace_attrs, m]
        })
        .flatten()
        .collect();
    if print_debug.is_some_and(|a| a) {
        println!("Gathering all attributes took {:.2?}", now.elapsed());
    }
    let utc_tz = Some("UTC".to_string());
    now = Instant::now();
    let x: Vec<Series> = all_attributes
        .par_iter()
        .map(|k: &String| {
            let mut entries: Vec<AnyValue> = log
                .traces
                .iter()
                .flat_map(|t| -> Vec<AnyValue> {
                    if k.starts_with(TRACE_PREFIX) {
                        let trace_k: String = k.chars().skip(TRACE_PREFIX.len()).collect();
                        vec![
                            attribute_to_any_value(
                                t.attributes.get_by_key_or_global(
                                    &trace_k,
                                    &log.global_trace_attrs
                                ),
                                &utc_tz
                            );
                            t.events.len()
                        ]
                    } else {
                        t.events
                            .iter()
                            .map(|e| {
                                attribute_to_any_value(
                                    e.attributes
                                        .get_by_key_or_global(k, &log.global_event_attrs),
                                    &utc_tz,
                                )
                            })
                            .collect()
                    }
                })
                .collect();

            let mut unique_dtypes: HashSet<DataType> = entries.iter().map(|v| v.dtype()).collect();
            unique_dtypes.remove(&DataType::Unknown);
            unique_dtypes.remove(&DataType::Null);
            if unique_dtypes.len() > 1 {
                eprintln!(
                    "Warning: Attribute {} contains values of different dtypes ({:?})",
                    k, unique_dtypes
                );
                if unique_dtypes
                    == vec![DataType::Float64, DataType::Int64]
                        .into_iter()
                        .collect()
                {
                    entries = entries
                        .into_iter()
                        .map(|val| match val {
                            AnyValue::Int64(n) => AnyValue::Float64(n as f64),
                            x => x,
                        })
                        .collect();
                } else {
                    entries = entries
                        .into_iter()
                        .map(|val| match val {
                            AnyValue::Null => AnyValue::Null,
                            AnyValue::String(s) => AnyValue::String(s),
                            x => AnyValue::StringOwned(x.to_string().into()),
                        })
                        .collect();
                }
            }
            Series::new(k, entries)
        })
        .collect();
    if print_debug.is_some_and(|a| a) {
        println!(
            "Creating a Series for every Attribute took {:.2?}",
            now.elapsed()
        );
    }
    now = Instant::now();
    let df = unsafe { DataFrame::new_no_checks(x) };
    if print_debug.is_some_and(|a| a) {
        println!(
            "Constructing DF from Attribute Series took {:.2?}",
            now.elapsed()
        );
    }
    Ok(df)
}

///
/// Import an XES event log
///
/// Returns a tuple of a Polars [DataFrame] for the event data and a json-encoding of  all log attributes/extensions/classifiers
///
/// * `path` - The filepath of the .xes or .xes.gz file to import
/// * `date_format` - Optional date format to use for parsing <date> tags (See https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
/// * `print_debug` - Optional flag to enable debug print outputs
///
#[pyfunction]
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
    let converted_log = convert_log_to_df(&log, print_debug).unwrap();
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

/// Python Module
#[pymodule]
fn rustxes(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(import_xes_rs, m)?)?;
    m.add_function(wrap_pyfunction!(import_ocel_xml_rs, m)?)?;
    m.add_function(wrap_pyfunction!(import_ocel_json_rs, m)?)?;
    Ok(())
}
