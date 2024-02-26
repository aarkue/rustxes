use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use polars::{
    prelude::*,
    series::Series,
};
use process_mining::{
    event_log::{
        stream_xes::XESOuterLogData, Attribute, AttributeAddable, AttributeValue, Trace,
    },
    import_xes_file, EventLog, XESImportOptions,
};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};



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
        AttributeValue::String(v) => AnyValue::Utf8Owned(v.into()),
        AttributeValue::Date(v) => {
            return AnyValue::Datetime(
                v.timestamp_nanos_opt().unwrap(),
                polars::prelude::TimeUnit::Nanoseconds,
                utc_tz,
            )
        }
        AttributeValue::Int(v) => AnyValue::Int64(*v),
        AttributeValue::Float(v) => AnyValue::Float64(*v),
        AttributeValue::Boolean(v) => AnyValue::Boolean(*v),
        AttributeValue::ID(v) => {
            let s = v.to_string();
            AnyValue::Utf8Owned(s.into())
        }
        // TODO: Add proper List/Container support
        AttributeValue::List(l) => AnyValue::Utf8Owned(format!("{:?}", l).into()),
        AttributeValue::Container(c) => AnyValue::Utf8Owned(format!("{:?}", c).into()),
        AttributeValue::None() => AnyValue::Null,
    }
}

///
/// Convert an [EventLog] to a Polars [DataFrame]
///
/// Flattens event log and adds trace-level attributes to events with prefixed attribute key (see [TRACE_PREFIX])
///
pub fn convert_log_to_df(log: &EventLog, print_debug: Option<bool>) -> Result<DataFrame, PolarsError> {
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
                                    &log.global_trace_attrs.as_ref()
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
                                        .get_by_key_or_global(k, &log.global_event_attrs.as_ref()),
                                    &utc_tz,
                                )
                            })
                            .collect()
                    }
                })
                .collect();

            let mut unique_dtypes: HashSet<DataType> = entries.iter().map(|v| v.dtype()).collect();
            unique_dtypes.remove(&DataType::Unknown);
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
                            AnyValue::Utf8Owned(s) => AnyValue::Utf8Owned(s),
                            x => AnyValue::Utf8Owned(x.to_string().into()),
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
    let df = DataFrame::new(x).unwrap();
    if print_debug.is_some_and(|a| a) {
        println!(
            "Constructing DF from Attribute Series took {:.2?}",
            now.elapsed()
        );
    }
    Ok(df)
}

pub fn convert_trace_stream_to_df<I>(trace_stream: I) -> Result<DataFrame, PolarsError>
where
    I: Iterator<Item = Trace>,
{
    let utc_tz = Some("UTC".to_string());
    let mut series_per_key: HashMap<String, Series> = HashMap::new();
    let mut visited_events = 0;
    for trace in trace_stream {
        for event in trace.events {
            for (key, series) in &mut series_per_key {
                let ss = Series::from_any_values_and_dtype(
                    key,
                    &[attribute_to_any_value(
                        event.attributes.get_by_key(key),
                        &utc_tz,
                    )],
                    &DataType::Utf8,
                    false,
                )
                .unwrap();
                series.append(&ss).unwrap();
            }
            for attr in &event.attributes {
                if !series_per_key.contains_key(&attr.key) {
                    let mut series = Series::full_null(&attr.key, visited_events, &DataType::Utf8);
                    let ss = Series::from_any_values_and_dtype(
                        &attr.key,
                        &[attribute_to_any_value(
                            event.attributes.get_by_key(&attr.key),
                            &utc_tz,
                        )],
                        &DataType::Utf8,
                        false,
                    )
                    .unwrap();
                    series.append(&ss).unwrap();
                    series_per_key.insert(attr.key.clone(), series);
                }
            }
            visited_events += 1;
        }
    }

    let serieses = series_per_key.into_values().collect();
    DataFrame::new(serieses)
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
    // add_start_end_acts(&mut log);
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
    Ok(())
}
