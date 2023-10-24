use std::{collections::HashSet, time::Instant};

use event_log_struct::{Attribute, AttributeValue, EventLog};
use polars::{
    prelude::{AnyValue, DataFrame, NamedFrom, PolarsError},
    series::Series,
};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

pub mod event_log_struct;
pub mod xes_import;

pub const TRACE_PREFIX: &str = "case:";

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
        AttributeValue::List(l) => AnyValue::Utf8Owned(format!("{:?}",l).into()),
        AttributeValue::Container(c) =>  AnyValue::Utf8Owned(format!("{:?}",c).into()),
        AttributeValue::None() => AnyValue::Null,
    }
}

fn convert_log_to_df(log: &EventLog) -> Result<DataFrame, PolarsError> {
    println!("Starting converting log to DataFrame");
    let mut now = Instant::now();
    let mut all_attributes: HashSet<String> = HashSet::new();
    log.traces.iter().for_each(|t| {
        t.attributes.keys().for_each(|s| {
            all_attributes.insert(TRACE_PREFIX.to_string() + s.as_str());
        });
        t.events.iter().for_each(|e| {
            e.attributes.keys().for_each(|s| {
                all_attributes.insert(s.into());
            });
        })
    });
    println!("Gathering all attributes took {:.2?}", now.elapsed());
    let utc_tz = Some("UTC".to_string());
    now = Instant::now();
    let x: Vec<Series> = all_attributes
        .par_iter()
        .map(|k| {
            let entries: Vec<AnyValue> = log
                .traces
                .iter()
                .map(|t| -> Vec<AnyValue> {
                    if k.starts_with(TRACE_PREFIX) {
                        let trace_k: String = k.chars().skip(TRACE_PREFIX.len()).collect();
                        vec![
                            attribute_to_any_value(t.attributes.get(&trace_k), &utc_tz);
                            t.events.len()
                        ]
                    } else {
                        t.events
                            .iter()
                            .map(|e| attribute_to_any_value(e.attributes.get(k), &utc_tz))
                            .collect()
                    }
                })
                .flatten()
                .collect();
            Series::new(k, &entries)
        })
        .collect();

    println!(
        "Creating a Series for every Attribute took {:.2?}",
        now.elapsed()
    );
    now = Instant::now();
    let df = DataFrame::new(x).unwrap();
    println!(
        "Constructing DF from Attribute Series took {:.2?}",
        now.elapsed()
    );
    return Ok(df);
}

#[pyfunction]
fn import_xes_rs(path: String) -> PyResult<PyDataFrame> {
    println!("Starting XES Import");
    let mut now = Instant::now();
    let log = xes_import::import_xes_file(&path);
    println!("Importing XES Log took {:.2?}", now.elapsed());
    now = Instant::now();
    // add_start_end_acts(&mut log);
    let converted_log = convert_log_to_df(&log).unwrap();
    println!("Finished Converting Log; Took {:.2?}", now.elapsed());
    Ok(PyDataFrame(converted_log))
}

/// Python Module
#[pymodule]
fn rustxes(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(import_xes_rs, m)?)?;
    Ok(())
}
