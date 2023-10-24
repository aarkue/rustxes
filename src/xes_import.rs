use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::str::FromStr;

use chrono::DateTime;
use flate2::bufread::GzDecoder;
use quick_xml::events::BytesStart;
use quick_xml::Reader;
use uuid::Uuid;

use crate::event_log_struct::{
    AttributeAddable, AttributeValue, Attributes, Event, EventLog, Trace,
};

///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
/// 
#[derive(Clone, Copy, Debug)]
enum Mode {
    Trace,
    Event,
    None,
}

///
/// Parse an attribute from a tag (reading the "key" and "value" fields) and parsing the inner value
/// 
fn add_attribute_from_tag(t: &BytesStart, mode: Mode, log: &mut EventLog) {
    let mut value = String::new();
    let mut key = String::new();
    t.attributes().for_each(|a| {
        let x = a.unwrap();
        match x.key.as_ref() {
            b"key" => {
                x.value.as_ref().read_to_string(&mut key).unwrap();
            }
            b"value" => {
                x.value.as_ref().read_to_string(&mut value).unwrap();
            }
            _ => {}
        }
    });
    let attribute_val: Option<AttributeValue> = match t.name().as_ref() {
        b"string" => Some(AttributeValue::String(value)),
        b"date" => Some(AttributeValue::Date(
            DateTime::parse_from_rfc3339(&value).unwrap().into(),
        )),
        b"int" => Some(AttributeValue::Int(value.parse::<i64>().unwrap())),
        b"float" => Some(AttributeValue::Float(value.parse::<f64>().unwrap())),
        b"boolean" => Some(AttributeValue::Boolean(value.parse::<bool>().unwrap())),
        b"id" => Some(AttributeValue::ID(Uuid::from_str(&value).unwrap())),
        _ => {
            match mode {
                Mode::None => {
                    // We do not parse any log-level attributes etc.
                    None
                }
                m => {
                    let mut name_str = String::new();
                    t.name().as_ref().read_to_string(&mut name_str).unwrap();
                    // TODO: Implement other attribute value types
                    eprintln!(
                        "Attribute type not implemented {} in mode {:?}",
                        name_str, m
                    );
                    None
                }
            }
        }
    };
    match attribute_val {
        Some(val) => match mode {
            Mode::Trace => {
                log.traces
                    .last_mut()
                    .unwrap()
                    .attributes
                    .add_to_attributes(key, val);
            }
            Mode::Event => {
                log.traces
                    .last_mut()
                    .unwrap()
                    .events
                    .last_mut()
                    .unwrap()
                    .attributes
                    .add_to_attributes(key, val);
            }

            Mode::None => {
                log.attributes.add_to_attributes(key, val);
            }
        },
        None => {}
    }
}

/// 
/// Import an XES [EventLog] from a [Reader] 
/// 
pub fn import_xes<T>(reader: &mut Reader<T>) -> EventLog
where
    T: BufRead,
{
    reader.trim_text(true);
    let mut buf: Vec<u8> = Vec::new();

    let mut current_mode: Mode = Mode::None;

    let mut log = EventLog {
        attributes: Attributes::new(),
        traces: Vec::new(),
    };

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(r) => match r {
                quick_xml::events::Event::Start(t) => match t.name().as_ref() {
                    b"trace" => {
                        current_mode = Mode::Trace;
                        log.traces.push(Trace {
                            attributes: Attributes::new(),
                            events: Vec::new(),
                        });
                    }
                    b"event" => {
                        current_mode = Mode::Event;
                        log.traces.last_mut().unwrap().events.push(Event {
                            attributes: Attributes::new(),
                        });
                    }
                    _ => {
                        add_attribute_from_tag(&t, current_mode, &mut log);
                    }
                },
                quick_xml::events::Event::Empty(t) => {
                    add_attribute_from_tag(&t, current_mode, &mut log)
                }
                quick_xml::events::Event::Eof => break,
                _ => {}
            },
            Err(e) => {
                eprintln!("[Rust] Error occured when parsing XES: {:?}", e);
            }
        }
    }
    buf.clear();
    return log;
}



/// 
/// Import an XES [EventLog] from a file path 
/// 
pub fn import_xes_file(path: &str) -> EventLog {
    if path.ends_with(".gz") {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(&file);
        let mut dec = GzDecoder::new(reader);
        let mut s = String::new();
        dec.read_to_string(&mut s).unwrap();
        let mut reader: Reader<&[u8]> = Reader::from_str(&s);
        return import_xes(&mut reader);
    } else {
        let mut reader: Reader<BufReader<std::fs::File>> = Reader::from_file(path).unwrap();
        return import_xes(&mut reader);
    }
}

/// 
/// Import an XES [EventLog] directly from a string
/// 
pub fn import_xes_str(xes_str: &str) -> EventLog {
    let mut reader: Reader<&[u8]> = Reader::from_str(&xes_str);
    return import_xes(&mut reader);
}
