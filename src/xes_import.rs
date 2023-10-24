use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::str::FromStr;

use chrono::DateTime;
use flate2::bufread::GzDecoder;
use quick_xml::events::BytesStart;
use quick_xml::Reader;
use uuid::Uuid;

use crate::event_log_struct::{
    Attribute, AttributeAddable, AttributeValue, Attributes, Event, EventLog, EventLogClassifier,
    EventLogExtension, Trace,
};

///
/// Current Parsing Mode (i.e., which tag is currently open / being parsed)
///
#[derive(Clone, Copy, Debug)]
enum Mode {
    Trace,
    Event,
    Attribute,
    Global,
    None,
}

fn parse_attribute_from_tag(t: &BytesStart, mode: Mode) -> (String, AttributeValue) {
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
        b"container" => Some(AttributeValue::Container(HashMap::new())),
        b"list" => Some(AttributeValue::List(Vec::new())),
        _ => {
            match mode {
                Mode::None => {
                    // We do not parse any log-level attributes etc.
                    None
                }
                m => {
                    let mut name_str = String::new();
                    t.name().as_ref().read_to_string(&mut name_str).unwrap();
                    eprintln!(
                        "Attribute type not implemented '{}' in mode {:?}",
                        name_str, m
                    );
                    None
                }
            }
        }
    };
    return (key, attribute_val.unwrap_or(AttributeValue::None()));
}

///
/// Parse an attribute from a tag (reading the "key" and "value" fields) and parsing the inner value
///
fn add_attribute_from_tag(
    t: &BytesStart,
    mode: Mode,
    log: &mut EventLog,
    current_nested_attributes: &mut Vec<Attribute>,
) {
    let (key, val) = parse_attribute_from_tag(t, mode);
    match mode {
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
        Mode::Attribute => {
            let last_attr = current_nested_attributes.last_mut().unwrap();
            last_attr.value = match last_attr.value.clone() {
                AttributeValue::List(mut l) => {
                    l.push(Attribute {
                        key,
                        value: val,
                        own_attributes: None,
                    });
                    AttributeValue::List(l)
                }
                AttributeValue::Container(mut c) => {
                    c.add_to_attributes(key, val);
                    AttributeValue::Container(c)
                }
                x => {
                    last_attr
                        .own_attributes
                        .as_mut()
                        .unwrap()
                        .add_to_attributes(key, val);
                    x
                }
            };
        }
        Mode::Global => {}
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
    let mut last_mode_before_attr: Mode = Mode::None;

    let mut log = EventLog {
        attributes: Attributes::new(),
        traces: Vec::new(),
        extensions: Some(Vec::new()),
        classifiers: Some(Vec::new()),
    };
    let mut current_nested_attributes: Vec<Attribute> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(r) => {
                match r {
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
                        b"global" => {
                            last_mode_before_attr = current_mode;
                            current_mode = Mode::Global;
                        }
                        x => {
                            match x {
                                b"log" => {}
                                _ => {
                                    // Nested attribute!
                                    let (key, value) = parse_attribute_from_tag(&t, current_mode);
                                    if !(key == "" && matches!(value, AttributeValue::None())) {
                                        current_nested_attributes.push(Attribute {
                                            key,
                                            value,
                                            own_attributes: Some(Attributes::new()),
                                        });
                                        match current_mode {
                                            Mode::Attribute => {}
                                            m => {
                                                last_mode_before_attr = m;
                                            }
                                        }
                                        current_mode = Mode::Attribute;
                                    }
                                }
                            }
                        }
                    },
                    quick_xml::events::Event::Empty(t) => match t.name().as_ref() {
                        b"extension" => {
                            let mut name = String::new();
                            let mut prefix = String::new();
                            let mut uri = String::new();
                            t.attributes().for_each(|a| {
                                let x = a.unwrap();
                                match x.key.as_ref() {
                                    b"name" => {
                                        x.value.as_ref().read_to_string(&mut name).unwrap();
                                    }
                                    b"prefix" => {
                                        x.value.as_ref().read_to_string(&mut prefix).unwrap();
                                    }
                                    b"uri" => {
                                        x.value.as_ref().read_to_string(&mut uri).unwrap();
                                    }
                                    _ => {}
                                }
                            });
                            log.extensions.as_mut().unwrap().push(EventLogExtension {
                                name,
                                prefix,
                                uri,
                            })
                        }
                        b"classifier" => {
                            let mut name = String::new();
                            let mut keys = String::new();
                            t.attributes().for_each(|a| {
                                let x = a.unwrap();
                                match x.key.as_ref() {
                                    b"name" => {
                                        x.value.as_ref().read_to_string(&mut name).unwrap();
                                    }
                                    b"keys" => {
                                        x.value.as_ref().read_to_string(&mut keys).unwrap();
                                    }
                                    _ => {}
                                }
                            });
                            log.classifiers.as_mut().unwrap().push(EventLogClassifier {
                                name,
                                keys: keys.split(" ").map(|s| s.to_string()).collect(),
                            })
                        }
                        _ => add_attribute_from_tag(
                            &t,
                            current_mode,
                            &mut log,
                            &mut current_nested_attributes,
                        ),
                    },
                    quick_xml::events::Event::End(t) => {
                        let mut t_string = String::new();
                        t.as_ref().read_to_string(&mut t_string).unwrap();
                        match t_string.as_str() {
                            "event" => current_mode = Mode::Trace,
                            "trace" => current_mode = Mode::None,
                            "log" => current_mode = Mode::None,
                            "global" => current_mode = last_mode_before_attr,
                            _ => match current_mode {
                                Mode::Attribute => {
                                    if current_nested_attributes.len() >= 1 {
                                        let attr = current_nested_attributes.pop().unwrap();
                                        if current_nested_attributes.len() >= 1 {
                                            current_nested_attributes
                                                .last_mut()
                                                .unwrap()
                                                .own_attributes
                                                .as_mut()
                                                .unwrap()
                                                .insert(attr.key.clone(), attr);
                                        } else {
                                            match last_mode_before_attr {
                                                Mode::Trace => {
                                                    log.traces
                                                        .last_mut()
                                                        .unwrap()
                                                        .attributes
                                                        .insert(attr.key.clone(), attr);
                                                }
                                                Mode::Event => {
                                                    log.traces
                                                        .last_mut()
                                                        .unwrap()
                                                        .events
                                                        .last_mut()
                                                        .unwrap()
                                                        .attributes
                                                        .insert(attr.key.clone(), attr);
                                                }
                                                Mode::None => {
                                                    log.attributes.insert(attr.key.clone(), attr);
                                                }
                                                x => {
                                                    panic!("Invalid Mode! {:?}; This should not happen!",x);
                                                }
                                            }
                                            current_mode = last_mode_before_attr;
                                        }
                                    } else {
                                        // This means there was no current nested attribute but the mode indicated otherwise
                                        // Should thus not happen, but execution can continue.
                                        eprintln!("[Rust] Warning: Attribute mode but no open nested attributes!");
                                        current_mode = last_mode_before_attr;
                                    }
                                }
                                _ => current_mode = Mode::None,
                            },
                        }
                    }
                    quick_xml::events::Event::Eof => break,
                    _ => {}
                }
            }
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
