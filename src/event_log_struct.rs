use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use uuid::Uuid;


use chrono::serde::ts_milliseconds;
/// 
/// Possible attribute values according to the XES Standard
/// 
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
pub enum AttributeValue {
    String(String),
    #[serde(with = "ts_milliseconds")]
    Date(DateTime<Utc>),
    Int(i64),
    Float(f64),
    Boolean(bool),
    ID(Uuid),
    List(Vec<Attribute>),
    Container(Attributes),
    None(),
}

///
/// Attribute made up of the key and value
/// 
/// Depending on usage, the key field might be redundant but useful for some implementations
/// 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
    pub own_attributes: Option<Attributes>
}
impl Attribute {
    ///
    /// Helper to create a new attribute
    /// 
    pub fn new(key: String, attribute_val: AttributeValue) -> Self {
        Self {
            key,
            value: attribute_val,
            own_attributes: None
        }
    }
    ///
    /// Helper to create a new attribute, while returning the key String additionally
    /// 
    /// This is useful for directly inserting the attribute in a [HashMap] afterwards
    /// 
    pub fn new_with_key(key: String, attribute_val: AttributeValue) -> (String, Self) {
        (
            key.clone(),
            Self {
                key,
                value: attribute_val,
                own_attributes: None
            },
        )
    }
}

///
/// Attributes are [HashMap] mapping a key ([String]) to an [Attribute]
/// 
pub type Attributes = HashMap<String, Attribute>;

/// 
/// Trait to easily add a new attribute
pub trait AttributeAddable {
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue) -> Option<&mut Attribute>;
}
impl AttributeAddable for Attributes {
    ///
    /// Add a new attribute (with key and value)
    /// 
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue) -> Option<&mut Attribute> {
        let (k, v) = Attribute::new_with_key(key, value);
        self.insert(k.clone(), v);
        return self.get_mut(&k);
    }
}

///
/// An event consists of multiple (event) attributes ([Attributes])
/// 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub attributes: Attributes,
}

///
/// A trace consists of a list of events and trace attributes (See also [Event] and [Attributes])
/// 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub attributes: Attributes,
    pub events: Vec<Event>,
}

///
/// A event log consists of a list of traces and log attributes (See also [Trace] and [Attributes])
/// 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLog {
    pub attributes: Attributes,
    pub traces: Vec<Trace>,
    pub extensions: Option<Vec<EventLogExtension>>,
    pub classifiers: Option<Vec<EventLogClassifier>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogExtension {
    pub name: String,
    pub prefix: String,
    pub uri: String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLogClassifier {
    pub name: String,
    pub keys: Vec<String>,
}