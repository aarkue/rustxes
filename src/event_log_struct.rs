use std::collections::HashMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;


/// 
/// Possible attribute values according to the XES Standard
/// 
#[derive(Debug, Clone)]
pub enum AttributeValue {
    String(String),
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
#[derive(Debug, Clone)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
}
impl Attribute {
    ///
    /// Helper to create a new attribute
    /// 
    pub fn new(key: String, attribute_val: AttributeValue) -> Self {
        Self {
            key,
            value: attribute_val,
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
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue);
}
impl AttributeAddable for Attributes {
    ///
    /// Add a new attribute (with key and value)
    /// 
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue) {
        let (k, v) = Attribute::new_with_key(key, value);
        self.insert(k, v);
    }
}

///
/// An event consists of multiple (event) attributes ([Attributes])
/// 
#[derive(Debug, Clone)]
pub struct Event {
    pub attributes: Attributes,
}

///
/// A trace consists of a list of events and trace attributes (See also [Event] and [Attributes])
/// 
#[derive(Debug, Clone)]
pub struct Trace {
    pub attributes: Attributes,
    pub events: Vec<Event>,
}

///
/// A event log consists of a list of traces and log attributes (See also [Trace] and [Attributes])
/// 
#[derive(Debug, Clone)]
pub struct EventLog {
    pub attributes: Attributes,
    pub traces: Vec<Trace>,
}