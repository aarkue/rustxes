use std::collections::HashMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;



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

#[derive(Debug, Clone)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
}
impl Attribute {
    pub fn new(key: String, attribute_val: AttributeValue) -> Self {
        Self {
            key,
            value: attribute_val,
        }
    }
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
pub type Attributes = HashMap<String, Attribute>;
pub trait AttributeAddable {
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue);
}

impl AttributeAddable for Attributes {
    fn add_to_attributes(self: &mut Self, key: String, value: AttributeValue) {
        let (k, v) = Attribute::new_with_key(key, value);
        self.insert(k, v);
    }
}
pub fn add_to_attributes(attributes: &mut Attributes, key: String, value: AttributeValue) {
    let (k, v) = Attribute::new_with_key(key, value);
    attributes.insert(k, v);
}

pub fn to_attributes(from: HashMap<String, AttributeValue>) -> Attributes {
    from.into_iter()
        .map(|(key, value)| {
            (
                key.clone(),
                Attribute {
                    key: key.clone(),
                    value,
                },
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct Event {
    pub attributes: Attributes,
}
#[derive(Debug, Clone)]
pub struct Trace {
    pub attributes: Attributes,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone)]
pub struct EventLog {
    pub attributes: Attributes,
    pub traces: Vec<Trace>,
}