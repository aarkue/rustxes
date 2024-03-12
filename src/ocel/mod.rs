use std::collections::{HashMap, HashSet};

use chrono::DateTime;
use polars::{prelude::*, series::Series};
use process_mining::{
    import_ocel_json_from_path, import_ocel_xml_file, ocel::ocel_struct::OCELAttributeValue, OCEL,
};
use pyo3::{pyfunction, PyResult};
use pyo3_polars::PyDataFrame;

fn ocel_attribute_val_to_any_value<'a>(
    val: &'a OCELAttributeValue,
    utc_tz: &'a Option<String>,
) -> AnyValue<'a> {
    match val {
        OCELAttributeValue::String(s) => AnyValue::StringOwned(s.into()),
        OCELAttributeValue::Time(t) => AnyValue::Datetime(
            t.timestamp_nanos_opt().unwrap(),
            TimeUnit::Nanoseconds,
            utc_tz,
        ),
        OCELAttributeValue::Integer(i) => AnyValue::Int64(*i),
        OCELAttributeValue::Float(f) => AnyValue::Float64(*f),
        OCELAttributeValue::Boolean(b) => AnyValue::Boolean(*b),
        OCELAttributeValue::Null => AnyValue::Null,
    }
}
pub const OCEL_EVENT_ID_KEY: &str = "ocel:eid";
pub const OCEL_EVENT_TYPE_KEY: &str = "ocel:activity";
pub const OCEL_EVENT_TIMESTAMP_KEY: &str = "ocel:timestamp";
pub const OCEL_OBJECT_ID_KEY: &str = "ocel:oid";
pub const OCEL_OBJECT_ID_2_KEY: &str = "ocel:oid_2";
pub const OCEL_OBJECT_TYPE_KEY: &str = "ocel:type";
pub const OCEL_QUALIFIER_KEY: &str = "ocel:qualifier";
pub const OCEL_CHANGED_FIELD_KEY: &str = "ocel:field";

pub struct OCEL2DataFrames {
    pub objects: DataFrame,
    pub events: DataFrame,
    pub object_changes: DataFrame,
    pub o2o: DataFrame,
    pub e2o: DataFrame,
}
pub fn ocel2_to_df(ocel: &OCEL) -> OCEL2DataFrames {
    let utc_tz = Some("UTC".to_string());
    let object_attributes: HashSet<String> = ocel
        .object_types
        .iter()
        .flat_map(|ot| &ot.attributes)
        .map(|at| at.name.clone())
        .collect();
    let actual_object_attributes: HashSet<String> = ocel
        .objects
        .iter()
        .flat_map(|o| o.attributes.iter().map(|oa| oa.name.clone()))
        .collect();
    // println!("Object attributes: {:?}; Actual object attributes: {:?}", object_attributes.len(), actual_object_attributes.len());
    if !object_attributes.is_superset(&actual_object_attributes) {
        eprintln!(
            "Warning: Global object attributes is not a superset of actual object attributes"
        );
    }
    let object_attributes_initial: HashSet<String> = object_attributes
        .clone()
        .into_iter()
        .filter(|a| {
            ocel.objects.iter().any(|o| {
                o.attributes
                    .iter()
                    .any(|oa| &oa.name == a && oa.time == DateTime::UNIX_EPOCH)
            })
        })
        .collect();
    let objects_df = DataFrame::from_iter(
        object_attributes_initial
            .into_iter()
            .map(|name| {
                Series::from_any_values(
                    &name,
                    ocel.objects
                        .iter()
                        .map(|o| {
                            let attr = o
                                .attributes
                                .iter()
                                .find(|a| a.name == name && a.time == DateTime::UNIX_EPOCH);
                            let val = match attr {
                                Some(v) => &v.value,
                                None => &OCELAttributeValue::Null,
                            };
                            ocel_attribute_val_to_any_value(val, &utc_tz)
                        })
                        .collect::<Vec<_>>()
                        .as_ref(),
                    false,
                )
                .unwrap()
            })
            .chain(vec![
                Series::from_any_values(
                    OCEL_OBJECT_ID_KEY,
                    &ocel
                        .objects
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.id.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_OBJECT_TYPE_KEY,
                    &ocel
                        .objects
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.object_type.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
            ]),
    );

    let all_evs_with_rels: Vec<_> = ocel
        .events
        .iter()
        .flat_map(|e| {
            e.relationships
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(move |r| (e, r))
        })
        .collect();

    let obj_id_to_type_map: HashMap<&String, &String> = ocel
        .objects
        .iter()
        .map(|o| (&o.id, &o.object_type))
        .collect();

    let mut e2o_df = DataFrame::from_iter(vec![
        Series::from_any_values(
            OCEL_EVENT_ID_KEY,
            &all_evs_with_rels
                .iter()
                .map(|(e, _r)| AnyValue::StringOwned(e.id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_EVENT_TYPE_KEY,
            &all_evs_with_rels
                .iter()
                .map(|(e, _r)| AnyValue::StringOwned(e.event_type.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_EVENT_TIMESTAMP_KEY,
            &all_evs_with_rels
                .iter()
                .map(|(e, _r)| {
                    AnyValue::Datetime(
                        e.time.timestamp_nanos_opt().unwrap(),
                        TimeUnit::Nanoseconds,
                        &utc_tz,
                    )
                })
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_OBJECT_ID_KEY,
            &all_evs_with_rels
                .iter()
                .map(|(_e, r)| AnyValue::StringOwned(r.object_id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_OBJECT_TYPE_KEY,
            &all_evs_with_rels
                .iter()
                .map(|(_e, r)| {
                    if let Some(obj_type) = obj_id_to_type_map.get(&r.object_id) {
                        AnyValue::StringOwned((*obj_type).into())
                    } else {
                        eprintln!(
                            "Invalid object id in E2O reference: Event: {}, Object: {}",
                            _e.id, r.object_id
                        );
                        AnyValue::Null
                    }
                })
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_QUALIFIER_KEY,
            &all_evs_with_rels
                .iter()
                .map(|(_e, r)| AnyValue::StringOwned(r.qualifier.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
    ]);

    let all_obj_with_rels: Vec<_> = ocel
        .objects
        .iter()
        .flat_map(|o| {
            o.relationships
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(move |r| (o, r))
        })
        .collect();

    let o2o_df = DataFrame::from_iter(vec![
        Series::from_any_values(
            OCEL_OBJECT_ID_KEY,
            &all_obj_with_rels
                .iter()
                .map(|(o, _r)| AnyValue::StringOwned(o.id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_OBJECT_ID_2_KEY,
            &all_obj_with_rels
                .iter()
                .map(|(_o, r)| AnyValue::StringOwned(r.object_id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_QUALIFIER_KEY,
            &all_obj_with_rels
                .iter()
                .map(|(_o, r)| AnyValue::StringOwned(r.qualifier.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
    ]);

    let mut object_changes_df = DataFrame::from_iter(
        object_attributes
            .into_iter()
            .map(|name| {
                Series::from_any_values(
                    &name,
                    ocel.objects
                        .iter()
                        .flat_map(|o| {
                            o.attributes.iter()
                            // .filter(|a| a.time != DateTime::UNIX_EPOCH)
                        })
                        .map(|a| {
                            if a.name == name {
                                ocel_attribute_val_to_any_value(&a.value, &utc_tz)
                            } else {
                                AnyValue::Null
                            }
                        })
                        .collect::<Vec<_>>()
                        .as_ref(),
                    false,
                )
                .unwrap()
            })
            .chain(vec![
                Series::from_any_values(
                    OCEL_OBJECT_ID_KEY,
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| vec![o.id.clone(); o.attributes.len()])
                        .map(|o_id| AnyValue::StringOwned(o_id.into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_OBJECT_TYPE_KEY,
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| vec![o.object_type.clone(); o.attributes.len()])
                        .map(|o_type| AnyValue::StringOwned(o_type.into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_CHANGED_FIELD_KEY,
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| {
                            o.attributes
                                .iter()
                                // .filter(|oa| oa.time != DateTime::UNIX_EPOCH)
                                .map(|oa| oa.name.clone())
                        })
                        .map(|chngd_field_name| AnyValue::StringOwned(chngd_field_name.into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_EVENT_TIMESTAMP_KEY,
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| {
                            o.attributes
                                .iter()
                                // .filter(|oa| oa.time != DateTime::UNIX_EPOCH)
                                .map(|oa| oa.time)
                        })
                        .map(|date| {
                            AnyValue::Datetime(
                                date.timestamp_nanos_opt().unwrap(),
                                TimeUnit::Nanoseconds,
                                &utc_tz,
                            )
                        })
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
            ]),
    );
    let event_attributes: HashSet<String> = ocel
        .event_types
        .iter()
        .flat_map(|et| &et.attributes)
        .map(|at| at.name.clone())
        .collect();
    let mut events_df = DataFrame::from_iter(
        event_attributes
            .into_iter()
            .map(|name| {
                Series::from_any_values(
                    &name,
                    ocel.events
                        .iter()
                        .map(|e| {
                            let attr = e.attributes.iter().find(|a| a.name == name);
                            let val = match attr {
                                Some(v) => &v.value,
                                None => &OCELAttributeValue::Null,
                            };
                            ocel_attribute_val_to_any_value(val, &utc_tz)
                        })
                        .collect::<Vec<_>>()
                        .as_ref(),
                    false,
                )
                .unwrap()
            })
            .chain(vec![
                Series::from_any_values(
                    OCEL_EVENT_ID_KEY,
                    &ocel
                        .events
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.id.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_EVENT_TYPE_KEY,
                    &ocel
                        .events
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.event_type.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_EVENT_TIMESTAMP_KEY,
                    &ocel
                        .events
                        .iter()
                        .map(|o| {
                            AnyValue::Datetime(
                                o.time.timestamp_nanos_opt().unwrap(),
                                TimeUnit::Nanoseconds,
                                &utc_tz,
                            )
                        })
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
            ]),
    );
    events_df
        .sort_in_place(vec![OCEL_EVENT_TIMESTAMP_KEY], false, true)
        .unwrap();

    e2o_df
        .sort_in_place(vec![OCEL_EVENT_TIMESTAMP_KEY], false, true)
        .unwrap();

    object_changes_df
        .sort_in_place(vec![OCEL_EVENT_TIMESTAMP_KEY], false, true)
        .unwrap();
    OCEL2DataFrames {
        objects: objects_df,
        events: events_df,
        object_changes: object_changes_df,
        o2o: o2o_df,
        e2o: e2o_df,
    }
}

pub fn ocel_dfs_to_py(ocel_dfs: OCEL2DataFrames) -> HashMap<String, PyDataFrame> {
    let mut res: HashMap<String, PyDataFrame> = HashMap::with_capacity(5);
    res.insert("events".to_string(), PyDataFrame(ocel_dfs.events));
    res.insert("objects".to_string(), PyDataFrame(ocel_dfs.objects));
    res.insert("o2o".to_string(), PyDataFrame(ocel_dfs.o2o));
    res.insert("relations".to_string(), PyDataFrame(ocel_dfs.e2o));
    res.insert(
        "object_changes".to_string(),
        PyDataFrame(ocel_dfs.object_changes),
    );
    res
}

#[pyfunction]
pub fn import_ocel_xml_rs(path: String) -> PyResult<HashMap<String, PyDataFrame>> {
    let ocel = import_ocel_xml_file(&path);
    let ocel_dfs = ocel2_to_df(&ocel);
    Ok(ocel_dfs_to_py(ocel_dfs))
}

#[pyfunction]
pub fn import_ocel_json_rs(path: String) -> PyResult<HashMap<String, PyDataFrame>> {
    let ocel = import_ocel_json_from_path(&path).unwrap();
    let ocel_dfs = ocel2_to_df(&ocel);
    Ok(ocel_dfs_to_py(ocel_dfs))
}
