#[cfg(test)]
mod xes_tests {
    use std::{
        collections::{HashMap, HashSet},
        time::Instant,
    };

    use polars::{
        datatypes::{AnyValue, Field},
        error::PolarsError,
        frame::DataFrame,
        prelude::NamedFrom,
        series::Series,
    };
    use process_mining::{
        event_log::{Trace, XESEditableAttribute}, import_ocel_xml_slice, import_xes_file,
        XESImportOptions,
    };

    use crate::{
        attribute_to_dtype, attribute_value_to_any_value, convert_log_to_df, ocel::ocel2_to_df,
    };

    #[test]
    fn basic_xes() {
        let now = Instant::now();
        let now_total = Instant::now();
        let log = import_xes_file(
            "/home/aarkue/doc/sciebo/alpha-revisit/BPI_Challenge_2017.xes",
            XESImportOptions {
                ..Default::default()
            },
        )
        .unwrap();
        let num_events = log.traces.iter().map(|t| t.events.len()).sum::<usize>();
        println!(
            "Got log with {} traces in {:?}",
            log.traces.len(),
            now.elapsed()
        );
        let now = Instant::now();
        let converted_log = convert_log_to_df(&log, Some(true)).unwrap();
        println!(
            "Converted to DF with shape {:?} in {:?}",
            converted_log.shape(),
            now.elapsed()
        );
        println!("Total: {:?}\n\n", now_total.elapsed());
        assert_eq!(converted_log.shape().0, num_events);
    }

    // #[test]
    // fn new_diag_xes() {
    //     let now = Instant::now();
    //     let (mut log, _) = stream_xes_from_path(
    //         "/home/aarkue/doc/sciebo/alpha-revisit/BPI_Challenge_2018.xes",
    //         XESImportOptions {
    //             ..Default::default()
    //         },
    //     )
    //     .unwrap();
    //     let df = convert_trace_stream_to_df_diagonal(&mut log).unwrap();
    //     println!(
    //         "Converted to DF with shape {:?} in {:?}",
    //         df.shape(),
    //         now.elapsed()
    //     );
    // }

    #[test]
    fn test_ocel2_container_df() {
        let now = Instant::now();
        let ocel_bytes = include_bytes!("../test_data/ContainerLogistics.xml");
        let ocel = import_ocel_xml_slice(ocel_bytes);
        let ocel_dfs = ocel2_to_df(&ocel);
        println!(
            "Got OCEL DF with {:?} objects in {:?}; Object change shape: {:?}; O2O shape: {:?}; E2O shape: {:?}",
            ocel_dfs.objects.shape(),
            now.elapsed(),
            ocel_dfs.object_changes.shape(),
            ocel_dfs.o2o.shape(),
            ocel_dfs.e2o.shape()
        );
        assert_eq!(ocel.objects.len(), 13910);
        assert_eq!(ocel.events.len(), 35413);


    }

    #[test]
    fn test_ocel2_df() {
        let now = Instant::now();
        let ocel_bytes = include_bytes!("../test_data/order-management.xml");
        let ocel = import_ocel_xml_slice(ocel_bytes);
        let ocel_dfs = ocel2_to_df(&ocel);
        println!(
            "Got OCEL DF with {:?} objects in {:?}; Object change shape: {:?}; O2O shape: {:?}; E2O shape: {:?}",
            ocel_dfs.objects.shape(),
            now.elapsed(),
            ocel_dfs.object_changes.shape(),
            ocel_dfs.o2o.shape(),
            ocel_dfs.e2o.shape()
        );

        // Assert DF shapes based on OCEL information
        assert_eq!(ocel.objects.len(), 10840);
        assert_eq!(ocel.objects.len(), ocel_dfs.objects.shape().0);

        assert_eq!(ocel.events.len(), 21008);
        assert_eq!(ocel.events.len(), ocel_dfs.events.shape().0);

        assert_eq!(
            ocel.events
                .iter()
                .flat_map(|ev| ev.relationships.clone().unwrap_or_default())
                .count(),
                ocel_dfs.e2o.shape().0
        );
        assert_eq!(ocel.events.len(), ocel_dfs.events.shape().0);

        // Known DF-shapes (match PM4PY implementation)
        assert_eq!(ocel_dfs.objects.shape(),(10840,2));
        assert_eq!(ocel_dfs.events.shape(),(21008,3));
        assert_eq!(ocel_dfs.e2o.shape(),(147463,6));
        assert_eq!(ocel_dfs.o2o.shape(),(28391,3));
        assert_eq!(ocel_dfs.object_changes.shape(),(18604,7));
    }

    // #[test]
    // fn pm4py_test_logs() {
    //     let paths = std::fs::read_dir("../../pm4py-core/tests/input_data/").unwrap();
    //     for path_el in paths {
    //         let path = path_el.unwrap().path();
    //         let path_str = path.to_str().unwrap();
    //         if path_str.ends_with(".xes") || path_str.ends_with(".xes.gz") {
    //             println!("Path: {}", path_str);
    //             let _log = import_xes_file(path_str, XESImportOptions::default());
    //             println!("Success!");
    //             // println!("{:#?}",_log);
    //         }
    //     }
    // }

    pub fn convert_trace_stream_to_df_diagonal<I>(trace_stream: I) -> Result<DataFrame, PolarsError>
    where
        I: Iterator<Item = Trace>,
    {
        let now = Instant::now();
        let utc_tz = Some("UTC".to_string());
        // let mut dfs: Vec<LazyFrame> = Vec::new();
        // let mut rows: Vec<Row> = Vec::new();
        let mut columns: HashMap<String, Vec<AnyValue>> = HashMap::new();
        // let mut attributes: Vec<String> = Vec::new();
        let mut attributes_set: HashSet<String> = HashSet::new();
        let mut fields: Vec<Field> = Vec::new();
        let mut prev_events = 0;
        for trace in trace_stream {
            for event in trace.events {
                for attr in &event.attributes {
                    if !attributes_set.contains(&attr.key) {
                        attributes_set.insert(attr.key.clone());
                        fields.push(Field::new(
                            &attr.key.clone(),
                            attribute_to_dtype(&attr.value, &utc_tz),
                        ));
                        // for row in &mut rows {
                        //     row.0.push(AnyValue::Null);
                        // }
                        columns.insert(attr.key.clone(), vec![AnyValue::Null; prev_events]);
                    }
                }
                columns.iter_mut().for_each(|(n, s)| {
                    s.push(match event.attributes.get_by_key(n) {
                        Some(v) => attribute_value_to_any_value(&v.value, &utc_tz),
                        None => AnyValue::Null,
                    })
                    // s.append(Series::new(n,))
                });
                // rows.push(Row::new(
                //         fields.iter()
                //         .map(|a| match event.attributes.get_by_key(&a.name){
                //             Some(v) => attribute_to_any_value(Some(v), &utc_tz),
                //             None => AnyValue::Null,
                //         })
                //         .collect(),
                // ));
                prev_events += 1;
                // let rows: Vec<Row> = vec![Row::new(
                //     event
                //         .attributes
                //         .iter()
                //         .map(|a| attribute_to_any_value(Some(&a), &utc_tz))
                //         .collect(),
                // )];
                // let schema = Schema::from_iter(
                //     event
                //         .attributes
                //         .iter()
                //         .map(|a| Field::new(&a.key, attribute_to_dtype(&a.value, &utc_tz))),
                // );
                // let new_df = DataFrame::from_rows_and_schema(&rows, &schema)
                //     .unwrap()
                //     .lazy();
                // dfs.push(new_df);
            }
        }
        println!("Got all dfs in {:?}!", now.elapsed());
        Ok(unsafe {
            DataFrame::new_no_checks(
                columns
                    .into_iter()
                    .map(|(n, s)| Series::new(&n, s))
                    .collect(),
            )
        })
        // DataFrame::from_rows(&rows)
        // DataFrame::from_rows_and_schema(&rows, &Schema::from_iter(fields.into_iter()))
        // concat_lf_diagonal(&dfs, UnionArgs::default())?.collect()
        // diag_concat_lf(&dfs, false, true)?.collect()
    }
}
