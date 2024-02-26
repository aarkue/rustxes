#[cfg(test)]
mod xes_tests {
    use std::time::Instant;

    use process_mining::{import_xes_file, XESImportOptions};

    use crate::convert_log_to_df;

    #[test]
    fn basic_xes() {
        let now = Instant::now();
        let log = import_xes_file(
            "../../../../dow/event_data/BPI_Challenge_2017.xes",
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
        assert_eq!(converted_log.shape().0, num_events);
    }

    #[test]
    fn pm4py_test_logs() {
        let paths = std::fs::read_dir("../../pm4py-core/tests/input_data/").unwrap();
        for path_el in paths {
            let path = path_el.unwrap().path();
            let path_str = path.to_str().unwrap();
            if path_str.ends_with(".xes") || path_str.ends_with(".xes.gz") {
                println!("Path: {}", path_str);
                let _log = import_xes_file(path_str, XESImportOptions::default());
                println!("Success!");
                // println!("{:#?}",_log);
            }
        }
    }
}
