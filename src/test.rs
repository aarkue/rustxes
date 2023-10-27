#[cfg(test)]
mod xes_tests {
    use std::fs;

    use crate::xes_import::import_xes_file;

    #[test]
    fn basic_xes() {
        let log = import_xes_file("../../../../dow/roadtraffic50traces.xes", None);
        println!("{:#?}",log);
    }

    #[test]
    fn pm4py_test_logs() {
        let paths = fs::read_dir("../../pm4py-core/tests/input_data/").unwrap();
        for path_el in paths {
            let path = path_el.unwrap().path();
            let path_str = path.to_str().unwrap();
            if path_str.ends_with(".xes") || path_str.ends_with(".xes.gz") {
                println!("Path: {}", path_str);
                let _log = import_xes_file(path_str, None);
                println!("Success!");
                // println!("{:#?}",_log);
            }
        }
    }
}
