#[cfg(test)]
mod xes_tests {
    use crate::xes_import::import_xes_file;

    #[test]
    fn basic_xes() {
        let log = import_xes_file("/home/aarkue/dow/test.xes");
        println!("{:#?}",log);}
}
