# rustxes
A Python package to efficiently import XES or OCEL2 event logs using Rust.

For traditional event data, it supports parsing data both from `.xes` XML files and `.xes.gz` archives (and also from strings directly if needed). 

For object-centric event data, it supports parsing OCEL2 XML or JSON files (`.xml` or `.json`).



## Usage
### XES Import

The `import_xes` returns a tuple of 1) the XES log polars dataframe and 2) JSON-encoding of global log attributes.

```python
import rustxes

[xes,log_attrs_json] = rustxes.import_xes("path/to/file.xes")
print(xes.shape)
```

#### Options
The following parameters can be passed to the `import_xes_rs` or the python wrapper (`import_xes`):
- `path` - The filepath of the .xes or .xes.gz file to import
- `date_format` - Optional date format to use for parsing `<date>` tags (See https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
- `print_debug` - Optional flag to enable debug print outputs


### XES Export

The `export_xes` exports the passed polars DataFrame to the given path (either `.xes` or `.xes.gz`).

```python
import rustxes

[xes,log_attrs_json] = rustxes.import_xes("path/to/file.xes")
rustxes.export_xes(xes,"path/to/export-file.xes")
```

#### Options
The following parameters can be passed to the `export_xes_rs` or the python wrapper (`export_xes`):
- `df` - The polars DataFrame representing the event log
- `path` - The filepath the .xes or .xes.gz file should be written to



### OCEL Import
The `import_ocel_xml` and `import_ocel_json` functions return a dict of polars DataFrames with the following keys: 'objects', 'events', 'o2o', 'object_changes', 'relations'.

```python
import rustxes

ocel = rustxes.import_ocel_xml("path/to/ocel.xml")
print(ocel['objects'].shape)
```
If you want to use PM4Py's OCEL data structure, you can use the `import_ocel_xml_pm4py` or `import_ocel_json_pm4py` functions, which return a `pm4py.ocel.OCEL` objects.
Note: PM4Py must be installed for this to work!


## Development
This package was scaffolded using [maturin](https://www.maturin.rs/).
The most important commands are:
- `maturin build --release` Builds the Rust code and python package (in release mode), producing the build artifacts (wheels)
- `maturin develop --release` Builds the Rust code and python package (in release mode) and automatically installs/updates it in the corresponding (virtual) python env

  Building this package requires [Rust](https://www.rust-lang.org/), which can be installed using Rustup (see [https://www.rust-lang.org/learn/get-started](https://www.rust-lang.org/learn/get-started)).
  The Rust part on its own can be build using `cargo build --release`.


## LICENSE
This package is licensed under either Apache License Version 2.0 or MIT License at your option. 