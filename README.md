# rustxes
A Python package to efficiently import XES or OCEL2 event logs using Rust.

It supports parsing data both from `.xes` XML files and `.xes.gz` archives (and also from strings directly if needed). 

Since version `0.2.0`, it also supports parsing OCEL2 XML or JSON files.


## Options
The following parameters can be passed to the `import_xes_rs` or the python wrapper:
- `path` - The filepath of the .xes or .xes.gz file to import
- `date_format` - Optional date format to use for parsing `<date>` tags (See https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
- `print_debug` - Optional flag to enable debug print outputs


## Development
This package was scaffolded using [maturin](https://www.maturin.rs/).
The most important commands are:
- `maturin build --release` Builds the Rust code and python package (in release mode), producing the build artificats (wheels)
- `maturin develop --release` Builds the Rust code and python package (in release mode) and automatically installs/updates it in the corresponding (virtual) python env

  Building this package requires [Rust](https://www.rust-lang.org/), which can be installed using Rustup (see [https://www.rust-lang.org/learn/get-started](https://www.rust-lang.org/learn/get-started)).
  The Rust part on its own can be build using `cargo build --release`.


## LICENSE
This package is licensed under either Apache License Version 2.0 or MIT License at your option. 