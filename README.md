# rustxes
A Python package to efficiently import XES event logs using Rust.

It currently parsing traces and events including basic attribute values, but not nested attributes (`Container`/`List`).
It supports parsing data both from `.xes` XML files and `.xes.gz` archives (and also from strings directly if needed). 

## Development
This package was scaffolded using [maturin](https://www.maturin.rs/).
The most important commands are:
- `maturin build --release` Builds the Rust code and python package (in release mode), producing the build artificats (wheels)
- `maturin develop --release` Builds the Rust code and python package (in release mode) and automatically installs/updates it in the corresponding (virtual) python env

  Building this package requires [Rust](https://www.rust-lang.org/), which can be installed using Rustup (see [https://www.rust-lang.org/learn/get-started](https://www.rust-lang.org/learn/get-started)).
  The Rust part on its own can be build using `cargo build --release`.
