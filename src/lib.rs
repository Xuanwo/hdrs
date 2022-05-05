//! hdrs is a HDFS Native Client in Rust based on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys).
//!
//! # Examples
//!
//! ```no_run
//! use std::io::{Read, Write};
//!
//! use hdrs::Client;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let fs = Client::connect("default")?;
//!
//! let mut f = fs.open_file().write(true).create(true).open("/tmp/hello.txt")?;
//! let n = f.write("Hello, World!".as_bytes())?;
//!
//! let mut f = fs.open_file().read(true).open("/tmp/hello.txt")?;
//! let mut buf = vec![0; 1024];
//! let n = f.read(&mut buf)?;
//!
//! let _ = fs.remove_file("/tmp/hello.txt")?;
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - `futures-io`: Enable [`futures`](https://docs.rs/futures/latest/futures/io/index.html) support for [`File`]
//! - `tokio-io`: Enable [`tokio::io`](https://docs.rs/tokio/latest/tokio/io/index.html) support for [`File`]

mod client;
pub use client::Client;

mod file;
pub use file::File;

mod metadata;
pub use metadata::Metadata;

mod readdir;
pub use readdir::Readdir;
