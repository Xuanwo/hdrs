//! hdrs is a HDFS Native Client in Rust based on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys).
//!
//! # Examples
//!
//! ```no_run
//! use hdrs::Client;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let fs = Client::connect("default")?;
//!
//! let f = fs
//!     .open("/tmp/hello.txt", libc::O_WRONLY | libc::O_CREAT)?
//!     .build()?;
//! let n = f.write("Hello, World!".as_bytes())?;
//!
//! let f = fs.open("/tmp/hello.txt", libc::O_RDONLY)?.build()?;
//! let mut buf = vec![0; 1024];
//! let n = f.read(&mut buf)?;
//!
//! let _ = fs.delete("/tmp/hello.txt", false)?;
//! # Ok(())
//! # }
//! ```

mod client;
pub use client::Client;

mod file;
pub use file::File;

mod file_info;
mod stream_builder;
