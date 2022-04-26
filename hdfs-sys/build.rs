extern crate bindgen;

use anyhow::Result;
use std::env;
use std::path::PathBuf;

fn main() -> Result<()> {
    // Make sure jvm has been linked.
    let java_home = env::var("JAVA_HOME")?;
    println!("cargo:rustc-link-search=native={java_home}/lib/server");
    println!("cargo:rustc-link-lib=jvm");

    let hadoop_home = env::var("HADOOP_HOME")?;
    println!("cargo:rustc-link-search=native={hadoop_home}/lib/native");
    println!("cargo:rustc-link-lib=hdfs");

    let bindings = bindgen::Builder::default()
        .header(format!("{hadoop_home}/include/hdfs.h"))
        .generate_comments(false)
        .generate()
        .expect("bind generated");

    let out_path = PathBuf::from(env::var("OUT_DIR")?);
    bindings.write_to_file(out_path.join("bindings.rs"))?;

    Ok(())
}
