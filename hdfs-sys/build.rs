extern crate bindgen;

use std::path::PathBuf;
use std::{env, fs};

fn main() -> anyhow::Result<()> {
    let headers: Vec<String> = fs::read_dir("./headers")?
        .map(|v| v.expect("dir entry got"))
        .filter(|v| v.path().to_string_lossy().ends_with(".h"))
        .map(|v| v.file_name().to_string_lossy().to_string())
        .collect();

    for header in headers {
        let bindings = bindgen::Builder::default()
            .parse_callbacks(Box::new(bindgen::CargoCallbacks))
            .header(format!("./headers/{}", &header))
            .generate_comments(false)
            .generate()
            .expect("bind generated");

        let out_path = PathBuf::from(env::var("OUT_DIR")?);
        bindings.write_to_file(out_path.join(format!("bindings_{}.rs", header)))?;
    }

    println!(
        "cargo:rustc-link-search=native={}/lib/native",
        env::var("HADOOP_HOME")?
    );
    println!("cargo:rustc-link-lib=hdfs");

    Ok(())
}
