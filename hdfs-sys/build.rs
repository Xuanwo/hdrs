extern crate bindgen;

use anyhow::{anyhow, Result};
use regex::Regex;
use std::path::PathBuf;
use std::{env, fs};

fn main() -> Result<()> {
    let hadoop_home =
        env::var("HADOOP_HOME").expect("HADOOP_HOME is required to make hdfs-sys works");

    println!("cargo:rustc-link-search=native={hadoop_home}/lib/native");
    println!("cargo:rustc-link-lib=hdfs");

    let hadoop_version = detect_hadoop_version(&hadoop_home)?;

    let header = fs::read_dir("./headers")?
        .map(|v| {
            v.expect("dir entry got")
                .file_name()
                .into_string()
                .expect("file name must be valid")
        })
        .filter(|v| v.ends_with(&format!("{}.h", hadoop_version.replace(".", "_"))))
        .next()
        .ok_or_else(|| anyhow!("hadoop version for {} is not supported", hadoop_version))?;

    let bindings = bindgen::Builder::default()
        .header(format!("./headers/{}", &header))
        .generate_comments(false)
        .generate()
        .expect("bind generated");

    let out_path = PathBuf::from(env::var("OUT_DIR")?);
    bindings.write_to_file(out_path.join("bindings.rs"))?;

    Ok(())
}

fn detect_hadoop_version(home: &str) -> Result<String> {
    let re = Regex::new(r"hadoop-hdfs-\d+\.\d+\.\d+\.jar").expect("regex must be valid");

    fs::read_dir(format!("{}/share/hadoop/hdfs", home))
        .map_err(|e| {
            anyhow!(
                "read hadoop home failed: {:?}, please check if HADOOP_HOME set correctly",
                e
            )
        })?
        .map(|v| {
            v.expect("dir entry got")
                .file_name()
                .into_string()
                .expect("file name must be valid string")
        })
        .filter(|v| re.is_match(v))
        .map(|v| {
            v.strip_prefix("hadoop-hdfs-")
                .expect("must have prefix hadoop-hdfs-")
                .strip_suffix(".jar")
                .expect("must have suffix .jar")
                .to_string()
        })
        .next()
        .ok_or_else(|| {
            anyhow!("hadoop version not detected, please check if HADOOP_HOME set correctly")
        })
}
