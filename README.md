# hdrs &emsp; [![Build Status]][actions] [![Latest Version]][crates.io]

[Build Status]: https://img.shields.io/github/actions/workflow/status/Xuanwo/hdrs/ci.yml?branch=main
[actions]: https://github.com/Xuanwo/hdrs/actions?query=branch%3Amain
[Latest Version]: https://img.shields.io/crates/v/hdrs.svg
[crates.io]: https://crates.io/crates/hdrs

HDFS Native Client in Rust based on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys).

## Quick Start

```rust
use std::io::{Read, Write};

use hdrs::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Client::connect("hdfs://127.0.0.1:9000")?;

    let mut f = fs.open_file().write(true).create(true).open("/tmp/hello.txt")?;
    let n = f.write("Hello, World!".as_bytes())?;

    let mut f = fs.open_file().read(true).open("/tmp/hello.txt")?;
    let mut buf = vec![0; 1024];
    let n = f.read(&mut buf)?;

    let _ = fs.remove_file("/tmp/hello.txt")?;

    Ok(())
}
```

## Compiletime

`hdrs` depends on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys) which links `libjvm` to work.

Please make sure `JAVA_HOME` is set correctly:

```shell
export JAVA_HOME=/path/to/java
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
```

- Enable `vendored` feature to compile `libhdfs` and link in static.
- Specify `HDFS_LIB_DIR` or `HADOOP_HOME` to load from specified path instead of compile.
- Specify `HDFS_STATIC=1` to link `libhdfs` in static.
- And finally, we will fallback to compile `libhdfs` and link in static.

## Runtime

`hdrs` depends on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys) which uses JNI to call functions provided by jars that provided by hadoop releases.

Please also make sure `HADOOP_HOME`, `LD_LIBRARY_PATH`, `CLASSPATH` is set correctly during runtime:

```shell
export HADOOP_HOME=/path/to/hadoop
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)
```

If `libhdfs` is configued to link dynamiclly, please also add `${HADOOP_HOME}/lib/native` in `LD_LIBRARY_PATH` to make sure linker can find `libhdfs.so`:

```shell
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}
```

## Version Requirement

`hdrs` requires at least hadoop 2.3 to work: hadoop 2.2 doesn't handle FileNotFound correctly.

`hdrs` requires at least hadoop 2.6 to work: Older version of hadoop doesn't handle errno correctly. In older versions, hadoop will set errno to `3` if input path is an empty dir.

## Contributing

Check out the [CONTRIBUTING.md](./CONTRIBUTING.md) guide for more details on getting started with contributing to this project.

## Getting help

Submit [issues](https://github.com/Xuanwo/hdrs/issues/new/choose) for bug report or asking questions in [discussion](https://github.com/Xuanwo/hdrs/discussions/new?category=q-a).

## Acknowledgment

This project is highly inspired by [clang-sys](https://github.com/KyleMayes/clang-sys)

#### License

<sup>
Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
</sup>
