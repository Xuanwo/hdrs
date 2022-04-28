# hdrs &emsp; [![Build Status]][actions] [![Latest Version]][crates.io]

[Build Status]: https://img.shields.io/github/workflow/status/Xuanwo/hdrs/CI/main
[actions]: https://github.com/Xuanwo/hdrs/actions?query=branch%3Amain
[Latest Version]: https://img.shields.io/crates/v/hdrs.svg
[crates.io]: https://crates.io/crates/hdrs

HDFS Native Client in Rust based on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys).

## Quick Start

```rust
use std::io::{Read, Write};

use hdrs::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Client::connect("default")?;
    
    let mut f = fs
        .open("/tmp/hello.txt", libc::O_WRONLY | libc::O_CREAT)?
        .build()?;
    let n = f.write("Hello, World!".as_bytes())?;
    
    let mut f = fs.open("/tmp/hello.txt", libc::O_RDONLY)?.build()?;
    let mut buf = vec![0; 1024];
    let n = f.read(&mut buf)?;
    
    let _ = fs.delete("/tmp/hello.txt", false)?;

    Ok(())
}
```

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
