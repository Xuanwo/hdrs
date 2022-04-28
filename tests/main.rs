use std::io::{Read, Seek, SeekFrom, Write};
use std::{env, io};

use anyhow::Result;
use hdrs::Client;
use log::debug;
use rand::{Rng, RngCore};

#[test]
fn test_connect() -> Result<()> {
    dotenv::from_filename(".env").ok();

    let name_node = env::var("HDRS_HDFS_NAMENODE")?;

    let fs = Client::connect(&name_node);
    assert!(fs.is_ok());

    Ok(())
}

#[test]
fn test_file() -> Result<()> {
    env_logger::try_init()?;
    dotenv::from_filename(".env").ok();

    if env::var("HDRS_HDFS_TEST").unwrap_or_default() != "on" {
        return Ok(());
    }

    let name_node = env::var("HDRS_HDFS_NAMENODE")?;
    let work_dir = env::var("HDRS_HDFS_WORKDIR").unwrap_or_default();

    let fs = Client::connect(&name_node)?;

    let path = format!("{work_dir}{}", uuid::Uuid::new_v4());

    let mut rng = rand::thread_rng();
    let mut content = vec![0; rng.gen_range(1024..4 * 1024 * 1024)];
    rng.fill_bytes(&mut content);

    {
        // Write file
        debug!("test file write");
        let mut f = fs.open(&path, libc::O_CREAT | libc::O_WRONLY)?.build()?;
        let _ = f.write_all(&content)?;
        // Flush file
        debug!("test file flush");
        let _ = f.flush()?;
    }

    {
        // Read file
        debug!("test file read");
        let mut f = fs.open(&path, libc::O_RDONLY)?.build()?;
        let mut buf = Vec::new();
        let n = f.read_to_end(&mut buf)?;
        assert_eq!(n, content.len());
        assert_eq!(buf, content);
    }

    {
        // Stat file.
        debug!("test file stat");
        let fi = fs.stat(&path)?;
        assert!(fi.is_file());
        assert_eq!(fi.size(), content.len() as i64);
    }

    {
        // Seek file.
        debug!("test file seek");
        let mut f = fs.open(&path, libc::O_RDONLY)?.build()?;
        let offset = content.len() / 2;
        let size = content.len() - offset;
        let mut buf = Vec::new();
        let _ = f.seek(SeekFrom::Start(offset as u64))?;
        let n = f.read_to_end(&mut buf)?;
        assert_eq!(n, size);
        assert_eq!(buf, content[offset..]);
    }

    {
        // Remove file
        debug!("test file remove");
        let result = fs.delete(&path, false);
        assert!(result.is_ok());
    }

    {
        // Stat it again, we should get a NotFound.
        debug!("test file stat again");
        let fi = fs.stat(&path);
        assert!(fi.is_err());
        assert_eq!(fi.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    Ok(())
}
