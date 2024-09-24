use std::{env, io};

use anyhow::Result;
use hdrs::ClientBuilder;
use log::debug;
use rand::{Rng, RngCore};

#[test]
fn test_connect() -> Result<()> {
    dotenv::from_filename(".env").ok();

    let name_node = env::var("HDRS_NAMENODE")?;

    let fs = ClientBuilder::new(&name_node).connect();
    assert!(fs.is_ok());

    Ok(())
}

#[test]
fn test_mkdir() -> Result<()> {
    let _ = env_logger::try_init();
    dotenv::from_filename(".env").ok();

    if env::var("HDRS_TEST").unwrap_or_default() != "on" {
        return Ok(());
    }

    let name_node = env::var("HDRS_NAMENODE")?;
    let work_dir = env::var("HDRS_WORKDIR").unwrap_or_default();

    let fs = ClientBuilder::new(&name_node).connect()?;

    let path = format!("{work_dir}{}", uuid::Uuid::new_v4());

    fs.create_dir(&path).expect("mkdir should succeed");
    fs.remove_dir(&path).expect("rmdir should succeed");

    Ok(())
}

#[test]
fn test_read_dir() -> Result<()> {
    let _ = env_logger::try_init();
    dotenv::from_filename(".env").ok();

    if env::var("HDRS_TEST").unwrap_or_default() != "on" {
        return Ok(());
    }

    let name_node = env::var("HDRS_NAMENODE")?;
    let work_dir = env::var("HDRS_WORKDIR").unwrap_or_default();

    let fs = ClientBuilder::new(&name_node).connect()?;

    let path = format!("{work_dir}{}", uuid::Uuid::new_v4());

    fs.create_dir(&path).expect("mkdir should succeed");
    debug!("read dir {}", path);
    let readdir = fs.read_dir(&path).expect("readdir should succeed");
    debug!("readdir: {:?}", readdir);
    assert_eq!(readdir.len(), 0);

    Ok(())
}

#[test]
fn test_rename() -> Result<()> {
    use std::io::{Read, Write};

    dotenv::from_filename(".env").ok();

    let name_node = env::var("HDRS_NAMENODE")?;
    let work_dir = env::var("HDRS_WORKDIR").unwrap_or_default();

    let fs = ClientBuilder::new(&name_node).connect()?;

    let path = format!("{work_dir}{}", uuid::Uuid::new_v4());
    {
        let mut f = fs.open_file().create(true).write(true).open(&path)?;
        f.write_all(b"test file content")?;
        f.flush()?;
    }
    let new_path = format!("{work_dir}{}", uuid::Uuid::new_v4());
    fs.rename_file(&path, &new_path)?;

    {
        let maybe_metadata = fs.metadata(&path);
        assert!(maybe_metadata.is_err());
        let err = maybe_metadata.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
    {
        let maybe_metadata = fs.metadata(&new_path);
        assert!(maybe_metadata.is_ok());
        let metadata = maybe_metadata.unwrap();
        assert!(metadata.is_file());
    }
    {
        let mut f = fs.open_file().read(true).open(&new_path)?;
        let mut content = String::new();
        f.read_to_string(&mut content)?;
        assert_eq!(content.as_str(), "test file content");
    }

    Ok(())
}

#[test]
fn test_file() -> Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};

    let _ = env_logger::try_init();
    dotenv::from_filename(".env").ok();

    if env::var("HDRS_TEST").unwrap_or_default() != "on" {
        return Ok(());
    }

    let name_node = env::var("HDRS_NAMENODE")?;
    let work_dir = env::var("HDRS_WORKDIR").unwrap_or_default();

    let fs = ClientBuilder::new(&name_node).connect()?;

    let path = format!("{work_dir}{}", uuid::Uuid::new_v4());

    let mut rng = rand::thread_rng();
    let mut content = vec![0; rng.gen_range(1024..4 * 1024 * 1024)];
    rng.fill_bytes(&mut content);

    {
        // Write file
        debug!("test file write");
        let mut f = fs.open_file().create(true).write(true).open(&path)?;
        f.write_all(&content)?;
        // Flush file
        debug!("test file flush");
        f.flush()?;
    }

    {
        // Read file
        debug!("test file read");
        let mut f = fs.open_file().read(true).open(&path)?;
        let mut buf = Vec::new();
        let n = f.read_to_end(&mut buf)?;
        assert_eq!(n, content.len());
        assert_eq!(buf, content);
    }

    {
        // Read not exist file
        debug!("test not exist file read");
        let f = fs
            .open_file()
            .read(true)
            .open(&format!("{work_dir}{}", uuid::Uuid::new_v4()));
        assert!(f.is_err());
        assert_eq!(f.unwrap_err().kind(), io::ErrorKind::NotFound)
    }

    {
        // Stat file.
        debug!("test file stat");
        let fi = fs.metadata(&path)?;
        assert!(fi.is_file());
        assert_eq!(&path, fi.path());
        assert_eq!(fi.len(), content.len() as u64);
    }

    {
        // Seek file.
        debug!("test file seek");
        let mut f = fs.open_file().read(true).open(&path)?;
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
        let result = fs.remove_file(&path);
        assert!(result.is_ok());
    }

    {
        // Stat it again, we should get a NotFound.
        debug!("test file stat again");
        let fi = fs.metadata(&path);
        assert!(fi.is_err());
        assert_eq!(fi.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    Ok(())
}

#[test]
fn test_client_with_user() -> Result<()> {
    let _ = env_logger::try_init();

    dotenv::from_filename(".env").ok();
    if std::env::var("HDRS_INTEGRATED_TEST").unwrap_or_default() != "on" {
        return Ok(());
    }
    let name_node = env::var("HDRS_NAMENODE")?;
    let work_dir = env::var("HDRS_WORKDIR").unwrap_or_default();

    let fs = ClientBuilder::new(&name_node)
        .with_user("test_user")
        .connect()?;
    let test_dir = format!("{}/test_dir", work_dir);
    let _ = fs.create_dir(&test_dir);
    let meta = fs.metadata(&test_dir);
    assert!(meta.is_ok());
    assert_eq!(meta.unwrap().owner(), "test_user");

    Ok(())
}
