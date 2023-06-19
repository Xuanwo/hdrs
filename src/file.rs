use hdfs_sys::*;
use libc::c_void;
use log::debug;
use std::io::{Error, Read, Result, Seek, SeekFrom, Write};
use std::ptr;

use crate::Client;

// at most 2^30 bytes, ~1GB
const FILE_LIMIT: usize = 1073741824;

/// File will hold the underlying pointer to `hdfsFile`.
///
/// The internal file will be closed while `Drop`, so their is no need to close it manually.
///
/// # Examples
///
/// ```no_run
/// use hdrs::Client;
///
/// let fs = ClientBuilder::new("default").with_user("default").with_kerberos_ticket_cache_path("/tmp/krb5_111").connect().expect("client connect succeed");
/// let mut f = fs
/// .open_file().read(true).open("/tmp/hello.txt")
///     .expect("must open success");
/// ```
#[derive(Debug)]
pub struct File {
    fs: hdfsFS,
    f: hdfsFile,
    path: String,
}

/// HDFS's client handle is thread safe.
unsafe impl Send for File {}
unsafe impl Sync for File {}

impl Drop for File {
    fn drop(&mut self) {
        unsafe {
            debug!("file has been closed");
            let _ = hdfsCloseFile(self.fs, self.f);
            // hdfsCloseFile will free self.f no matter success or failed.
            self.f = ptr::null_mut();
        }
    }
}

impl File {
    pub(crate) fn new(fs: hdfsFS, f: hdfsFile, path: &str) -> Self {
        File {
            fs,
            f,
            path: path.to_string(),
        }
    }

    /// Works only for files opened in read-only mode.
    fn inner_seek(&self, offset: i64) -> Result<()> {
        let n = unsafe { hdfsSeek(self.fs, self.f, offset) };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(())
    }

    fn tell(&self) -> Result<i64> {
        let n = unsafe { hdfsTell(self.fs, self.f) };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n)
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = unsafe {
            hdfsRead(
                self.fs,
                self.f,
                buf.as_ptr() as *mut c_void,
                buf.len().min(FILE_LIMIT) as i32,
            )
        };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n as usize)
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(n) => {
                self.inner_seek(n as i64)?;
                Ok(n)
            }
            SeekFrom::Current(n) => {
                let current = self.tell()?;
                let offset = (current + n) as u64;
                self.inner_seek(offset as i64)?;
                Ok(offset)
            }
            SeekFrom::End(n) => {
                let meta = Client::new(self.fs).metadata(&self.path)?;
                let offset = meta.len() as i64 + n;
                self.inner_seek(offset)?;
                Ok(offset as u64)
            }
        }
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = unsafe {
            hdfsWrite(
                self.fs,
                self.f,
                buf.as_ptr() as *const c_void,
                buf.len().min(FILE_LIMIT) as i32,
            )
        };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n as usize)
    }

    fn flush(&mut self) -> Result<()> {
        let n = unsafe { hdfsFlush(self.fs, self.f) };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(())
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = unsafe {
            hdfsRead(
                self.fs,
                self.f,
                buf.as_ptr() as *mut c_void,
                buf.len().min(FILE_LIMIT) as i32,
            )
        };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n as usize)
    }
}

impl Seek for &File {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(n) => {
                self.inner_seek(n as i64)?;
                Ok(n)
            }
            SeekFrom::Current(n) => {
                let current = self.tell()?;
                let offset = (current + n) as u64;
                self.inner_seek(offset as i64)?;
                Ok(offset)
            }
            SeekFrom::End(n) => {
                let meta = Client::new(self.fs).metadata(&self.path)?;
                let offset = meta.len() as i64 + n;
                self.inner_seek(offset)?;
                Ok(offset as u64)
            }
        }
    }
}

impl Write for &File {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = unsafe {
            hdfsWrite(
                self.fs,
                self.f,
                buf.as_ptr() as *const c_void,
                buf.len().min(FILE_LIMIT) as i32,
            )
        };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n as usize)
    }

    fn flush(&mut self) -> Result<()> {
        let n = unsafe { hdfsFlush(self.fs, self.f) };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Client, ClientBuilder};

    #[test]
    fn test_file_build() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default").connect().expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let f = fs
            .open_file()
            .create(true)
            .write(true)
            .open(&format!("/tmp/{path}"))
            .expect("open file success");

        assert!(!f.f.is_null());
        assert!(!f.fs.is_null());
    }

    #[test]
    fn test_file_write() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default").connect().expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open_file()
            .create(true)
            .write(true)
            .open(&format!("/tmp/{path}"))
            .expect("open file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }

    #[test]
    fn test_file_read() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default").connect().expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open_file()
            .create(true)
            .write(true)
            .open(&format!("/tmp/{path}"))
            .expect("open file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }
}
