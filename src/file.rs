use std::{io, ptr};

use hdfs_sys::*;
use libc::c_void;
use log::debug;

/// File will hold the underlying pointer to `hdfsFile`.
///
/// The internal file will be closed while `Drop`, so their is no need to close it manually.
///
/// # Examples
///
/// ```
/// use hdrs::Client;
///
/// let fs = Client::connect("default", 0).expect("client connect succeed");
/// let mut builder = fs
///     .open("/tmp/hello.txt", libc::O_RDONLY)
///     .expect("must open success");
/// let f = builder.build();
/// ```
#[derive(Debug)]
pub struct File {
    fs: hdfsFS,
    f: hdfsFile,
}

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
    pub(crate) fn new(fs: hdfsFS, f: hdfsFile) -> Self {
        File { fs, f }
    }

    /// Works only for files opened in read-only mode.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// # let fs = Client::connect("default", 0).expect("client connect succeed");
    /// # let mut builder = fs.open("/tmp/hello.txt", libc::O_RDONLY).expect("must open succeed");
    /// # let f = builder.build().expect("must build succeed");
    /// let _ = f.seek(1024);
    /// ```
    pub fn seek(&self, offset: i64) -> io::Result<()> {
        let n = unsafe { hdfsSeek(self.fs, self.f, offset) };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    /// Read data from file into `buf` and return read size.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// # let fs = Client::connect("default", 0).expect("client connect succeed");
    /// # let mut builder = fs.open("/tmp/hello.txt", libc::O_RDONLY).expect("must open succeed");
    /// # let mut f = builder.build().expect("must build succeed");
    /// let mut buf = vec![0; 1024];
    /// let _ = f.read(&mut buf);
    /// ```
    pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let n = unsafe {
            hdfsRead(
                self.fs,
                self.f,
                buf.as_ptr() as *mut c_void,
                buf.len() as i32,
            )
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(n as usize)
    }

    /// Write data into file from `buf` and return written size.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// # let fs = Client::connect("default", 0).expect("client connect succeed");
    /// # let mut builder = fs.open("/tmp/hello.txt", libc::O_RDONLY).expect("must open succeed");
    /// # let mut f = builder.build().expect("must build succeed");
    /// let buf = "Hello, World!".as_bytes();
    /// let _ = f.write(&buf);
    /// ```
    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        let n = unsafe {
            hdfsWrite(
                self.fs,
                self.f,
                buf.as_ptr() as *const c_void,
                buf.len() as i32,
            )
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(n as usize)
    }

    /// Flush data into file.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// # let fs = Client::connect("default", 0).expect("client connect succeed");
    /// # let mut builder = fs.open("/tmp/hello.txt", libc::O_RDONLY).expect("must open succeed");
    /// # let mut f = builder.build().expect("must build succeed");
    /// let _ = f.flush();
    /// ```
    pub fn flush(&self) -> io::Result<()> {
        let n = unsafe { hdfsFlush(self.fs, self.f) };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::client::Client;

    #[test]
    fn test_file_build() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open(&format!("/tmp/{path}"), libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        let f = f.build().expect("build file success");
        assert!(!f.f.is_null());
        assert!(!f.fs.is_null());
    }

    #[test]
    fn test_file_write() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open(&format!("/tmp/{path}"), libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        let f = f.build().expect("build file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }

    #[test]
    fn test_file_read() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open(&format!("/tmp/{path}"), libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        let f = f.build().expect("build file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }
}
