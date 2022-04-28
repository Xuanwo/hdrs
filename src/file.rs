use hdfs_sys::*;
use libc::c_void;

use std::{io, ptr};

#[derive(Debug)]
pub struct File {
    fs: hdfsFS,
    f: hdfsFile,
}

impl Drop for File {
    fn drop(&mut self) {
        unsafe {
            let _ = hdfsCloseFile(self.fs, self.f);
            // hdfsCloseFile will free self.f no matter success or failed.
            self.f = ptr::null_mut();
        }
    }
}

impl File {
    pub fn new(fs: hdfsFS, f: hdfsFile) -> Self {
        File { fs, f }
    }

    // Works only for files opened in read-only mode.
    pub fn seek(&self, offset: i64) -> io::Result<()> {
        let n = unsafe { hdfsSeek(self.fs, self.f, offset) };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
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

        let path = uuid::Uuid::default().to_string();

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

        let path = uuid::Uuid::default().to_string();

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

        let path = uuid::Uuid::default().to_string();

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
