use hdfs_sys::*;
use libc::c_void;
use std::{io, ptr};

#[derive(Debug)]
pub struct StreamBuilder {
    fs: hdfsFS,
    b: *mut hdfsStreamBuilder,
}

impl Drop for StreamBuilder {
    fn drop(&mut self) {
        if self.b.is_null() {
            return;
        }

        unsafe {
            let _ = hdfsStreamBuilderFree(self.b);
        }
    }
}

impl StreamBuilder {
    pub fn new(fs: hdfsFS, b: *mut hdfsStreamBuilder) -> Self {
        StreamBuilder { fs, b }
    }

    pub fn set_buffer_size(&mut self, buffer_size: i32) -> io::Result<&mut Self> {
        assert!(!self.b.is_null());

        let errno = unsafe { hdfsStreamBuilderSetBufferSize(self.b, buffer_size) };

        if errno == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(self)
    }

    pub fn build(&mut self) -> io::Result<File> {
        assert!(!self.b.is_null());

        let file = unsafe { hdfsStreamBuilderBuild(self.b) };
        // hdfsStreamBuilderBuild will free self.b no matter success or failed.
        self.b = ptr::null_mut();

        if file.is_null() {
            return Err(io::Error::last_os_error());
        }

        Ok(File::new(self.fs, file))
    }
}

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
    use log::debug;

    #[test]
    fn test_file_build() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");

        let mut f = fs
            .open("/tmp/hello.txt", libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        let f = f.build().expect("build file success");
        debug!("{:?}", f)
    }

    #[test]
    fn test_file_write() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");

        let mut f = fs
            .open("/tmp/hello.txt", libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        let f = f.build().expect("build file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }
}
