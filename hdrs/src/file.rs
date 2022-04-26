use crate::client::Client;
use anyhow::anyhow;
use anyhow::Result;
use hdfs_sys::*;
use log::debug;
use std::io;

#[derive(Debug)]
pub struct StreamBuilder<'f> {
    fs: &'f Client,
    b: *mut hdfsStreamBuilder,
}

impl<'f> Drop for StreamBuilder<'f> {
    fn drop(&mut self) {
        if self.b.is_null() {
            return;
        }

        unsafe {
            let _ = hdfsStreamBuilderFree(self.b);
        }
    }
}

impl<'f> StreamBuilder<'f> {
    pub fn new(fs: &'f Client, b: *mut hdfsStreamBuilder) -> Self {
        StreamBuilder { fs, b }
    }

    pub fn set_buffer_size(&mut self, buffer_size: i32) -> Result<&mut Self> {
        assert!(!self.b.is_null());

        let errno = unsafe { hdfsStreamBuilderSetBufferSize(self.b, buffer_size) };
        match errno {
            0 => Ok(self),
            v => Err(anyhow!(
                "hdfs set_buffer_size: {}",
                io::Error::from_raw_os_error(v)
            )),
        }
    }

    pub fn build(&mut self) -> Result<File> {
        assert!(!self.b.is_null());

        let file = unsafe { hdfsStreamBuilderBuild(self.b) };

        if file.is_null() {
            return Err(anyhow!(
                "hdfs stream builder alloc: {}",
                io::Error::last_os_error()
            ));
        }

        Ok(File::new(self.fs, file))
    }
}

#[derive(Debug)]
pub struct File<'f> {
    fs: &'f Client,
    f: hdfsFile,
}

impl<'f> Drop for File<'f> {
    fn drop(&mut self) {
        unsafe {
            debug!("File dropped");
            let _ = hdfsCloseFile(self.fs.inner(), self.f);
        }
    }
}

impl<'f> File<'f> {
    pub fn new(fs: &'f Client, f: hdfsFile) -> Self {
        File { fs, f }
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
            .open("/tmp/hello.txt", libc::O_RDONLY)
            .expect("open file success");

        let f = f.build().expect("build file success");
        debug!("{:?}", f)
    }
}
