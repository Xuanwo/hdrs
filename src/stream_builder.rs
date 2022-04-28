use std::{io, ptr};

use hdfs_sys::*;

use crate::File;

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
