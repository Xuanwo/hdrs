use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
#[cfg(any(feature = "futures-io", feature = "tokio-io"))]
use std::pin::Pin;
use std::ptr;
#[cfg(any(feature = "futures-io", feature = "tokio-io"))]
use std::task::{Context, Poll};

use hdfs_sys::*;
use libc::c_void;
use log::debug;

/// File will hold the underlying pointer to `hdfsFile`.
///
/// The internal file will be closed while `Drop`, so their is no need to close it manually.
///
/// # Examples
///
/// ```no_run
/// use hdrs::Client;
///
/// let fs = Client::connect("default").expect("client connect succeed");
/// let mut f = fs
///     .open("/tmp/hello.txt", libc::O_RDONLY)
///     .expect("must open success");
/// ```
#[derive(Debug)]
pub struct File {
    fs: hdfsFS,
    f: hdfsFile,
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
    pub(crate) fn new(fs: hdfsFS, f: hdfsFile) -> Self {
        File { fs, f }
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
                buf.len() as i32,
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
                let _ = self.inner_seek(n as i64)?;
                Ok(n)
            }
            SeekFrom::End(_) => Err(Error::new(ErrorKind::Other, "not supported seek operation")),
            SeekFrom::Current(n) => {
                let current = self.tell()?;
                let offset = (current + n) as u64;
                let _ = self.inner_seek(offset as i64)?;
                Ok(offset)
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
                buf.len() as i32,
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

#[cfg(feature = "futures-io")]
impl futures_io::AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        match self.read(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }
}

#[cfg(feature = "futures-io")]
impl futures_io::AsyncSeek for File {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        Poll::Ready(self.seek(pos))
    }
}

#[cfg(feature = "futures-io")]
impl futures_io::AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        match self.write(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(self.flush())
    }

    fn poll_close(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(self.flush())
    }
}

#[cfg(feature = "tokio-io")]
impl tokio::io::AsyncRead for File {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        match self.read(buf.initialize_unfilled()) {
            Ok(n) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }
}

#[cfg(feature = "tokio-io")]
impl tokio::io::AsyncSeek for File {
    fn start_seek(self: Pin<&mut Self>, pos: SeekFrom) -> Result<()> {
        match pos {
            SeekFrom::Start(n) => {
                let _ = self.inner_seek(n as i64)?;
                Ok(())
            }
            SeekFrom::End(_) => Err(Error::new(ErrorKind::Other, "not supported seek operation")),
            SeekFrom::Current(n) => {
                let current = self.tell()?;
                let offset = (current + n) as u64;
                let _ = self.inner_seek(offset as i64)?;
                Ok(())
            }
        }
    }

    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u64>> {
        Poll::Ready(Ok(self.tell()? as u64))
    }
}

#[cfg(feature = "tokio-io")]
impl tokio::io::AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        match self.write(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(err))
                }
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(self.flush())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(self.flush())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::client::Client;

    #[test]
    fn test_file_build() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let f = fs
            .open(&format!("/tmp/{path}"), libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        assert!(!f.f.is_null());
        assert!(!f.fs.is_null());
    }

    #[test]
    fn test_file_write() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open(&format!("/tmp/{path}"), libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }

    #[test]
    fn test_file_read() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open(&format!("/tmp/{path}"), libc::O_CREAT | libc::O_WRONLY)
            .expect("open file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }
}
