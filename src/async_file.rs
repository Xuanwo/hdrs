use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use blocking::Unblock;
use futures::lock::Mutex;
use futures::{ready, AsyncSeek};

use crate::File;

/// A wrapper around `Arc<File>` that implements `Read`, `Write`, and `Seek`.
struct ArcFile(Arc<File>);

impl Read for ArcFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (&*self.0).read(buf)
    }
}

impl Write for ArcFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (&*self.0).write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        (&*self.0).flush()
    }
}

impl Seek for ArcFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        (&*self.0).seek(pos)
    }
}

/// Async version of file.
///
/// Most code are inspired by [async-fs](https://github.com/smol-rs/async-fs).
pub struct AsyncFile {
    /// Always accessible reference to the file.
    ///
    /// Not used for now, just save for future use.
    _file: Arc<File>,

    /// Performs blocking I/O operations on a thread pool.
    unblock: Mutex<Unblock<ArcFile>>,

    /// Logical file cursor, tracked when reading from the file.
    ///
    /// This will be set to an error if the file is not seekable.
    read_pos: Option<Result<u64>>,

    /// Set to `true` if the file needs flushing.
    is_dirty: bool,
}

impl AsyncFile {
    /// Creates an async file from a blocking file.
    pub(crate) fn new(inner: File, is_dirty: bool) -> AsyncFile {
        let file = Arc::new(inner);
        let unblock = Mutex::new(Unblock::new(ArcFile(file.clone())));
        let read_pos = None;
        AsyncFile {
            _file: file,
            unblock,
            read_pos,
            is_dirty,
        }
    }

    /// Repositions the cursor after reading.
    ///
    /// When reading from a file, actual file reads run asynchronously in the background, which
    /// means the real file cursor is usually ahead of the logical cursor, and the data between
    /// them is buffered in memory. This kind of buffering is an important optimization.
    ///
    /// After reading ends, if we decide to perform a write or a seek operation, the real file
    /// cursor must first be repositioned back to the correct logical position.
    fn poll_reposition(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(Ok(read_pos)) = self.read_pos {
            ready!(Pin::new(self.unblock.get_mut()).poll_seek(cx, SeekFrom::Start(read_pos)))?;
        }
        self.read_pos = None;
        Poll::Ready(Ok(()))
    }
}

impl futures::AsyncRead for AsyncFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        // Before reading begins, remember the current cursor position.
        if self.read_pos.is_none() {
            // Initialize the logical cursor to the current position in the file.
            self.read_pos = Some(ready!(self.as_mut().poll_seek(cx, SeekFrom::Current(0))));
        }

        let n = ready!(Pin::new(self.unblock.get_mut()).poll_read(cx, buf))?;

        // Update the logical cursor if the file is seekable.
        if let Some(Ok(pos)) = self.read_pos.as_mut() {
            *pos += n as u64;
        }

        Poll::Ready(Ok(n))
    }
}

impl futures::AsyncSeek for AsyncFile {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        ready!(self.poll_reposition(cx))?;
        Pin::new(self.unblock.get_mut()).poll_seek(cx, pos)
    }
}

impl futures::AsyncWrite for AsyncFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        ready!(self.poll_reposition(cx))?;
        self.is_dirty = true;
        Pin::new(self.unblock.get_mut()).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.is_dirty {
            ready!(Pin::new(self.unblock.get_mut()).poll_flush(cx))?;
            self.is_dirty = false;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(self.unblock.get_mut()).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use super::*;
    use crate::client::ClientBuilder;

    #[tokio::test]
    async fn test_file_build() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let _ = fs
            .open_file()
            .create(true)
            .write(true)
            .async_open(&format!("/tmp/{path}"))
            .await
            .expect("open file success");
    }

    #[tokio::test]
    async fn test_file_write() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open_file()
            .create(true)
            .write(true)
            .async_open(&format!("/tmp/{path}"))
            .await
            .expect("open file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .await
            .expect("write must success");
        assert_eq!(n, 13)
    }

    #[tokio::test]
    async fn test_file_read() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        {
            let mut f = fs
                .open_file()
                .create(true)
                .write(true)
                .async_open(&format!("/tmp/{path}"))
                .await
                .expect("open file success");

            f.write_all("Hello, World!".as_bytes())
                .await
                .expect("write must success");
            f.close().await.expect("close must success");
        }

        let mut f = fs
            .open_file()
            .read(true)
            .async_open(&format!("/tmp/{path}"))
            .await
            .expect("open file success");

        let _ = f.seek(SeekFrom::Start(0)).await.expect("seek must success");
        let mut s = String::new();
        let n = f.read_to_string(&mut s).await.expect("read must succeed");
        assert_eq!(n, 13);
        assert_eq!(s, "Hello, World!");
    }
}
