use std::future::Future;
use std::io::{Error, ErrorKind, Result, SeekFrom, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use blocking::Unblock;
use futures::{ready, AsyncSeek};

use crate::File;

/// Async version of file.
///
/// Closing waits for buffered writes and reports errors from the native HDFS close.
/// Once closing starts, further I/O is rejected and subsequent closes return the same result.
/// Call `close().await` after writing: dropping alone may block the calling thread
/// and cannot report close errors.
///
/// Most code are inspired by [async-fs](https://github.com/smol-rs/async-fs).
pub struct AsyncFile {
    /// Performs blocking I/O operations on a thread pool.
    unblock: Option<Unblock<File>>,

    /// Retain the close operation if its caller stops polling it.
    closing: Option<CloseFuture>,
    close_result: Option<std::result::Result<(), Arc<Error>>>,

    /// Logical file cursor, tracked when reading from the file.
    ///
    /// This will be set to an error if the file is not seekable.
    read_pos: Option<Result<u64>>,

    /// Set to `true` if the file needs flushing.
    is_dirty: bool,
}

type CloseFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + Sync>>;

async fn close_io<T, F>(mut io: Unblock<T>, close: F) -> Result<()>
where
    T: Write + Send + Sync + 'static,
    F: FnOnce(T) -> Result<()> + Send + Sync + 'static,
{
    // into_inner ignores pending I/O errors, so collect them before extracting the handle.
    let flush_result = futures::AsyncWriteExt::flush(&mut io).await;
    let inner = io.into_inner().await;
    let close_result = blocking::unblock(move || close(inner)).await;
    flush_result.and(close_result)
}

fn clone_close_result(result: &std::result::Result<(), Arc<Error>>) -> Result<()> {
    result.as_ref().map(|_| ()).map_err(|err| {
        if let Some(code) = err.raw_os_error() {
            Error::from_raw_os_error(code)
        } else {
            Error::new(err.kind(), Arc::clone(err))
        }
    })
}

impl AsyncFile {
    /// Creates an async file from a blocking file.
    pub(crate) fn new(inner: File, is_dirty: bool) -> AsyncFile {
        let unblock = Some(Unblock::new(inner));
        let read_pos = None;
        AsyncFile {
            unblock,
            closing: None,
            close_result: None,
            read_pos,
            is_dirty,
        }
    }

    fn io(&mut self) -> Result<&mut Unblock<File>> {
        self.unblock
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "file is closing or closed"))
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
            ready!(Pin::new(self.io()?).poll_seek(cx, SeekFrom::Start(read_pos)))?;
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

        let n = ready!(Pin::new(self.io()?).poll_read(cx, buf))?;

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
        let io = self.io()?;
        // Keep the async seek contract previously provided by &File.
        if matches!(pos, SeekFrom::End(_)) {
            return Poll::Ready(Err(Error::new(
                ErrorKind::Unsupported,
                "hdfs doesn't support seek from end",
            )));
        }
        Pin::new(io).poll_seek(cx, pos)
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
        Pin::new(self.io()?).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.close_result.is_some() || self.closing.is_some() {
            return Poll::Ready(Err(Error::new(
                ErrorKind::BrokenPipe,
                "file is closing or closed",
            )));
        }
        if self.is_dirty {
            ready!(Pin::new(self.io()?).poll_flush(cx))?;
            self.is_dirty = false;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(result) = &self.close_result {
            return Poll::Ready(clone_close_result(result));
        }

        if self.closing.is_none() {
            let io = self
                .unblock
                .take()
                .expect("open file must have an I/O handle");
            self.closing = Some(Box::pin(close_io(io, File::try_close)));
        }

        let result = ready!(self
            .closing
            .as_mut()
            .expect("close operation must be initialized")
            .as_mut()
            .poll(cx));
        let result = result.map_err(Arc::new);
        let ret = clone_close_result(&result);
        self.close_result = Some(result);
        self.closing = None;
        self.is_dirty = false;
        self.read_pos = None;
        Poll::Ready(ret)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    use futures::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use super::*;
    use crate::client::ClientBuilder;

    struct TestWriter {
        events: Arc<StdMutex<Vec<&'static str>>>,
        written: Arc<StdMutex<Vec<u8>>>,
        fail_write: bool,
        fail_flush: bool,
    }

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> Result<usize> {
            // piper::Reader::drain_inner ends a drain by calling write(&[]).
            // That is not application payload; only record non-empty writes.
            if buf.is_empty() {
                return Ok(0);
            }
            self.events.lock().unwrap().push("write");
            if self.fail_write {
                Err(Error::from_raw_os_error(libc::EACCES))
            } else {
                self.written.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
        }

        fn flush(&mut self) -> Result<()> {
            self.events.lock().unwrap().push("flush");
            if self.fail_flush {
                Err(Error::from_raw_os_error(libc::EIO))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn test_close_drains_writes_before_native_close() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let written = Arc::new(StdMutex::new(Vec::new()));
        let mut io = Unblock::new(TestWriter {
            events: Arc::clone(&events),
            written: Arc::clone(&written),
            fail_write: false,
            fail_flush: false,
        });
        io.write_all(b"payload").await.unwrap();
        let caller_thread = std::thread::current().id();

        close_io(io, move |writer| {
            assert_ne!(std::thread::current().id(), caller_thread);
            writer.events.lock().unwrap().push("close");
            Ok(())
        })
        .await
        .unwrap();

        // Bytes must be drained exactly once before flush/close; no duplication from close_io.
        assert_eq!(*written.lock().unwrap(), b"payload");
        assert_eq!(*events.lock().unwrap(), ["write", "flush", "close"]);
    }

    #[tokio::test]
    async fn test_close_preserves_write_errors_and_releases_handle() {
        for (fail_write, fail_flush, fail_close, expected) in [
            (false, false, true, libc::EBADF),
            (false, true, false, libc::EIO),
            (false, true, true, libc::EIO),
            (true, false, true, libc::EACCES),
        ] {
            let events = Arc::new(StdMutex::new(Vec::new()));
            let written = Arc::new(StdMutex::new(Vec::new()));
            let mut io = Unblock::new(TestWriter {
                events: Arc::clone(&events),
                written: Arc::clone(&written),
                fail_write,
                fail_flush,
            });
            io.write_all(b"payload").await.unwrap();

            let err = close_io(io, move |writer| {
                writer.events.lock().unwrap().push("close");
                if fail_close {
                    Err(Error::from_raw_os_error(libc::EBADF))
                } else {
                    Ok(())
                }
            })
            .await
            .unwrap_err();

            assert_eq!(err.raw_os_error(), Some(expected));
            if fail_write {
                assert!(written.lock().unwrap().is_empty());
            } else {
                assert_eq!(*written.lock().unwrap(), b"payload");
            }
            assert_eq!(*events.lock().unwrap(), ["write", "flush", "close"]);
        }
    }

    fn closing_file(close: CloseFuture) -> AsyncFile {
        AsyncFile {
            unblock: None,
            closing: Some(close),
            close_result: None,
            read_pos: None,
            is_dirty: false,
        }
    }

    #[test]
    fn test_async_file_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AsyncFile>();
    }

    #[tokio::test]
    async fn test_cancelled_close_resumes_and_caches_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let close_attempts = Arc::clone(&attempts);
        let (sender, receiver) = futures::channel::oneshot::channel::<()>();
        let mut file = closing_file(Box::pin(async move {
            close_attempts.fetch_add(1, Ordering::SeqCst);
            receiver.await.unwrap();
            Err(Error::from_raw_os_error(libc::EIO))
        }));

        let mut close = Box::pin(file.close());
        assert!(futures::poll!(&mut close).is_pending());
        drop(close);
        assert_eq!(
            file.write(b"x").await.unwrap_err().kind(),
            ErrorKind::BrokenPipe
        );
        sender.send(()).unwrap();

        for _ in 0..2 {
            assert_eq!(
                file.close().await.unwrap_err().raw_os_error(),
                Some(libc::EIO)
            );
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_closed_file_rejects_io() {
        let mut file = closing_file(Box::pin(async { Ok(()) }));
        file.close().await.unwrap();
        file.close().await.unwrap();

        assert_eq!(
            file.write(b"x").await.unwrap_err().kind(),
            ErrorKind::BrokenPipe
        );
        assert_eq!(
            file.read(&mut [0]).await.unwrap_err().kind(),
            ErrorKind::BrokenPipe
        );
        assert_eq!(
            file.seek(SeekFrom::Start(0)).await.unwrap_err().kind(),
            ErrorKind::BrokenPipe
        );
        assert_eq!(
            file.flush().await.unwrap_err().kind(),
            ErrorKind::BrokenPipe
        );
    }

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

        assert_eq!(
            f.seek(SeekFrom::End(0)).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        let _ = f.seek(SeekFrom::Start(0)).await.expect("seek must success");
        let mut s = String::new();
        let n = f.read_to_string(&mut s).await.expect("read must succeed");
        assert_eq!(n, 13);
        assert_eq!(s, "Hello, World!");
    }
}
