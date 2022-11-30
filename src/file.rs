use std::ffi::CString;
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
#[cfg(any(feature = "futures-io", feature = "tokio-io"))]
use std::pin::Pin;
use std::ptr;
#[cfg(any(feature = "futures-io", feature = "tokio-io"))]
use std::task::{Context, Poll};

use hdfs_sys::*;
use libc::c_void;
use log::debug;

/// Options and flags which can be used to configure how a file is opened.
///
/// This builder exposes the ability to configure how a [`File`] is opened and
/// what operations are permitted on the open file.
///
/// # Examples
///
/// Opening a file to read:
///
/// ```no_run
/// use hdrs::Client;
///
/// let fs = Client::connect("default").expect("client connect succeed");
/// let file = fs.open_file().read(true).open("foo.txt");
/// ```
///
/// Opening a file for both reading and writing, as well as creating it if it
/// doesn't exist:
///
/// ```no_run
/// use hdrs::Client;
///
/// let fs = Client::connect("default").expect("client connect succeed");
/// let file = fs.open_file()
///             .read(true)
///             .write(true)
///             .create(true)
///             .open("foo.txt");
/// ```
#[derive(Debug)]
pub struct OpenOptions {
    fs: hdfsFS,

    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
}

impl OpenOptions {
    pub(crate) fn new(fs: hdfsFS) -> Self {
        OpenOptions {
            fs,

            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
        }
    }

    /// Sets the option for read access.
    ///
    /// This option, when true, will indicate that the file should be
    /// `read`-able if opened.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let file = fs.open_file().read(true).open("foo.txt");
    /// ```
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    /// Sets the option for write access.
    ///
    /// This option, when true, will indicate that the file should be
    /// `write`-able if opened.
    ///
    /// If the file already exists, any write calls on it will overwrite its
    /// contents, without truncating it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let file = fs.open_file().write(true).open("foo.txt");
    /// ```
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Sets the option for the append mode.
    ///
    /// This option, when true, means that writes will append to a file instead
    /// of overwriting previous contents.
    /// Note that setting `.write(true).append(true)` has the same effect as
    /// setting only `.append(true)`.
    ///
    /// One maybe obvious note when using append-mode: make sure that all data
    /// that belongs together is written to the file in one operation. This
    /// can be done by concatenating strings before passing them to [`write()`],
    /// or using a buffered writer (with a buffer of adequate size),
    /// and calling [`flush()`] when the message is complete.
    ///
    /// If a file is opened with both read and append access, beware that after
    /// opening, and after every write, the position for reading may be set at the
    /// end of the file. So, before writing, save the current position (using
    /// <code>[seek]\([SeekFrom]::[Current]\(0))</code>), and restore it before the next read.
    ///
    /// ## Note
    ///
    /// This function doesn't create the file if it doesn't exist. Use the
    /// [`OpenOptions::create`] method to do so.
    ///
    /// [`write()`]: Write::write "io::Write::write"
    /// [`flush()`]: Write::flush "io::Write::flush"
    /// [seek]: Seek::seek "io::Seek::seek"
    /// [Current]: SeekFrom::Current "io::SeekFrom::Current"
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let file = fs.open_file().append(true).open("foo.txt");
    /// ```
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    /// Sets the option for truncating a previous file.
    ///
    /// If a file is successfully opened with this option set it will truncate
    /// the file to 0 length if it already exists.
    ///
    /// The file must be opened with write access for truncate to work.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let file = fs.open_file().truncate(true).open("foo.txt");
    /// ```
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    /// Sets the option to create a new file, or open it if it already exists.
    ///
    /// In order for the file to be created, [`OpenOptions::write`] or
    /// [`OpenOptions::append`] access must be used.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let file = fs.open_file().create(true).open("foo.txt");
    /// ```
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Sets the option to create a new file, failing if it already exists.
    ///
    /// No file is allowed to exist at the target location, also no (dangling) symlink. In this
    /// way, if the call succeeds, the file returned is guaranteed to be new.
    ///
    /// This option is useful because it is atomic. Otherwise between checking
    /// whether a file exists and creating a new one, the file may have been
    /// created by another process (a TOCTOU race condition / attack).
    ///
    /// If `.create_new(true)` is set, [`.create()`] and [`.truncate()`] are
    /// ignored.
    ///
    /// The file must be opened with write or append access in order to create
    /// a new file.
    ///
    /// [`.create()`]: OpenOptions::create
    /// [`.truncate()`]: OpenOptions::truncate
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let file = fs.open_file().write(true)
    ///                              .create_new(true)
    ///                              .open("foo.txt");
    /// ```
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create = create_new;
        self
    }

    /// Borrowed from rust-lang
    fn get_access_mode(&self) -> Result<libc::c_int> {
        match (self.read, self.write, self.append) {
            (true, false, false) => Ok(libc::O_RDONLY),
            (false, true, false) => Ok(libc::O_WRONLY),
            (true, true, false) => Ok(libc::O_RDWR),
            (false, _, true) => Ok(libc::O_WRONLY | libc::O_APPEND),
            (true, _, true) => Ok(libc::O_RDWR | libc::O_APPEND),
            (false, false, false) => Err(Error::from_raw_os_error(libc::EINVAL)),
        }
    }

    /// Borrowed from rust-lang
    fn get_creation_mode(&self) -> Result<libc::c_int> {
        match (self.write, self.append) {
            (true, false) => {}
            (false, false) => {
                if self.truncate || self.create || self.create_new {
                    return Err(Error::from_raw_os_error(libc::EINVAL));
                }
            }
            (_, true) => {
                if self.truncate && !self.create_new {
                    return Err(Error::from_raw_os_error(libc::EINVAL));
                }
            }
        }

        Ok(match (self.create, self.truncate, self.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        })
    }

    /// Opens a file at `path` with the options specified by `self`.
    ///
    /// # Errors
    ///
    /// This function will return an error under a number of different
    /// circumstances. Some of these error conditions are listed here, together
    /// with their [`io::ErrorKind`]. The mapping to [`io::ErrorKind`]s is not
    /// part of the compatibility contract of the function.
    ///
    /// * [`NotFound`]: The specified file does not exist and neither `create`
    ///   or `create_new` is set.
    /// * [`NotFound`]: One of the directory components of the file path does
    ///   not exist.
    /// * [`PermissionDenied`]: The user lacks permission to get the specified
    ///   access rights for the file.
    /// * [`PermissionDenied`]: The user lacks permission to open one of the
    ///   directory components of the specified path.
    /// * [`AlreadyExists`]: `create_new` was specified and the file already
    ///   exists.
    /// * [`InvalidInput`]: Invalid combinations of open options (truncate
    ///   without write access, no access mode set, etc.).
    ///
    /// The following errors don't match any existing [`io::ErrorKind`] at the moment:
    /// * One of the directory components of the specified file path
    ///   was not, in fact, a directory.
    /// * Filesystem-level errors: full disk, write permission
    ///   requested on a read-only file system, exceeded disk quota, too many
    ///   open files, too long filename, too many symbolic links in the
    ///   specified path (Unix-like systems only), etc.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let file = fs.open_file().write(true).open("foo.txt");
    /// ```
    ///
    /// [`AlreadyExists`]: io::ErrorKind::AlreadyExists
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    /// [`NotFound`]: io::ErrorKind::NotFound
    /// [`PermissionDenied`]: io::ErrorKind::PermissionDenied
    pub fn open(&self, path: &str) -> Result<File> {
        let flags = libc::O_CLOEXEC | self.get_access_mode()? | self.get_creation_mode()?;

        debug!("open file {} with flags {}", path, flags);
        let b = unsafe {
            let p = CString::new(path)?;
            // TODO: we need to support buffer size, replication and block size.
            hdfsOpenFile(self.fs, p.as_ptr(), flags, 0, 0, 0)
        };

        if b.is_null() {
            return Err(Error::last_os_error());
        }

        debug!("file {} with flags {} opened", path, flags);
        Ok(File::new(self.fs, b))
    }
}

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
/// .open_file().read(true).open("/tmp/hello.txt")
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
                // at most 2^30 bytes, ~1GB
                buf.len().min(1073741824) as i32,
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
                // at most 2^30 bytes, ~1GB
                buf.len().min(1073741824) as i32,
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

        let fs = Client::connect("default").expect("init success");

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

        let fs = Client::connect("default").expect("init success");

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
