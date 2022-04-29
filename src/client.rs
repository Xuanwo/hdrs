use std::ffi::CString;
use std::io;

use hdfs_sys::*;
use log::debug;

use crate::metadata::Metadata;
use crate::File;

/// Client holds the underlying connection to hdfs clusters.
///
/// The connection will be disconnected while `Drop`, so their is no need to terminate it manually.
///
/// # Note
///
/// Hadoop will have it's own filesystem logic which may return the same filesystem instance while
/// `hdfsConnect`. If we call `hdfsDisconnect`, all clients that hold this filesystem instance will
/// meet `java.io.IOException: Filesystem closed` during I/O operations.
///
/// So it's better for us to not call `hdfsDisconnect` manually.
/// Aka, don't implement `Drop` to disconnect the connection.
///
/// Reference: [IOException: Filesystem closed exception when running oozie workflo](https://stackoverflow.com/questions/23779186/ioexception-filesystem-closed-exception-when-running-oozie-workflow)
///
/// # Examples
///
/// ```
/// use hdrs::Client;
///
/// let fs = Client::connect("default");
/// ```
#[derive(Debug)]
pub struct Client {
    fs: hdfsFS,
}

/// HDFS's client handle is thread safe.
unsafe impl Send for Client {}
unsafe impl Sync for Client {}

impl Client {
    /// Connect to a name node with port
    ///
    /// Returns an [`io::Result`] if any error happens.
    ///
    /// # Examples
    ///
    /// ```
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default");
    /// ```
    pub fn connect(name_node: &str) -> io::Result<Self> {
        debug!("connect name node {}", name_node);

        let fs = unsafe {
            let name_node = CString::new(name_node)?;
            hdfsConnect(name_node.as_ptr(), 0)
        };

        if fs.is_null() {
            return Err(io::Error::last_os_error());
        }

        debug!("name node {} connected", name_node);
        Ok(Client { fs })
    }

    /// Open will create a stream builder for later IO operations.
    ///
    /// # Examples
    ///
    /// ```
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let builder = fs.open("/tmp/hello.txt", libc::O_RDONLY);
    /// ```
    pub fn open(&self, path: &str, flags: i32) -> io::Result<File> {
        debug!("open file {} with flags {}", path, flags);
        let b = unsafe {
            let p = CString::new(path)?;
            // TODO: we need to support buffer size, replication and block size.
            hdfsOpenFile(self.fs, p.as_ptr(), flags, 0, 0, 0)
        };

        if b.is_null() {
            return Err(io::Error::last_os_error());
        }

        debug!("file {} with flags {} opened", path, flags);
        Ok(File::new(self.fs, b))
    }

    /// Delete a dir or directory.
    ///
    /// - If the path is a file, recursive will have no effect.
    /// - If the path is a dir and recursive is `true`, the dir will be deleted.
    /// - If the path is an empty dir and recursive is `false`, the dir will be deleted.
    /// - If the path is non-empty dir and recursive is `false`, an error will be raised.
    ///
    /// # Todo
    ///
    /// - We need to handle the `Directory /tmp/xxx is not empty` error.
    /// - Do we need to split this function into `remove_file` and `remove_dir`?
    ///
    /// # Examples
    ///
    /// ## Delete a file
    ///
    /// ```
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let _ = fs.delete("/tmp/hello.txt", false);
    /// ```
    ///
    /// ## Delete a directory
    ///
    /// ```
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let _ = fs.delete("/tmp/hello", true);
    /// ```
    pub fn delete(&self, path: &str, recursive: bool) -> io::Result<()> {
        debug!("delete path {} with recursive {}", path, recursive);

        let n = unsafe {
            let p = CString::new(path)?;
            hdfsDelete(self.fs, p.as_ptr(), recursive.into())
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        debug!("delete path {} with recursive {} finished", path, recursive);
        Ok(())
    }

    /// Stat a path to get file info.
    ///
    /// # Examples
    ///
    /// ## Stat a path to file info
    ///
    /// ```
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let fi = fs.stat("/tmp/hello.txt");
    /// ```
    ///
    /// ## Stat a non-exist path
    ///
    /// ```
    /// use std::io;
    ///
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let fi = fs.stat("/tmp/not-exist.txt");
    /// assert!(fi.is_err());
    /// assert_eq!(fi.unwrap_err().kind(), io::ErrorKind::NotFound)
    /// ```
    pub fn stat(&self, path: &str) -> io::Result<Metadata> {
        let hfi = unsafe {
            let p = CString::new(path)?;
            hdfsGetPathInfo(self.fs, p.as_ptr())
        };

        if hfi.is_null() {
            return Err(io::Error::last_os_error());
        }

        // Safety: hfi must be valid
        let fi = unsafe { Metadata::from(*hfi) };

        // Make sure hfi has been freed.
        //
        // FIXME: do we need to free file info if `FileInfo::try_from` failed?
        unsafe { hdfsFreeFileInfo(hfi, 1) };

        Ok(fi)
    }

    /// readdir will read file entries from a file.
    ///
    /// # Examples
    ///
    /// ```
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let fis = fs.readdir("/tmp/hello/");
    /// ```
    pub fn readdir(&self, path: &str) -> io::Result<Vec<Metadata>> {
        let mut entries = 0;
        let hfis = unsafe {
            let p = CString::new(path)?;
            hdfsListDirectory(self.fs, p.as_ptr(), &mut entries)
        };

        // hfis will be NULL on error or empty directory.
        // We will try to check last_os_error's code.
        // - If there is no error, return empty vec directly.
        // - If errno == 0, there is no error, return empty vec directly.
        // - If errno != 0, return the last os error.
        if hfis.is_null() {
            let e = io::Error::last_os_error();

            return match e.raw_os_error() {
                None => Ok(Vec::new()),
                Some(code) if code == 0 => Ok(Vec::new()),
                Some(_) => Err(e),
            };
        }

        let mut fis = Vec::with_capacity(entries as usize);

        for i in 0..entries {
            let m = unsafe { Metadata::from(*hfis.offset(i as isize)) };

            fis.push(m)
        }

        // Make sure hfis has been freed.
        unsafe { hdfsFreeFileInfo(hfis, entries) };

        Ok(fis)
    }

    /// mkdir create dir and all it's parent directories.
    ///
    /// The behavior is similar to `mkdir -p /path/to/dir`.
    ///
    /// # Examples
    ///
    /// ```
    /// use hdrs::Client;
    ///
    /// let fs = Client::connect("default").expect("client connect succeed");
    /// let _ = fs.mkdir("/tmp");
    /// ```
    pub fn mkdir(&self, path: &str) -> io::Result<()> {
        let n = unsafe {
            let p = CString::new(path)?;
            hdfsCreateDirectory(self.fs, p.as_ptr())
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use log::debug;

    use crate::client::Client;

    #[test]
    fn test_client_connect() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");
        assert!(!fs.fs.is_null())
    }

    #[test]
    fn test_client_open() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let _ = fs.open(&format!("/tmp/{path}"), libc::O_RDONLY);
    }

    #[test]
    fn test_client_stat() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");
        debug!("Client: {:?}", fs);

        let path = uuid::Uuid::new_v4().to_string();

        let f = fs.stat(&format!("/tmp/{path}"));
        assert!(f.is_err());
        assert_eq!(f.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn test_client_readdir() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");
        debug!("Client: {:?}", fs);

        let f = fs.readdir("/tmp").expect("open file success");
        debug!("Metadata: {:?}", f);
        assert!(!f.is_empty())
    }

    #[test]
    fn test_client_mkdir() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default").expect("init success");
        debug!("Client: {:?}", fs);

        let _ = fs.mkdir("/tmp").expect("mkdir on exist dir should succeed");
    }
}
