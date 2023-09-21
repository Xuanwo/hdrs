use std::ffi::CString;
use std::io;
use std::mem::MaybeUninit;

use errno::{set_errno, Errno};
use hdfs_sys::*;
use log::debug;

use crate::metadata::Metadata;
use crate::{OpenOptions, Readdir};

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
/// ```no_run
/// use hdrs::{Client, ClientBuilder};
///
/// let fs = ClientBuilder::new("default")
///     .with_user("default")
///     .with_kerberos_ticket_cache_path("/tmp/krb5_111")
///     .connect();
/// ```
#[derive(Debug)]
pub struct Client {
    fs: hdfsFS,
}

/// The builder of connecting to hdfs clusters.
///
/// # Examples
///
/// ```no_run
/// use hdrs::{Client, ClientBuilder};
///
/// let fs = ClientBuilder::new("default")
///     .with_user("default")
///     .with_kerberos_ticket_cache_path("/tmp/krb5_111")
///     .connect();
/// ```
pub struct ClientBuilder {
    name_node: String,
    user: Option<String>,
    kerberos_ticket_cache_path: Option<String>,
}

impl ClientBuilder {
    /// Create a ClientBuilder with name node
    ///
    /// Returns an [`hdrs::ClientBuilder`]
    ///
    /// # Notes
    ///
    /// The NameNode to use.
    ///
    /// If the string given is profile name like 'default', the related
    /// NameNode configuration will be used (from the XML configuration
    /// files).
    ///
    /// If the string starts with a protocol type such as `file://` or
    /// `hdfs://`, this protocol type will be used. If not, the `hdfs://`
    /// protocol type will be used. You may specify a NameNode port in the
    /// usual way by passing a string of the format `hdfs://<hostname>:<port>`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let builder = ClientBuilder::new("default");
    /// ```
    pub fn new(name_node: &str) -> ClientBuilder {
        ClientBuilder {
            name_node: name_node.to_string(),
            user: None,
            kerberos_ticket_cache_path: None,
        }
    }

    /// Set the user for existing ClientBuilder
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let client = ClientBuilder::new("default").with_user("default").connect();
    /// ```
    pub fn with_user(mut self, user: &str) -> ClientBuilder {
        self.user = Some(user.to_string());
        self
    }

    /// Set the krb5 ticket cache path for existing ClientBuilder
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let mut client = ClientBuilder::new("default")
    ///     .with_kerberos_ticket_cache_path("/tmp/krb5_1001")
    ///     .connect();
    /// ```
    pub fn with_kerberos_ticket_cache_path(
        mut self,
        kerberos_ticket_cache_path: &str,
    ) -> ClientBuilder {
        self.kerberos_ticket_cache_path = Some(kerberos_ticket_cache_path.to_string());
        self
    }

    /// Connect for existing ClientBuilder to get a hdfs client
    ///
    /// Returns an [`io::Result`] if any error happens.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let mut client = ClientBuilder::new("default").connect();
    /// ```
    pub fn connect(self) -> io::Result<Client> {
        set_errno(Errno(0));

        debug!("connect name node {}", &self.name_node);

        let fs = {
            let builder = unsafe { hdfsNewBuilder() };

            let name_node = CString::new(self.name_node.as_bytes())?;
            let mut user = MaybeUninit::uninit();
            let mut ticket_cache_path = MaybeUninit::uninit();

            unsafe { hdfsBuilderSetNameNode(builder, name_node.as_ptr()) };

            if let Some(v) = self.user {
                user.write(CString::new(v)?);
                unsafe {
                    hdfsBuilderSetUserName(builder, user.assume_init_ref().as_ptr());
                }
            }

            if let Some(v) = self.kerberos_ticket_cache_path {
                ticket_cache_path.write(CString::new(v)?);
                unsafe {
                    hdfsBuilderSetKerbTicketCachePath(
                        builder,
                        ticket_cache_path.assume_init_ref().as_ptr(),
                    );
                }
            }

            unsafe { hdfsBuilderConnect(builder) }
        };

        if fs.is_null() {
            return Err(io::Error::last_os_error());
        }

        debug!("name node {} connected", self.name_node);
        Ok(Client::new(fs))
    }
}

/// HDFS's client handle is thread safe.
unsafe impl Send for Client {}
unsafe impl Sync for Client {}

impl Client {
    pub(crate) fn new(fs: hdfsFS) -> Self {
        Self { fs }
    }

    /// Open will create a stream builder for later IO operations.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let open_options = fs.open_file();
    /// ```
    pub fn open_file(&self) -> OpenOptions {
        OpenOptions::new(self.fs)
    }

    /// Delete a file.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let _ = fs.remove_file("/tmp/hello.txt");
    /// ```
    pub fn remove_file(&self, path: &str) -> io::Result<()> {
        debug!("remove file {}", path);

        let n = unsafe {
            let p = CString::new(path)?;
            hdfsDelete(self.fs, p.as_ptr(), false.into())
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        debug!("delete file {} finished", path);
        Ok(())
    }

    /// Rename a file.
    ///
    /// **ATTENTION**: the destination directory must exist.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let _ = fs.rename_file("/tmp/hello.txt._COPY_", "/tmp/hello.txt");
    /// ```
    pub fn rename_file(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        debug!("rename file {} -> {}", old_path, new_path);

        let n = {
            let old_path = CString::new(old_path)?;
            let new_path = CString::new(new_path)?;
            unsafe { hdfsRename(self.fs, old_path.as_ptr(), new_path.as_ptr()) }
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        debug!("rename file {} -> {} finished", old_path, new_path);
        Ok(())
    }

    /// Delete a dir.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let _ = fs.remove_dir("/tmp/xxx");
    /// ```
    pub fn remove_dir(&self, path: &str) -> io::Result<()> {
        debug!("remove dir {}", path);

        let n = unsafe {
            let p = CString::new(path)?;
            hdfsDelete(self.fs, p.as_ptr(), false.into())
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        debug!("delete dir {} finished", path);
        Ok(())
    }

    /// Delete a dir recursively.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let _ = fs.remove_dir_all("/tmp/xxx/");
    /// ```
    pub fn remove_dir_all(&self, path: &str) -> io::Result<()> {
        debug!("remove dir all {}", path);

        let n = unsafe {
            let p = CString::new(path)?;
            hdfsDelete(self.fs, p.as_ptr(), true.into())
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        debug!("delete dir all {} finished", path);
        Ok(())
    }

    /// Stat a path to get file info.
    ///
    /// # Examples
    ///
    /// ## Stat a path to file info
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let fi = fs.metadata("/tmp/hello.txt");
    /// ```
    ///
    /// ## Stat a non-exist path
    ///
    /// ```no_run
    /// use std::io;
    ///
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let fi = fs.metadata("/tmp/not-exist.txt");
    /// assert!(fi.is_err());
    /// assert_eq!(fi.unwrap_err().kind(), io::ErrorKind::NotFound)
    /// ```
    pub fn metadata(&self, path: &str) -> io::Result<Metadata> {
        set_errno(Errno(0));

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
        unsafe { hdfsFreeFileInfo(hfi, 1) };

        Ok(fi)
    }

    /// readdir will read file entries from a file.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let fis = fs.read_dir("/tmp/hello/");
    /// ```
    pub fn read_dir(&self, path: &str) -> io::Result<Readdir> {
        set_errno(Errno(0));

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
                None => Ok(Vec::new().into()),
                Some(code) if code == 0 => Ok(Vec::new().into()),
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

        Ok(fis.into())
    }

    /// mkdir create dir and all it's parent directories.
    ///
    /// The behavior is similar to `mkdir -p /path/to/dir`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use hdrs::{Client, ClientBuilder};
    ///
    /// let fs = ClientBuilder::new("default")
    ///     .with_user("default")
    ///     .connect()
    ///     .expect("client connect succeed");
    /// let _ = fs.create_dir("/tmp");
    /// ```
    pub fn create_dir(&self, path: &str) -> io::Result<()> {
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

    use crate::client::ClientBuilder;

    #[test]
    fn test_client_connect() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");
        assert!(!fs.fs.is_null())
    }

    #[test]
    fn test_client_open() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let _ = fs.open_file().read(true).open(&format!("/tmp/{path}"));
    }

    #[test]
    fn test_client_stat() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");
        debug!("Client: {:?}", fs);

        let path = uuid::Uuid::new_v4().to_string();

        let f = fs.metadata(&format!("/tmp/{path}"));
        assert!(f.is_err());
        assert_eq!(f.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn test_client_readdir() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");
        debug!("Client: {:?}", fs);

        let f = fs.read_dir("/tmp").expect("open file success");
        debug!("Metadata: {:?}", f);
        assert!(f.len() > 0)
    }

    #[test]
    fn test_client_mkdir() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");
        debug!("Client: {:?}", fs);

        fs.create_dir("/tmp")
            .expect("mkdir on exist dir should succeed");
    }
}
