use crate::file_info::FileInfo;
use hdfs_sys::*;
use log::debug;
use std::ffi::CString;
use std::{io, slice};

use crate::stream_builder::StreamBuilder;

#[derive(Debug)]
pub struct Client {
    fs: hdfsFS,
}

/// # Notes
///
/// It's possible that hdfsDisconnect returns an error, but hdfs will make sure
/// the associated resources be freed so it's safe for us to ignore it.
impl Drop for Client {
    fn drop(&mut self) {
        unsafe {
            let _ = hdfsDisconnect(self.fs);
        }
    }
}

impl Client {
    pub fn connect(name_node: &str, port: usize) -> io::Result<Self> {
        debug!("connect name node {}:{}", name_node, port);
        let fs = unsafe {
            let name_node = CString::new(name_node)?;
            hdfsConnect(
                name_node.as_ptr(),
                port.try_into()
                    .map_err(|v| io::Error::new(io::ErrorKind::Other, v))?,
            )
        };

        if fs.is_null() {
            return Err(io::Error::last_os_error());
        }

        debug!("name node {}:{} connected", name_node, port);
        Ok(Client { fs })
    }

    pub fn open(&self, path: &str, flags: i32) -> io::Result<StreamBuilder> {
        debug!("open file {} with flags {}", path, flags);
        let b = unsafe {
            let p = CString::new(path)?;
            hdfsStreamBuilderAlloc(self.fs, p.as_ptr(), flags)
        };

        if b.is_null() {
            return Err(io::Error::last_os_error());
        }

        debug!("file {} with flags {} opened", path, flags);
        Ok(StreamBuilder::new(self.fs, b))
    }

    pub fn delete(&self, path: &str, recursive: bool) -> io::Result<()> {
        debug!("delete path {} with recursive {}", path, recursive);

        let n = unsafe {
            let p = CString::new(path)?;
            hdfsDelete(self.fs, p.as_ptr(), recursive.into())
        };

        if n == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn stat(&self, path: &str) -> io::Result<FileInfo> {
        let hfi = unsafe {
            let p = CString::new(path)?;
            hdfsGetPathInfo(self.fs, p.as_ptr())
        };

        if hfi.is_null() {
            return Err(io::Error::last_os_error());
        }

        // Safety: hfi must be valid
        let fi = unsafe { FileInfo::try_from(*hfi)? };

        // Make sure hfi has been freed.
        //
        // FIXME: do we need to free file info if `FileInfo::try_from` failed?
        unsafe { hdfsFreeFileInfo(hfi, 1) };

        Ok(fi)
    }

    pub fn readdir(&self, path: &str) -> io::Result<Vec<FileInfo>> {
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

        let hf = unsafe { slice::from_raw_parts(hfis, entries as usize) };
        for v in hf {
            fis.push(FileInfo::try_from(*v)?)
        }

        // Make sure hfis has been freed.
        //
        // FIXME: do we need to free file info if `FileInfo::try_from` failed?
        unsafe { hdfsFreeFileInfo(hfis, entries) };

        Ok(fis)
    }
}

#[cfg(test)]
mod tests {
    use crate::client::Client;
    use log::debug;

    #[test]
    fn test_client_connect() {
        let fs = Client::connect("default", 0).expect("init success");
        println!("{:?}", fs);
    }

    #[test]
    fn test_client_open() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");
        debug!("Client: {:?}", fs);

        let f = fs
            .open("/tmp/hello.txt", libc::O_RDONLY)
            .expect("open file success");
        debug!("StreamBuilder: {:?}", f);
    }

    #[test]
    fn test_client_stat() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");
        debug!("Client: {:?}", fs);

        let f = fs.stat("/tmp/hello.txt").expect("open file success");
        debug!("FileInfo: {:?}", f);
    }

    #[test]
    fn test_client_readdir() {
        let _ = env_logger::try_init();

        let fs = Client::connect("default", 0).expect("init success");
        debug!("Client: {:?}", fs);

        let f = fs.readdir("/tmp").expect("open file success");
        assert!(!f.is_empty())
    }
}
