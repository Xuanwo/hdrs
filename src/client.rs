use crate::file::StreamBuilder;
use anyhow::anyhow;
use anyhow::Result;
use hdfs_sys::*;
use log::debug;
use std::ffi::CString;
use std::io;

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
            debug!("Client dropped");
            let _ = hdfsDisconnect(self.fs);
        }
    }
}

impl Client {
    pub fn connect(name_node: &str, port: usize) -> Result<Self> {
        debug!("connect name node {}:{}", name_node, port);
        let fs = unsafe { hdfsConnect(CString::new(name_node)?.as_ptr(), port.try_into()?) };

        if fs.is_null() {
            return Err(anyhow!("hdfs connect failed"));
        }

        debug!("name node {}:{} connected", name_node, port);
        Ok(Client { fs })
    }

    pub(crate) fn inner(&self) -> hdfsFS {
        self.fs
    }

    pub fn open(&self, path: &str, flags: i32) -> Result<StreamBuilder> {
        debug!("open file {} with flags {}", path, flags);
        let b = unsafe {
            let p = CString::new(path)?;
            hdfsStreamBuilderAlloc(self.fs, p.as_ptr(), flags)
        };

        if b.is_null() {
            return Err(anyhow!(
                "hdfs stream builder alloc: {}",
                io::Error::last_os_error()
            ));
        }

        debug!("file {} with flags {} opened", path, flags);
        Ok(StreamBuilder::new(self, b))
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
}
