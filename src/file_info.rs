use hdfs_sys::*;
use std::ffi::CStr;
use std::io;

#[derive(Debug, Clone)]
pub struct FileInfo {
    /// the name of the file, like `file:/path/to/file`
    name: String,
    /// the size of the file in bytes
    size: i64,
    /// file or directory
    kind: u32,
    /// the permissions associated with the file
    permissions: i16,
    /// the count of replicas
    replication: i16,
    /// the block size for the file
    block_size: i64,
    /// the owner of the file
    owner: String,
    /// the group associated with the file
    group: String,
    /// the last modification time for the file in seconds
    last_mod: i64,
    /// the last access time for the file in seconds
    last_access: i64,
}

impl FileInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> i64 {
        self.size
    }

    pub fn is_dir(&self) -> bool {
        self.kind == tObjectKind_kObjectKindDirectory
    }

    pub fn is_file(&self) -> bool {
        self.kind == tObjectKind_kObjectKindFile
    }

    // FIXME: how to use it?
    pub fn permissions(&self) -> i16 {
        self.permissions
    }

    pub fn replication(&self) -> i16 {
        self.replication
    }

    pub fn block_size(&self) -> i64 {
        self.block_size
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn last_mod(&self) -> i64 {
        self.last_mod
    }

    pub fn last_access(&self) -> i64 {
        self.last_access
    }
}

impl TryFrom<hdfsFileInfo> for FileInfo {
    type Error = io::Error;

    fn try_from(hfi: hdfsFileInfo) -> io::Result<Self> {
        let fi = Self {
            name: unsafe {
                CStr::from_ptr(hfi.mName)
                    .to_str()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .to_string()
            },
            size: hfi.mSize,
            kind: hfi.mKind,
            permissions: hfi.mPermissions,
            replication: hfi.mReplication,
            block_size: hfi.mBlockSize,
            owner: unsafe {
                CStr::from_ptr(hfi.mOwner)
                    .to_str()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .to_string()
            },
            group: unsafe {
                CStr::from_ptr(hfi.mGroup)
                    .to_str()
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .to_string()
            },
            last_mod: hfi.mLastMod,
            last_access: hfi.mLastAccess,
        };

        Ok(fi)
    }
}
