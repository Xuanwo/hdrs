use std::ffi::{CStr, CString};

use hdfs_sys::*;

/// Metadata of a path.
#[derive(Debug, Clone)]
pub struct Metadata {
    /// the name of the file, like `file:/path/to/file`
    name: CString,
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
    owner: CString,
    /// the group associated with the file
    group: CString,
    /// the last modification time for the file in seconds
    last_mod: i64,
    /// the last access time for the file in seconds
    last_access: i64,
}

impl Metadata {
    /// the name of the file, like `file:/path/to/file`
    pub fn name(&self) -> &CStr {
        &self.name
    }

    /// the size of the file in bytes
    pub fn size(&self) -> i64 {
        self.size
    }

    /// file or directory
    pub fn is_dir(&self) -> bool {
        self.kind == tObjectKind_kObjectKindDirectory
    }

    /// file or directory
    pub fn is_file(&self) -> bool {
        self.kind == tObjectKind_kObjectKindFile
    }

    /// the permissions associated with the file
    pub fn permissions(&self) -> i16 {
        self.permissions
    }

    /// the count of replicas
    pub fn replication(&self) -> i16 {
        self.replication
    }

    /// the block size for the file
    pub fn block_size(&self) -> i64 {
        self.block_size
    }

    /// the owner of the file
    pub fn owner(&self) -> &CStr {
        &self.owner
    }

    /// the group associated with the file
    pub fn group(&self) -> &CStr {
        &self.group
    }

    /// the last modification time for the file in seconds
    pub fn last_mod(&self) -> i64 {
        self.last_mod
    }

    /// the last access time for the file in seconds
    pub fn last_access(&self) -> i64 {
        self.last_access
    }
}

impl From<hdfsFileInfo> for Metadata {
    fn from(hfi: hdfsFileInfo) -> Self {
        Self {
            name: unsafe { CStr::from_ptr(hfi.mName).into() },
            size: hfi.mSize,
            kind: hfi.mKind,
            permissions: hfi.mPermissions,
            replication: hfi.mReplication,
            block_size: hfi.mBlockSize,
            owner: unsafe { CStr::from_ptr(hfi.mOwner).into() },
            group: unsafe { CStr::from_ptr(hfi.mGroup).into() },
            last_mod: hfi.mLastMod,
            last_access: hfi.mLastAccess,
        }
    }
}
