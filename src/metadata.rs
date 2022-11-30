use std::ffi::CStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hdfs_sys::*;

/// Metadata of a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// the name of the file, like `file:/path/to/file`
    path: String,
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

impl Metadata {
    /// the path of the file, like `/path/to/file`
    ///
    /// # Notes
    ///
    /// Hadoop has [restrictions](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/introduction.html) of path name:
    ///
    /// - A Path is comprised of Path elements separated by "/".
    /// - A path element is a unicode string of 1 or more characters.
    /// - Path element MUST NOT include the characters ":" or "/".
    /// - Path element SHOULD NOT include characters of ASCII/UTF-8 value 0-31 .
    /// - Path element MUST NOT be "." or ".."
    /// - Note also that the Azure blob store documents say that paths SHOULD NOT use a trailing "." (as their .NET URI class strips it).
    /// - Paths are compared based on unicode code-points.
    /// - Case-insensitive and locale-specific comparisons MUST NOT not be used.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// the size of the file in bytes
    ///
    /// Metadata is not a collection, so we will not provide `is_empty`.
    /// Keep the same style with `std::fs::File`
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.size as u64
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
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// the group associated with the file
    pub fn group(&self) -> &str {
        &self.group
    }

    /// the last modification time for the file in seconds
    pub fn modified(&self) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(self.last_mod as u64))
            .expect("must be valid SystemTime")
    }

    /// the last access time for the file in seconds
    pub fn accessed(&self) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(self.last_access as u64))
            .expect("must be valid SystemTime")
    }
}

impl From<hdfsFileInfo> for Metadata {
    fn from(hfi: hdfsFileInfo) -> Self {
        Self {
            path: {
                let p = unsafe {
                    CStr::from_ptr(hfi.mName)
                        .to_str()
                        .expect("hdfs owner must be valid utf-8")
                };

                match p.find(':') {
                    None => p.to_string(),
                    Some(idx) => match &p[..idx] {
                        // `file:/path/to/file` => `/path/to/file`
                        "file" => p[idx + 1..].to_string(),
                        // `hdfs://127.0.0.1:9000/path/to/file` => `/path/to/file`
                        _ => {
                            // length of `hdfs://`
                            let scheme = idx + 2;
                            // the first occur of `/` in `127.0.0.1:9000/path/to/file`
                            let endpoint = &p[scheme + 1..]
                                .find('/')
                                .expect("hdfs must returns an absolute path");
                            p[scheme + endpoint + 1..].to_string()
                        }
                    },
                }
            },
            size: hfi.mSize,
            kind: hfi.mKind,
            permissions: hfi.mPermissions,
            replication: hfi.mReplication,
            block_size: hfi.mBlockSize,
            owner: unsafe {
                CStr::from_ptr(hfi.mOwner)
                    .to_str()
                    .expect("hdfs owner must be valid utf-8")
                    .into()
            },
            group: unsafe {
                CStr::from_ptr(hfi.mGroup)
                    .to_str()
                    .expect("hdfs owner must be valid utf-8")
                    .into()
            },
            last_mod: hfi.mLastMod,
            last_access: hfi.mLastAccess,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_from_hdfs_file_info() -> anyhow::Result<()> {
        let cases = vec![
            (
                hdfsFileInfo {
                    mKind: 0,
                    mName: CString::new("file:/path/to/file")?.into_raw(),
                    mLastMod: 0,
                    mSize: 123,
                    mReplication: 0,
                    mBlockSize: 0,
                    mOwner: CString::new("xuanwo")?.into_raw(),
                    mGroup: CString::new("xuanwo")?.into_raw(),
                    mPermissions: 0,
                    mLastAccess: 0,
                },
                Metadata {
                    path: "/path/to/file".into(),
                    size: 123,
                    kind: 0,
                    permissions: 0,
                    replication: 0,
                    block_size: 0,
                    owner: "xuanwo".into(),
                    group: "xuanwo".into(),
                    last_mod: 0,
                    last_access: 0,
                },
            ),
            (
                hdfsFileInfo {
                    mKind: 0,
                    mName: CString::new("hdfs://127.0.0.1:9000/path/to/file")?.into_raw(),
                    mLastMod: 455,
                    mSize: 0,
                    mReplication: 0,
                    mBlockSize: 0,
                    mOwner: CString::new("xuanwo")?.into_raw(),
                    mGroup: CString::new("xuanwo")?.into_raw(),
                    mPermissions: 0,
                    mLastAccess: 0,
                },
                Metadata {
                    path: "/path/to/file".into(),
                    size: 0,
                    kind: 0,
                    permissions: 0,
                    replication: 0,
                    block_size: 0,
                    owner: "xuanwo".into(),
                    group: "xuanwo".into(),
                    last_mod: 455,
                    last_access: 0,
                },
            ),
            (
                hdfsFileInfo {
                    mKind: 0,
                    mName: CString::new("/path/to/file")?.into_raw(),
                    mLastMod: 455,
                    mSize: 0,
                    mReplication: 0,
                    mBlockSize: 0,
                    mOwner: CString::new("xuanwo")?.into_raw(),
                    mGroup: CString::new("xuanwo")?.into_raw(),
                    mPermissions: 0,
                    mLastAccess: 0,
                },
                Metadata {
                    path: "/path/to/file".into(),
                    size: 0,
                    kind: 0,
                    permissions: 0,
                    replication: 0,
                    block_size: 0,
                    owner: "xuanwo".into(),
                    group: "xuanwo".into(),
                    last_mod: 455,
                    last_access: 0,
                },
            ),
        ];

        for case in cases {
            let meta = Metadata::from(case.0);

            assert_eq!(meta, case.1);
        }

        Ok(())
    }
}
