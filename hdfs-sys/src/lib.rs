#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(deref_nullptr)]
#![allow(rustdoc::invalid_rust_codeblocks)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use libc::{c_char, c_int, c_short, c_void, time_t, O_CREAT, O_RDONLY, O_WRONLY};
    use std::ffi::CString;

    #[test]
    fn test_x() {
        unsafe {
            let fs = hdfsConnect(CString::new("default").unwrap().as_ptr(), 0);
            println!("{:?}", fs);

            let writeFile = hdfsOpenFile(
                fs,
                CString::new("/tmp/testfile.txt").unwrap().as_ptr(),
                O_WRONLY | O_CREAT,
                0,
                0,
                0,
            );
            println!("{:?}", writeFile);

            let content = "Hello, World!";
            let n = hdfsWrite(
                fs,
                writeFile,
                content.as_ptr() as *mut c_void,
                content.len() as tSize,
            );
            if hdfsFlush(fs, writeFile) != 0 {
                panic!("flush failed");
            }
            hdfsCloseFile(fs, writeFile);

            let readFile = hdfsOpenFile(
                fs,
                CString::new("/tmp/testfile.txt").unwrap().as_ptr(),
                O_RDONLY,
                0,
                0,
                0,
            );
            println!("{:?}", readFile);

            let content = String::new();
            let n = hdfsRead(fs, readFile, content.as_ptr() as *mut c_void, 13);
            println!("read: {:?}", content);
            hdfsCloseFile(fs, readFile);
        }
    }
}
