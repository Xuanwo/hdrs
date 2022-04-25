#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(deref_nullptr)]
#![allow(rustdoc::invalid_rust_codeblocks)]

pub mod hdfs_2_10_1 {
    include!(concat!(env!("OUT_DIR"), "/bindings_hdfs_2_10_1.h.rs"));
}

pub mod hdfs_3_2_3 {
    include!(concat!(env!("OUT_DIR"), "/bindings_hdfs_3_2_3.h.rs"));
}

pub mod hdfs_3_3_2 {
    include!(concat!(env!("OUT_DIR"), "/bindings_hdfs_3_3_2.h.rs"));
}
