use std::ops::{Deref, DerefMut};
use std::vec::IntoIter;

use crate::Metadata;

/// Readdir is an Iterator of metadata, returned by [`read_dir`][crate::Client::read_dir]
#[derive(Debug)]
pub struct Readdir {
    inner: IntoIter<Metadata>,
}

impl Readdir {
    pub fn into_inner(self) -> IntoIter<Metadata> {
        self.inner
    }
}

impl From<Vec<Metadata>> for Readdir {
    fn from(v: Vec<Metadata>) -> Self {
        Readdir {
            inner: v.into_iter(),
        }
    }
}

impl Deref for Readdir {
    type Target = IntoIter<Metadata>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Readdir {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
