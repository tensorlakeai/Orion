use serde::{Deserialize, Serialize};

pub(crate) fn to_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_stdvec(value)
}

pub(crate) fn from_bytes<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(bytes)
}

pub(crate) fn to_versioned_vec<T: Serialize + ?Sized>(
    version: u16,
    value: &T,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_stdvec(&(version, value))
}

pub(crate) fn from_versioned_bytes<'a, T: Deserialize<'a>>(
    bytes: &'a [u8],
) -> Result<(u16, T), postcard::Error> {
    postcard::from_bytes(bytes)
}
