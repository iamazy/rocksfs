use crate::fs::error::{FsError, Result};
use crate::fs::serialize::ENCODING;
use crate::fs::serialize::{deserialize, serialize};
use serde::{Deserialize, Serialize};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileHandler {
    pub cursor: u64,
}

impl FileHandler {
    pub const fn new(cursor: u64) -> Self {
        Self { cursor }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        serialize(self).map_err(|err| FsError::Serialize {
            target: "file handler",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        deserialize(bytes).map_err(|err| FsError::Serialize {
            target: "file handler",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }
}

impl Default for FileHandler {
    fn default() -> Self {
        Self::new(0)
    }
}
