use crate::fs::error::{FsError, Result};
use crate::fs::key::ROOT_INODE;
use crate::fs::reply::StatFs;
use crate::fs::serialize::{deserialize, serialize, ENCODING};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Meta {
    pub inode_next: u64,
    pub block_size: u64,
    pub last_stat: Option<StatFs>,
}

impl Meta {
    pub const fn new(block_size: u64) -> Self {
        Self {
            inode_next: ROOT_INODE,
            block_size,
            last_stat: None,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        serialize(self).map_err(|err| FsError::Serialize {
            target: "meta",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        deserialize(bytes).map_err(|err| FsError::Serialize {
            target: "meta",
            typ: ENCODING,
            msg: err.to_string(),
        })
    }
}
