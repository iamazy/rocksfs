use crate::fs::error::{FsError, Result};
use std::mem::size_of;
use std::ops::Range;

pub const ROOT_INODE: u64 = fuser::FUSE_ROOT_ID;

pub type Key = Vec<u8>;

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Clone, Copy)]
pub enum ScopedKey<'a> {
    Meta,
    Inode(u64),
    Block { ino: u64, block: u64 },
    FileHandler { ino: u64, handler: u64 },
    FileIndex { parent: u64, name: &'a str },
}

impl<'a> ScopedKey<'a> {
    const META: u8 = 0;
    const INODE: u8 = 1;
    const BLOCK: u8 = 2;
    const HANDLER: u8 = 3;
    const INDEX: u8 = 4;

    pub const fn meta() -> Self {
        Self::Meta
    }

    pub const fn inode(ino: u64) -> Self {
        Self::Inode(ino)
    }

    pub const fn block(ino: u64, block: u64) -> Self {
        Self::Block { ino, block }
    }

    pub const fn root() -> Self {
        Self::inode(ROOT_INODE)
    }

    pub const fn handler(ino: u64, handler: u64) -> Self {
        Self::FileHandler { ino, handler }
    }

    pub fn index(parent: u64, name: &'a str) -> Self {
        Self::FileIndex { parent, name }
    }

    pub fn block_range(ino: u64, block_range: Range<u64>) -> Range<Key> {
        debug_assert_ne!(0, ino);
        Self::block(ino, block_range.start).into()..Self::block(ino, block_range.end).into()
    }

    pub fn inode_range(ino_range: Range<u64>) -> Range<Key> {
        Self::inode(ino_range.start).into()..Self::inode(ino_range.end).into()
    }

    pub fn scope(&self) -> u8 {
        match self {
            &ScopedKey::Meta => Self::META,
            &ScopedKey::Inode(_) => Self::INODE,
            &ScopedKey::Block { ino: _, block: _ } => Self::BLOCK,
            &ScopedKey::FileHandler { ino: _, handler: _ } => Self::HANDLER,
            &ScopedKey::FileIndex { parent: _, name: _ } => Self::INDEX,
        }
    }

    pub fn len(&self) -> usize {
        1 + match self {
            &ScopedKey::Meta => 0,
            &ScopedKey::Inode(_) => size_of::<u64>(),
            &ScopedKey::Block { ino: _, block: _ } => size_of::<u64>() * 2,
            &ScopedKey::FileHandler { ino: _, handler: _ } => size_of::<u64>() * 2,
            &ScopedKey::FileIndex { parent: _, name } => size_of::<u64>() + name.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn parse(key: &'a [u8]) -> Result<Self> {
        let invalid_key = || FsError::InvalidScopedKey(key.to_owned());
        let (scope, data) = key.split_first().ok_or_else(invalid_key)?;
        match *scope {
            Self::META => Ok(Self::meta()),
            Self::INODE => {
                let ino = u64::from_be_bytes(*data.array_chunks().next().ok_or_else(invalid_key)?);
                Ok(Self::inode(ino))
            }
            Self::BLOCK => {
                let mut arrays = data.array_chunks();
                let ino = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                let block = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                Ok(Self::block(ino, block))
            }
            Self::HANDLER => {
                let mut arrays = data.array_chunks();
                let ino = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                let handler = u64::from_be_bytes(*arrays.next().ok_or_else(invalid_key)?);
                Ok(Self::handler(ino, handler))
            }
            Self::INDEX => {
                let parent =
                    u64::from_be_bytes(*data.array_chunks().next().ok_or_else(invalid_key)?);
                Ok(Self::index(
                    parent,
                    std::str::from_utf8(&data[size_of::<u64>()..]).map_err(|_| invalid_key())?,
                ))
            }
            _ => Err(invalid_key()),
        }
    }
}

impl<'a> From<ScopedKey<'a>> for Key {
    fn from(key: ScopedKey<'a>) -> Self {
        let mut data = Vec::with_capacity(key.len());
        data.push(key.scope());
        match key {
            ScopedKey::Meta => (),
            ScopedKey::Inode(ino) => data.extend(ino.to_be_bytes().iter()),
            ScopedKey::Block { ino, block } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(block.to_be_bytes().iter());
            }
            ScopedKey::FileHandler { ino, handler } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(handler.to_be_bytes().iter());
            }
            ScopedKey::FileIndex { parent, name } => {
                data.extend(parent.to_be_bytes().iter());
                data.extend(name.as_bytes().iter());
            }
        }
        data
    }
}

impl<'a> From<&ScopedKey<'a>> for Key {
    fn from(key: &ScopedKey<'a>) -> Self {
        let mut data = Vec::with_capacity(key.len());
        data.push(key.scope());
        match key {
            ScopedKey::Meta => (),
            ScopedKey::Inode(ino) => data.extend(ino.to_be_bytes().iter()),
            ScopedKey::Block { ino, block } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(block.to_be_bytes().iter());
            }
            ScopedKey::FileHandler { ino, handler } => {
                data.extend(ino.to_be_bytes().iter());
                data.extend(handler.to_be_bytes().iter());
            }
            ScopedKey::FileIndex { parent, name } => {
                data.extend(parent.to_be_bytes().iter());
                data.extend(name.as_bytes().iter());
            }
        }
        data
    }
}
