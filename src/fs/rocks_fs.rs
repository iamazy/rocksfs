use crate::fs::async_fs::AsyncFileSystem;
use crate::fs::dir::Directory;
use crate::fs::error::{FsError, Result};
use crate::fs::key::ROOT_INODE;
use crate::fs::mode::make_mode;
use crate::fs::reply::{
    get_time, Attr, Bmap, Create, Data, Dir, DirPlus, Entry, Lock, Lseek, Open, StatFs, Write,
    Xattr,
};
use crate::fs::transaction::Txn;
use crate::MountOption;

use bytes::Bytes;
use bytestring::ByteString;
use fuser::consts::FOPEN_DIRECT_IO;
use fuser::{FileAttr, FileType, KernelConfig, TimeOrNow};
use parse_size::parse_size;
use std::fmt::{Debug, Formatter};
use std::future::Future;

use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing::{debug, error, instrument, trace, warn};

pub const DIR_SELF: ByteString = ByteString::from_static(".");
pub const DIR_PARENT: ByteString = ByteString::from_static("..");

pub struct RocksFs {
    pub db: rocksdb::TransactionDB,
    pub direct_io: bool,
    pub block_size: u64,
    pub max_size: Option<u64>,
}

impl Debug for RocksFs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksFs")
            .field("direct_io", &self.direct_io)
            .field("block_size", &self.block_size)
            .field("max_size", &self.max_size)
            .finish()
    }
}

type BoxedFuture<'a, T> = Pin<Box<dyn 'a + Send + Future<Output = Result<T>>>>;

impl RocksFs {
    pub const SCAN_LIMIT: u32 = 1 << 10;
    pub const DEFAULT_BLOCK_SIZE: u64 = 1 << 16;
    pub const MAX_NAME_LEN: u32 = 1 << 8;

    #[instrument]
    pub async fn construct(path: String, options: Vec<MountOption>) -> anyhow::Result<Self> {
        let db = rocksdb::TransactionDB::open_default(path)?;
        Ok(RocksFs {
            db,
            direct_io: options
                .iter()
                .any(|option| matches!(option, MountOption::DirectIO)),
            block_size: options
                .iter()
                .find_map(|option| match option {
                    MountOption::BlkSize(size) => parse_size(size)
                        .map_err(|err| {
                            error!("fail to parse blksize({}): {}", size, err);
                            err
                        })
                        .map(|size| {
                            debug!("block size: {}", size);
                            size
                        })
                        .ok(),
                    _ => None,
                })
                .unwrap_or(Self::DEFAULT_BLOCK_SIZE),
            max_size: options.iter().find_map(|option| match option {
                MountOption::MaxSize(size) => parse_size(size)
                    .map_err(|err| {
                        error!("fail to parse maxsize({}): {}", size, err);
                        err
                    })
                    .map(|size| {
                        debug!("max size: {}", size);
                        size
                    })
                    .ok(),
                _ => None,
            }),
        })
    }

    async fn process_txn<F, T>(&self, txn: Txn<'_>, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a RocksFs, &'a Txn) -> BoxedFuture<'a, T>,
    {
        match f(self, &txn).await {
            Ok(v) => {
                let commit_start = SystemTime::now();
                txn.txn.commit()?;
                debug!(
                    "transaction committed in {} ms",
                    commit_start.elapsed().unwrap().as_millis()
                );
                Ok(v)
            }
            Err(e) => {
                debug!("transaction rollback");
                txn.txn.rollback()?;
                Err(e)
            }
        }
    }

    async fn with_optimistic<F, T>(&self, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a RocksFs, &'a Txn) -> BoxedFuture<'a, T>,
    {
        let rocks_txn = self.db.transaction();
        let txn = Txn::begin_optimistic(
            rocks_txn,
            self.block_size,
            self.max_size,
            Self::MAX_NAME_LEN,
        )
        .await;
        self.process_txn(txn, f).await
    }

    async fn spin<F, T>(&self, delay: Option<Duration>, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> Fn(&'a RocksFs, &'a Txn<'_>) -> BoxedFuture<'a, T>,
    {
        loop {
            match self.with_optimistic(&f).await {
                Ok(v) => break Ok(v),
                Err(FsError::KeyError(err)) => {
                    trace!("spin because of a key error({})", err);
                    if let Some(time) = delay {
                        sleep(time).await;
                    }
                }
                Err(err) => break Err(err),
            }
        }
    }

    async fn spin_no_delay<F, T>(&self, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> Fn(&'a RocksFs, &'a Txn) -> BoxedFuture<'a, T>,
    {
        self.spin(None, f).await
    }

    async fn read_dir(&self, ino: u64) -> Result<Directory> {
        self.spin_no_delay(move |_, txn| Box::pin(txn.read_dir(ino)))
            .await
    }

    async fn read_inode(&self, ino: u64) -> Result<FileAttr> {
        let ino = self
            .spin_no_delay(move |_, txn| Box::pin(txn.read_inode(ino)))
            .await?;
        Ok(ino.file_attr)
    }

    async fn setlkw(
        &self,
        ino: u64,
        lock_owner: u64,
        #[cfg(target_os = "linux")] typ: i32,
        #[cfg(any(target_os = "freebsd", target_os = "macos"))] typ: i16,
    ) -> Result<()> {
        while !self
            .spin_no_delay(move |_, txn| {
                Box::pin(async move {
                    let mut inode = txn.read_inode(ino).await?;
                    match typ {
                        libc::F_WRLCK => {
                            if inode.lock_state.owner_set.len() > 1 {
                                Ok(false)
                            } else if inode.lock_state.owner_set.is_empty() {
                                inode.lock_state.lk_type = libc::F_WRLCK;
                                inode.lock_state.owner_set.insert(lock_owner);
                                txn.save_inode(&inode).await?;
                                Ok(true)
                            } else if inode.lock_state.owner_set.get(&lock_owner)
                                == Some(&lock_owner)
                            {
                                inode.lock_state.lk_type = libc::F_WRLCK;
                                txn.save_inode(&inode).await?;
                                Ok(true)
                            } else {
                                Err(FsError::InvalidLock)
                            }
                        }
                        libc::F_RDLCK => {
                            if inode.lock_state.lk_type == libc::F_WRLCK {
                                Ok(false)
                            } else {
                                inode.lock_state.lk_type = libc::F_RDLCK;
                                inode.lock_state.owner_set.insert(lock_owner);
                                txn.save_inode(&inode).await?;
                                Ok(true)
                            }
                        }
                        _ => Err(FsError::InvalidLock),
                    }
                })
            })
            .await?
        {}
        Ok(())
    }

    fn check_file_name(name: &str) -> Result<()> {
        if name.len() <= Self::MAX_NAME_LEN as usize {
            Ok(())
        } else {
            Err(FsError::NameTooLong {
                file: name.to_string(),
            })
        }
    }
}

impl AsyncFileSystem for RocksFs {
    type InitResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type DestroyResultFuture<'a> = impl Future<Output = ()> + Send + 'a;
    type LookupResultFuture<'a> = impl Future<Output = Result<Entry>> + Send + 'a;
    type ForgetResultFuture<'a> = impl Future<Output = ()> + Send + 'a;
    type GetAttrResultFuture<'a> = impl Future<Output = Result<Attr>> + Send + 'a;
    type SetAttrResultFuture<'a> = impl Future<Output = Result<Attr>> + Send + 'a;
    type ReadLinkResultFuture<'a> = impl Future<Output = Result<Data>> + Send + 'a;
    type MkNodResultFuture<'a> = impl Future<Output = Result<Entry>> + Send + 'a;
    type MkDirResultFuture<'a> = impl Future<Output = Result<Entry>> + Send + 'a;
    type UnlinkResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type RmDirResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type SymlinkResultFuture<'a> = impl Future<Output = Result<Entry>> + Send + 'a;
    type RenameResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type LinkResultFuture<'a> = impl Future<Output = Result<Entry>> + Send + 'a;
    type OpenResultFuture<'a> = impl Future<Output = Result<Open>> + Send + 'a;
    type ReadResultFuture<'a> = impl Future<Output = Result<Data>> + Send + 'a;
    type WriteResultFuture<'a> = impl Future<Output = Result<Write>> + Send + 'a;
    type ReleaseResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type FlushResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type FsyncResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type OpenDirResultFuture<'a> = impl Future<Output = Result<Open>> + Send + 'a;
    type ReadDirResultFuture<'a> = impl Future<Output = Result<Dir>> + Send + 'a;
    type ReadDirPlusResultFuture<'a> = impl Future<Output = Result<DirPlus>> + Send + 'a;
    type ReleaseDirResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type FsyncDirResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type StatFsResultFuture<'a> = impl Future<Output = Result<StatFs>> + Send + 'a;
    type SetxAttrResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type GetxAttrResultFuture<'a> = impl Future<Output = Result<Xattr>> + Send + 'a;
    type ListxAttrResultFuture<'a> = impl Future<Output = Result<Xattr>> + Send + 'a;
    type RemovexAttrResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type AccessResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type CreateResultFuture<'a> = impl Future<Output = Result<Create>> + Send + 'a;
    type GetlkResultFuture<'a> = impl Future<Output = Result<Lock>> + Send + 'a;
    type SetlkResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type FallocateResultFuture<'a> = impl Future<Output = Result<()>> + Send + 'a;
    type BmapResultFuture<'a> = impl Future<Output = Result<Bmap>> + Send + 'a;
    type LseekResultFuture<'a> = impl Future<Output = Result<Lseek>> + Send + 'a;
    type CopyFileRangeResultFuture<'a> = impl Future<Output = Result<Write>> + Send + 'a;

    #[instrument]
    #[allow(unused_variables)]
    fn init<'a>(
        &'a self,
        gid: u32,
        uid: u32,
        config: &'a mut KernelConfig,
    ) -> Self::InitResultFuture<'_> {
        async move {
            #[cfg(not(target_os = "macos"))]
            config
                .add_capabilities(fuser::consts::FUSE_POSIX_LOCKS)
                .expect("kernel config failed to add cap_fuse FUSE_POSIX_LOCKS");

            self.spin_no_delay(move |_fs, txn| {
                Box::pin(async move {
                    if let Some(meta) = txn.read_meta().await? {
                        if meta.block_size != txn.block_size() {
                            let err =
                                FsError::block_size_conflict(meta.block_size, txn.block_size());
                            error!("{}", err);
                            return Err(err);
                        }
                    }

                    let root_inode = txn.read_inode(ROOT_INODE).await;
                    if let Err(FsError::InodeNotFound { inode: _ }) = root_inode {
                        let attr = txn
                            .mkdir(
                                0,
                                Default::default(),
                                make_mode(FileType::Directory, 0o777),
                                gid,
                                uid,
                            )
                            .await?;
                        debug!("make root directory {:?}", &attr);
                        Ok(())
                    } else {
                        root_inode.map(|_| ())
                    }
                })
            })
            .await
        }
    }

    #[instrument]
    fn destroy(&self) -> Self::DestroyResultFuture<'_> {
        async move {}
    }

    #[instrument]
    fn lookup(&self, parent: u64, name: ByteString) -> Self::LookupResultFuture<'_> {
        async move {
            Self::check_file_name(&name)?;
            self.spin_no_delay(move |_, txn| {
                let name = name.clone();
                Box::pin(async move {
                    let ino = txn.lookup(parent, name).await?;
                    Ok(Entry::new(txn.read_inode(ino).await?.into(), 0))
                })
            })
            .await
        }
    }

    #[instrument]
    fn forget(&self, _ino: u64, _nlookup: u64) -> Self::ForgetResultFuture<'_> {
        async move {}
    }

    #[instrument]
    fn getattr(&self, ino: u64) -> Self::GetAttrResultFuture<'_> {
        async move { Ok(Attr::new(self.read_inode(ino).await?)) }
    }

    #[instrument]
    fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Self::SetAttrResultFuture<'_> {
        async move {
            self.spin_no_delay(move |_, txn| {
                Box::pin(async move {
                    // TODO: how to deal with fh, chgtime, bkuptime?
                    let mut attr = txn.read_inode(ino).await?;
                    attr.perm = match mode {
                        Some(m) => m as _,
                        None => attr.perm,
                    };
                    attr.uid = uid.unwrap_or(attr.uid);
                    attr.gid = gid.unwrap_or(attr.gid);
                    attr.set_size(size.unwrap_or(attr.size), txn.block_size());
                    attr.atime = match atime {
                        None => attr.atime,
                        Some(TimeOrNow::SpecificTime(t)) => t,
                        Some(TimeOrNow::Now) => SystemTime::now(),
                    };
                    attr.mtime = match mtime {
                        Some(TimeOrNow::SpecificTime(t)) => t,
                        Some(TimeOrNow::Now) | None => SystemTime::now(),
                    };
                    attr.ctime = ctime.unwrap_or_else(SystemTime::now);
                    attr.crtime = crtime.unwrap_or(attr.crtime);
                    attr.flags = flags.unwrap_or(attr.flags);
                    txn.save_inode(&attr).await?;
                    Ok(Attr {
                        time: get_time(),
                        attr: attr.into(),
                    })
                })
            })
            .await
        }
    }

    #[instrument]
    fn readlink(&self, ino: u64) -> Self::ReadLinkResultFuture<'_> {
        async move {
            self.spin(None, move |_, txn| {
                Box::pin(async move { Ok(Data::new(txn.read_link(ino).await?)) })
            })
            .await
        }
    }

    #[instrument]
    fn mknod(
        &self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        _umask: u32,
        rdev: u32,
    ) -> Self::MkNodResultFuture<'_> {
        async move {
            Self::check_file_name(&name)?;
            let attr = self
                .spin_no_delay(move |_, txn| {
                    Box::pin(txn.make_inode(parent, name.clone(), mode, gid, uid, rdev))
                })
                .await?;
            Ok(Entry::new(attr.into(), 0))
        }
    }

    /// Create a directory.
    #[instrument]
    fn mkdir(
        &self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        _umask: u32,
    ) -> Self::MkDirResultFuture<'_> {
        async move {
            Self::check_file_name(&name)?;
            let attr = self
                .spin_no_delay(move |_, txn| {
                    Box::pin(txn.mkdir(parent, name.clone(), mode, gid, uid))
                })
                .await?;
            Ok(Entry::new(attr.into(), 0))
        }
    }

    #[instrument]
    fn unlink(&self, parent: u64, raw_name: ByteString) -> Self::UnlinkResultFuture<'_> {
        async move {
            self.spin_no_delay(move |_, txn| Box::pin(txn.unlink(parent, raw_name.clone())))
                .await
        }
    }

    #[instrument]
    fn rmdir(&self, parent: u64, raw_name: ByteString) -> Self::RmDirResultFuture<'_> {
        async move {
            Self::check_file_name(&raw_name)?;
            self.spin_no_delay(move |_, txn| Box::pin(txn.rmdir(parent, raw_name.clone())))
                .await
        }
    }

    #[instrument]
    fn symlink(
        &self,
        gid: u32,
        uid: u32,
        parent: u64,
        name: ByteString,
        link: ByteString,
    ) -> Self::SymlinkResultFuture<'_> {
        async move {
            Self::check_file_name(&name)?;
            self.spin_no_delay(move |_, txn| {
                let name = name.clone();
                let link = link.clone();
                Box::pin(async move {
                    let mut attr = txn
                        .make_inode(
                            parent,
                            name,
                            make_mode(FileType::Symlink, 0o777),
                            gid,
                            uid,
                            0,
                        )
                        .await?;

                    txn.write_link(&mut attr, link.into_bytes()).await?;
                    Ok(Entry::new(attr.into(), 0))
                })
            })
            .await
        }
    }

    #[instrument]
    fn rename(
        &self,
        parent: u64,
        raw_name: ByteString,
        newparent: u64,
        new_raw_name: ByteString,
        _flags: u32,
    ) -> Self::RenameResultFuture<'_> {
        async move {
            Self::check_file_name(&raw_name)?;
            Self::check_file_name(&new_raw_name)?;
            self.spin_no_delay(move |_, txn| {
                let name = raw_name.clone();
                let new_name = new_raw_name.clone();
                Box::pin(async move {
                    let ino = txn.lookup(parent, name.clone()).await?;
                    txn.link(ino, newparent, new_name).await?;
                    txn.unlink(parent, name).await?;
                    let inode = txn.read_inode(ino).await?;
                    if inode.file_attr.kind == FileType::Directory {
                        txn.unlink(ino, DIR_PARENT).await?;
                        txn.link(newparent, ino, DIR_PARENT).await?;
                    }
                    Ok(())
                })
            })
            .await
        }
    }

    /// Create a hard link.
    #[instrument]
    fn link(&self, ino: u64, newparent: u64, newname: ByteString) -> Self::LinkResultFuture<'_> {
        async move {
            Self::check_file_name(&newname)?;
            let inode = self
                .spin_no_delay(move |_, txn| Box::pin(txn.link(ino, newparent, newname.clone())))
                .await?;
            Ok(Entry::new(inode.into(), 0))
        }
    }

    #[instrument]
    #[allow(unused_variables)]
    fn open(&self, ino: u64, flags: i32) -> Self::OpenResultFuture<'_> {
        async move {
            // TODO: deal with flags
            let fh = self
                .spin_no_delay(move |_, txn| Box::pin(txn.open(ino)))
                .await?;

            let mut open_flags = 0;
            #[cfg(target_os = "linux")]
            if self.direct_io || flags & libc::O_DIRECT != 0 {
                open_flags |= FOPEN_DIRECT_IO;
            }
            #[cfg(not(target_os = "linux"))]
            if self.direct_io {
                open_flags |= FOPEN_DIRECT_IO;
            }
            Ok(Open::new(fh, open_flags))
        }
    }

    #[instrument]
    fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Self::ReadResultFuture<'_> {
        async move {
            let data = self
                .spin_no_delay(move |_, txn| Box::pin(txn.read(ino, fh, offset, size)))
                .await?;
            Ok(Data::new(data))
        }
    }

    #[instrument(skip(data))]
    fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Self::WriteResultFuture<'_> {
        async move {
            let data: Bytes = data.into();
            let len = self
                .spin_no_delay(move |_, txn| Box::pin(txn.write(ino, fh, offset, data.clone())))
                .await?;
            Ok(Write::new(len as u32))
        }
    }

    #[instrument]
    fn release(
        &self,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
    ) -> Self::ReleaseResultFuture<'_> {
        async move {
            self.spin_no_delay(move |_, txn| Box::pin(txn.close(ino, fh)))
                .await
        }
    }

    #[instrument]
    fn flush(&self, _ino: u64, _fh: u64, _lock_owner: u64) -> Self::FlushResultFuture<'_> {
        async move { Err(FsError::unimplemented()) }
    }

    #[instrument]
    fn fsync(&self, _ino: u64, _fh: u64, _datasync: bool) -> Self::FsyncResultFuture<'_> {
        async move { Err(FsError::unimplemented()) }
    }

    #[instrument]
    fn opendir(&self, _ino: u64, _flags: i32) -> Self::OpenDirResultFuture<'_> {
        async move { Ok(Open::new(0, 0)) }
    }

    #[instrument]
    fn readdir(&self, ino: u64, _fh: u64, offset: i64) -> Self::ReadDirResultFuture<'_> {
        async move {
            let mut dir = Dir::offset(offset as usize);
            let directory = self.read_dir(ino).await?;
            for item in directory.into_iter().skip(offset as usize) {
                dir.push(item)
            }
            debug!("read directory {:?}", &dir);
            Ok(dir)
        }
    }

    #[instrument]
    fn readdirplus(&self, _ino: u64, _fh: u64, offset: i64) -> Self::ReadDirPlusResultFuture<'_> {
        async move { Ok(DirPlus::offset(offset as usize)) }
    }

    #[instrument]
    fn releasedir(&self, _ino: u64, _fh: u64, _flags: i32) -> Self::ReleaseDirResultFuture<'_> {
        async move { Err(FsError::unimplemented()) }
    }

    #[instrument]
    fn fsyncdir(&self, _ino: u64, _fh: u64, _datasync: bool) -> Self::FsyncDirResultFuture<'_> {
        async move { Err(FsError::unimplemented()) }
    }

    // TODO: Find an api to calculate total and available space on rocksdb.
    #[instrument]
    fn statfs(&self, _ino: u64) -> Self::StatFsResultFuture<'_> {
        async move { self.spin_no_delay(|_, txn| Box::pin(txn.statfs())).await }
    }

    /// Set an extended attribute.
    #[instrument]
    fn setxattr(
        &self,
        _ino: u64,
        _name: ByteString,
        _value: Vec<u8>,
        _flags: i32,
        _position: u32,
    ) -> Self::SetxAttrResultFuture<'_> {
        async move {
            // TODO: implement me
            Ok(())
        }
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    #[instrument]
    fn getxattr(&self, _ino: u64, _name: ByteString, size: u32) -> Self::GetxAttrResultFuture<'_> {
        async move {
            // TODO: implement me
            if size == 0 {
                Ok(Xattr::size(0))
            } else {
                Ok(Xattr::data(Vec::new()))
            }
        }
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    #[instrument]
    fn listxattr(&self, _ino: u64, size: u32) -> Self::ListxAttrResultFuture<'_> {
        async move {
            // TODO: implement me
            if size == 0 {
                Ok(Xattr::size(0))
            } else {
                Ok(Xattr::data(Vec::new()))
            }
        }
    }

    /// Remove an extended attribute.
    #[instrument]
    fn removexattr(&self, _ino: u64, _name: ByteString) -> Self::RemovexAttrResultFuture<'_> {
        async move {
            // TODO: implement me
            Ok(())
        }
    }

    #[instrument]
    fn access(&self, _ino: u64, _mask: i32) -> Self::AccessResultFuture<'_> {
        async move { Ok(()) }
    }

    #[instrument]
    fn create(
        &self,
        uid: u32,
        gid: u32,
        parent: u64,
        name: ByteString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Self::CreateResultFuture<'_> {
        async move {
            Self::check_file_name(&name)?;
            let entry = self.mknod(parent, name, mode, gid, uid, umask, 0).await?;
            let open = self.open(entry.stat.ino, flags).await?;
            Ok(Create::new(
                entry.stat,
                entry.generation,
                open.fh,
                open.flags,
            ))
        }
    }

    #[instrument]
    fn getlk(
        &self,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: i32,
        pid: u32,
    ) -> Self::GetlkResultFuture<'_> {
        async move {
            // TODO: read only operation need not txn?
            self.spin_no_delay(move |_, txn| {
                Box::pin(async move {
                    let inode = txn.read_inode(ino).await?;
                    warn!("getlk, inode:{:?}, pid:{:?}", inode, pid);
                    Ok(Lock::new(0, 0, inode.lock_state.lk_type as i32, 0))
                })
            })
            .await
        }
    }

    #[instrument]
    fn setlk(
        &self,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        _start: u64,
        _end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
    ) -> Self::SetlkResultFuture<'_> {
        async move {
            #[cfg(any(target_os = "freebsd", target_os = "macos"))]
            let typ = typ as i16;
            let not_again = self.spin_no_delay(move |_, txn| {
                Box::pin(async move {
                    let mut inode = txn.read_inode(ino).await?;
                    warn!("setlk, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                    if inode.file_attr.kind == FileType::Directory {
                        return Err(FsError::InvalidLock);
                    }
                    match typ {
                        libc::F_RDLCK if inode.lock_state.lk_type == libc::F_WRLCK => {
                            if sleep {
                                warn!("setlk F_RDLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                Ok(false)
                            } else {
                                Err(FsError::InvalidLock)
                            }
                        }
                        libc::F_RDLCK => {
                            inode.lock_state.owner_set.insert(lock_owner);
                            inode.lock_state.lk_type = libc::F_RDLCK;
                            txn.save_inode(&inode).await?;
                            warn!("setlk F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            Ok(true)
                        }
                        libc::F_WRLCK => match inode.lock_state.lk_type {
                            libc::F_RDLCK if inode.lock_state.owner_set.len() == 1
                                && inode.lock_state.owner_set.get(&lock_owner) == Some(&lock_owner) => {
                                inode.lock_state.lk_type = libc::F_WRLCK;
                                txn.save_inode(&inode).await?;
                                warn!("setlk F_WRLCK on F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                Ok(true)
                            }
                            libc::F_RDLCK if sleep => {
                                warn!("setlk F_WRLCK on F_RDLCK sleep return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                Ok(false)
                            },
                            libc::F_RDLCK => Err(FsError::InvalidLock),
                            libc::F_UNLCK => {
                                inode.lock_state.owner_set.clear();
                                inode.lock_state.owner_set.insert(lock_owner);
                                inode.lock_state.lk_type = libc::F_WRLCK;
                                warn!("setlk F_WRLCK on F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                txn.save_inode(&inode).await?;
                                Ok(true)
                            },
                            libc::F_WRLCK if sleep => {
                                warn!("setlk F_WRLCK on F_WRLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                Ok(false)
                            }
                            libc::F_WRLCK => Err(FsError::InvalidLock),
                            _ => Err(FsError::InvalidLock),
                        },
                        libc::F_UNLCK => {
                            inode.lock_state.owner_set.remove(&lock_owner);
                            if inode.lock_state.owner_set.is_empty() {
                                inode.lock_state.lk_type = libc::F_UNLCK;
                            }
                            txn.save_inode(&inode).await?;
                            warn!("setlk F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            Ok(true)
                        }
                        _ => Err(FsError::InvalidLock),
                    }
                })
            })
                .await?;

            if !not_again {
                self.setlkw(ino, lock_owner, typ).await
            } else {
                Ok(())
            }
        }
    }

    #[instrument]
    fn bmap(&self, _ino: u64, _blocksize: u32, _idx: u64) -> Self::BmapResultFuture<'_> {
        async move { Err(FsError::unimplemented()) }
    }

    #[instrument]
    fn fallocate(
        &self,
        ino: u64,
        _fh: u64,
        offset: i64,
        length: i64,
        _mode: i32,
    ) -> Self::FallocateResultFuture<'_> {
        async move {
            self.spin_no_delay(move |_, txn| {
                Box::pin(async move {
                    let mut inode = txn.read_inode(ino).await?;
                    txn.fallocate(&mut inode, offset, length).await
                })
            })
            .await?;
            Ok(())
        }
    }

    #[instrument]
    fn lseek(&self, ino: u64, fh: u64, offset: i64, whence: i32) -> Self::LseekResultFuture<'_> {
        async move {
            self.spin_no_delay(move |_, txn| {
                Box::pin(async move {
                    let mut file_handler = txn.read_fh(ino, fh).await?;
                    let inode = txn.read_inode(ino).await?;
                    let target_cursor = match whence {
                        libc::SEEK_SET => offset,
                        libc::SEEK_CUR => file_handler.cursor as i64 + offset,
                        libc::SEEK_END => inode.size as i64 + offset,
                        _ => return Err(FsError::UnknownWhence { whence }),
                    };

                    if target_cursor < 0 {
                        return Err(FsError::InvalidOffset {
                            ino: inode.ino,
                            offset: target_cursor,
                        });
                    }

                    file_handler.cursor = target_cursor as u64;
                    txn.save_fh(ino, fh, &file_handler).await?;
                    Ok(Lseek::new(target_cursor))
                })
            })
            .await
        }
    }

    #[instrument]
    fn copy_file_range(
        &self,
        _ino_in: u64,
        _fh_in: u64,
        _offset_in: i64,
        _ino_out: u64,
        _fh_out: u64,
        _offset_out: i64,
        _len: u64,
        _flags: u32,
    ) -> Self::CopyFileRangeResultFuture<'_> {
        async move { Err(FsError::unimplemented()) }
    }
}
