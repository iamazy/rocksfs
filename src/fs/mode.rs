use fuser::FileType;

pub const fn as_file_perm(mode: u32) -> u16 {
    (mode & !(libc::S_ISUID | libc::S_ISGID) as u32) as _
}

#[cfg(any(target_os = "freebsd", target_os = "macos"))]
pub fn as_file_kind(mode: u32) -> FileType {
    match mode as u16 & libc::S_IFMT {
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFSOCK => FileType::Socket,
        _ => unimplemented!("{}", mode),
    }
}

#[cfg(any(target_os = "linux"))]
pub fn as_file_kind(mode: u32) -> FileType {
    match mode & libc::S_IFMT as u32 {
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFSOCK => FileType::Socket,
        _ => unimplemented!("{}", mode),
    }
}

#[cfg(any(target_os = "freebsd", target_os = "macos"))]
pub fn make_mode(typ: FileType, perm: u16) -> u32 {
    let kind = match typ {
        FileType::RegularFile => libc::S_IFREG,
        FileType::Symlink => libc::S_IFLNK,
        FileType::Directory => libc::S_IFDIR,
        FileType::NamedPipe => libc::S_IFIFO,
        FileType::BlockDevice => libc::S_IFBLK,
        FileType::CharDevice => libc::S_IFCHR,
        FileType::Socket => libc::S_IFSOCK,
    };

    kind as u32 | perm as u32
}

#[cfg(any(target_os = "linux"))]
pub fn make_mode(typ: FileType, perm: u16) -> u32 {
    let kind = match typ {
        FileType::RegularFile => libc::S_IFREG,
        FileType::Symlink => libc::S_IFLNK,
        FileType::Directory => libc::S_IFDIR,
        FileType::NamedPipe => libc::S_IFIFO,
        FileType::BlockDevice => libc::S_IFBLK,
        FileType::CharDevice => libc::S_IFCHR,
        FileType::Socket => libc::S_IFSOCK,
    };

    kind | perm as u32
}
