#![feature(array_chunks)]
#[allow(clippy::too_many_arguments)]
pub mod fs;

use crate::fs::rocks_fs::RocksFs;
use fs::async_fs::AsyncFs;
use fuser::MountOption as FuseMountOption;
use paste::paste;

macro_rules! define_options {
    {
        $name: ident ($type: ident) {
            $(builtin $($optname: literal)? $opt: ident,)*
            $(define $($newoptname: literal)? $newopt: ident $( ( $optval: ident ) )? ,)*
        }
    } =>
    {
        #[derive(Debug,Clone,PartialEq)]
        pub enum $name {
            Unknown(String),
            $($opt,)*
            $($newopt $(($optval))?,)*
        }
        impl $name {
            pub fn to_vec<'a, I: Iterator<Item=&'a str>>(iter: I) -> Vec<Self> {
                iter.map(|v| v.split(',').map(Self::from)).flatten().collect()
            }
            pub fn collect_builtin<'a, I: Iterator<Item=&'a Self>>(iter: I) -> Vec<$type> {
                iter.filter_map(|v| v.to_builtin()).collect()
            }
            pub fn to_builtin(&self) -> Option<$type> {
                match self {
                    $(Self::$opt => Some($type::$opt),)*
                    _ => None,
                }
            }
        }
        paste! {
            impl std::str::FromStr for $name {
                type Err = anyhow::Error;
                fn from_str(fullopt: &str) -> Result<Self, Self::Err> {
                    let mut splitter = fullopt.splitn(2, '=');
                    let optname = splitter.next().unwrap_or("");
                    let optval = splitter.next();
                    let optval_present = optval.is_some();
                    let optval = optval.unwrap_or("");

                    let (parsed, optval_used) = match &optname as &str {
                        // "dirsync" => ( Self::DirSync, false),
                        // "direct_io" if "" != "directio" => ( Self::DirectIO, false),
                        // "blksize" => ( Self::BlkSize ( "0".parse::<u64>()? , false || (None as Option<u64>).is_none() ),
                        $( $($optname if "" != )? stringify!([<$opt:lower>]) => (Self::$opt, false), )*
                        $(
                            $($newoptname if "" != )? stringify!([<$newopt:lower>]) => (
                                Self::$newopt $(( optval.parse::<$optval>()?))? , false $( || (None as Option<$optval>).is_none() )?
                            ),
                        )*
                        _ => (Self::Unknown(fullopt.to_owned()), false),
                    };

                    if !optval_used && optval_present {
                        Err(anyhow::anyhow!("Option {} do not accept an argument", optname))
                    } else if optval_used && !optval_present {
                        Err(anyhow::anyhow!("Argument for {} is not supplied", optname))
                    } else {
                        Ok(parsed)
                    }
                }
            }
            impl<T> From<T> for $name
            where
                T: ToString
            {
                fn from(v: T) -> Self {
                    let fullopt = v.to_string();
                    match fullopt.parse::<Self>() {
                        Ok(v) => v,
                        Err(_) => Self::Unknown(v.to_string()),
                    }
                }
            }
            impl From<$name> for String {
                fn from(v: $name) -> Self {
                    Self::from(&v)
                }
            }
            impl From<&$name> for String {
                fn from(v: &$name) -> Self {
                    match v {
                        // MountOption::DirSync => ("dirsync", "").0.to_owned() ,
                        // MountOption::DirectIO => format!(concat!("{}"), ("direct_io", "directio", "").0 ),
                        // MountOption::BlkSize (v) => format!(concat!("{}", "={}", ""), ("blksize", "").0, v.to_owned() as u64 ),
                        $($name::$opt => ( $($optname,)? stringify!([<$opt:lower>]), "" ).0 .to_owned() , )*
                        $(
                            $name::$newopt $( ( define_options!(@ignore $optval v) ) )? =>
                                format!(
                                    concat!("{}" $(,"={}", define_options!(@ignore $optval) )? ),
                                    ( $($newoptname,)? stringify!([<$newopt:lower>]), "" ).0
                                    $( , v.to_owned() as $optval )?
                                ),
                        )*
                        $name::Unknown(v) => v.to_owned(),
                    }
                }
            }
        }
    };

    // internal rules
    {@ignore $id: tt } => { "" };
    {@ignore $id: tt $($replacement: tt),* } => { $($replacement),* };
}

define_options! { MountOption (FuseMountOption) {
    builtin Dev,
    builtin NoDev,
    builtin Suid,
    builtin NoSuid,
    builtin RO,
    builtin RW,
    builtin Exec,
    builtin NoExec,
    builtin DirSync,
    define "direct_io" DirectIO,
    define BlkSize(String),
    define MaxSize(String), // size of filesystem
    define Tls(String),
//    define "opt" OptionName(Display_Debug_Clone_PartialEq_FromStr_able)
}}

pub async fn mount_rocksfs_daemonize<F>(
    mountpoint: String,
    db_path: String,
    options: Vec<MountOption>,
    make_daemon: F,
) -> anyhow::Result<()>
where
    F: FnOnce() -> anyhow::Result<()>,
{
    let mut fuse_options = vec![
        FuseMountOption::FSName(format!("rocksfs:{}", mountpoint)),
        FuseMountOption::AllowOther,
        FuseMountOption::DefaultPermissions,
    ];

    #[cfg(target_os = "linux")]
    fuse_options.push(FuseMountOption::AutoUnmount);

    fuse_options.extend(MountOption::collect_builtin(options.iter()));

    let fs_impl = RocksFs::construct(db_path, options).await?;

    make_daemon()?;

    fuser::mount2(AsyncFs::from(fs_impl), mountpoint, &fuse_options)?;

    Ok(())
}

pub async fn mount_rocksfs(
    mountpoint: String,
    path: String,
    options: Vec<MountOption>,
) -> anyhow::Result<()> {
    mount_rocksfs_daemonize(mountpoint, path, options, || Ok(())).await
}
