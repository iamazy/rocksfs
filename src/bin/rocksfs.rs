use clap::{crate_version, App, Arg};
use rocksfs::{mount_rocksfs_daemonize, MountOption};
use tracing::{debug, info, trace};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = App::new("rocksfs")
        .version(crate_version!())
        .author("iamazy")
        .arg(
            Arg::with_name("mount-point")
                .value_name("MOUNT_POINT")
                .required(true)
                .help("act as a client, and mount FUSE at given path")
                .index(1)
        )
        .arg(
            Arg::with_name("db-path")
                .value_name("DB_PATH")
                .required(true)
                .help("the path of db file")
                .index(2)
        )
        .arg(
            Arg::with_name("options")
                .value_name("OPTION")
                .long("option")
                .short('o')
                .multiple(true)
                .help("filesystem mount options")
        )
        .arg(
            Arg::with_name("foreground")
                .long("foreground")
                .short('f')
                .help("foreground operation")
        )
        .arg(
            Arg::with_name("serve")
                .long("serve")
                .help("run in server mode (implies --foreground)")
                .hidden(true)
        )
        .arg(
            Arg::with_name("logfile")
                .long("log-file")
                .value_name("LOGFILE")
                .help("log file in server mode (ignored if --foreground is present)")
        )
        .get_matches();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init()
        .map_err(|err| anyhow::anyhow!("fail to init tracing subscriber: {}", err))?;

    let serve = matches.is_present("serve");
    let foreground = serve || matches.is_present("foreground");
    let logfile = matches
        .value_of("logfile")
        .and_then(|v| Some(std::fs::canonicalize(v).ok()?.to_str()?.to_owned()));

    trace!("serve={} foreground={}", serve, foreground);

    let mountpoint = std::fs::canonicalize(
        &matches
            .value_of("mount-point")
            .ok_or_else(|| anyhow::anyhow!("mount-point is required"))?,
    )?
    .to_string_lossy()
    .to_string();

    let db_path = &matches
        .value_of("db-path")
        .ok_or_else(|| anyhow::anyhow!("db-path is required"))?
        .to_string();

    let options = MountOption::to_vec(matches.values_of("options").unwrap_or_default());

    let runtime_config_string = format!(
        "mountpoint={:?} dbpath={:?} opt={:?}",
        mountpoint, db_path, options
    );

    // if !foreground {
    //     use std::io::{Read, Write};
    //     use std::process::{Command, Stdio};
    //
    //     let exe = std::env::current_exe()?.to_string_lossy().to_string();
    //     debug!("launching server, current_exe={}", exe);
    //     info!("{}", runtime_config_string);
    //
    //     let mut args = vec![
    //         mountpoint.to_string(),
    //         db_path.to_string()
    //     ];
    //     if !options.is_empty() {
    //         args.push("-o".to_owned());
    //         args.push(
    //             options
    //                 .iter()
    //                 .map(|v| v.into())
    //                 .collect::<Vec<String>>()
    //                 .join(","),
    //         );
    //     }
    //     if let Some(f) = logfile {
    //         args.push("--log-file".to_owned());
    //         args.push(f);
    //     }
    //     let child = Command::new(&exe)
    //         .args(args)
    //         .current_dir("/")
    //         .stdin(Stdio::null())
    //         .stdout(Stdio::piped())
    //         .stderr(Stdio::piped())
    //         .spawn()?;
    //
    //     if let Some(mut stdout) = child.stdout {
    //         let mut my_stdout = std::io::stdout();
    //         let mut buffer: [u8; 256] = [0; 256];
    //         while let Ok(size) = stdout.read(&mut buffer) {
    //             if size == 0 {
    //                 break; // EOF
    //             }
    //             my_stdout.write_all(&buffer[0..size])?;
    //         }
    //     }
    //     if let Some(mut stderr) = child.stderr {
    //         let mut my_stderr = std::io::stderr();
    //         let mut buffer: [u8; 256] = [0; 256];
    //         while let Ok(size) = stderr.read(&mut buffer) {
    //             if size == 0 {
    //                 break; // EOF
    //             }
    //             my_stderr.write_all(&buffer[0..size])?;
    //         }
    //     }
    //     return Ok(());
    // }

    mount_rocksfs_daemonize(mountpoint.to_string(), db_path.to_string(), options, move || {
        if serve {
            use std::ffi::CString;
            use std::io::{Error, Write};

            use anyhow::bail;
            use libc;

            debug!("using log file: {:?}", logfile);

            std::io::stdout().flush()?;
            std::io::stderr().flush()?;

            let mut logfd = None;
            if let Some(f) = logfile {
                let log_file_name = CString::new(f)?;
                unsafe {
                    let fd = libc::open(log_file_name.as_ptr(), libc::O_WRONLY | libc::O_APPEND, 0);
                    if fd == -1 {
                        bail!(Error::last_os_error());
                    }
                    logfd = Some(fd);

                    libc::dup2(fd, 1);
                    libc::dup2(fd, 2);
                    if fd > 2 {
                        libc::close(fd);
                    }
                }
                debug!("output redirected");
            }

            let null_file_name = CString::new("/dev/null")?;

            unsafe {
                let nullfd = libc::open(null_file_name.as_ptr(), libc::O_RDWR, 0);
                if nullfd != -1 {
                    libc::dup2(nullfd, 0);
                    if logfd.is_none() {
                        libc::dup2(nullfd, 1);
                        libc::dup2(nullfd, 2);
                    }
                    if nullfd > 2 {
                        libc::close(nullfd);
                    }
                }
            }
        }
        debug!("{}", runtime_config_string);

        Ok(())
    })
    .await
}
