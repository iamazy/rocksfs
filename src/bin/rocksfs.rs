use clap::{crate_version, App, Arg};
use rocksfs::{mount_rocksfs_daemonize, MountOption};
use tracing::debug;
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
                .index(1),
        )
        .arg(
            Arg::with_name("db-path")
                .value_name("DB_PATH")
                .required(true)
                .help("the path of db file")
                .index(2),
        )
        .arg(
            Arg::with_name("options")
                .value_name("OPTION")
                .long("option")
                .short('o')
                .multiple(true)
                .help("filesystem mount options"),
        )
        .get_matches();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init()
        .map_err(|err| anyhow::anyhow!("fail to init tracing subscriber: {}", err))?;

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

    mount_rocksfs_daemonize(
        mountpoint.to_string(),
        db_path.to_string(),
        options,
        move || {
            debug!("{}", runtime_config_string);
            Ok(())
        },
    )
    .await
}
