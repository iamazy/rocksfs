# rocksfs

A local filesystem based on fuse and rocksdb. 

⚠️ This project is almost entirely copied from [tifs](https://github.com/Hexilee/tifs) and mainly for learning purposes

## Motivation

- learn rust and storage (e.g. fuse, object storage and so on)
- find a way to learn rocksdb and how to optimize it

## Environment

### Build

- Linux
  `libfuse` and `build-essential` are required, in ubuntu/debian:

```
sudo apt install -y libfuse-dev libfuse3-dev build-essential
```

- macOS
```
brew install --cask osxfuse
```

### Runtime
- Linux
  `fuse3` and `openssl` are required, in ubuntu/debian:

```
sudo apt-get install -y libfuse3-dev fuse3 libssl-dev
```

- macOS

```
brew install --cask osxfuse
```

In Catalina or former version, you need to load osxfuse into the kernel:

```
/Library/Filesystems/osxfuse.fs/Contents/Resources/load_osxfuse
```

or

> * Verify that Terminal has Full Disk Access: System Preference > Security & Privacy > Privacy > Full Disk Access
> * Run `sudo kextunload -b io.macfuse.filesystems.macfuse`

## Usage

```bash
git clone https://github.com/iamazy/rocksfs.git
cd rocksfs
mkdir -p ./rocksfs/fs # as mount point
mkdir -p ./rocksfs/db # save rocksdb's log
cargo run rocksfs ./rocksfs/fs ./rocksfs/db
```

## Acknowledgments

- [tifs](https://github.com/Hexilee/tifs) - A distributed POSIX filesystem based on TiKV, with partition tolerance and strict consistency.