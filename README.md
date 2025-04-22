# Carol

Asynchronous managed storage of files in the filesystem. Designed to manage a set of huge files.

The main idea of Carol is to pair files stored in a filesystem directory with a database holding
additional metadata and providing synchronization.

Source tracking and storage eviction features allows to use Carol as a **cache backend** when
fetching huge files from the network (see [`carol-reqwest-middleware`][1]).

This repository contains:

- `carol` - main library crate
- `carol-reqwest-middleware` - HTTP caching middleware for [`reqwest`][2] library

Find out more from docs:

```shell
cargo doc --open
```

## API example

```rust
use carol::{FileSource, StorageManager, StorePolicy};

let cache_dir = "/tmp/carol";

// Create storage directory
std::fs::create_dir_all(&cache_dir).unwrap();

// Initialize storage manager
let manager = StorageManager::init("carol.sqlite", &cache_dir, None).await.unwrap();

// Copy some local file into storage
std::fs::write("myfile", "hello world").unwrap();

let file = manager
    .copy_local_file(
        FileSource::parse("./myfile"),
        StorePolicy::StoreForever,
        Some("myfile".to_string()),
        "myfile",
    )
    .await
    .unwrap();
```

## Limiting storage size

Carol itself doesn't have a mechanism to limit the total stored files size. However it does use
eviction policies when it faces "no space left" errors in attempt to free some space in the storage.

That means that you can limit your storage size with different ways:

- By using Linux [quota][3]
- By placing storage on a separate filesystem of desired size

Here is an example on how you can do the latter approach on Linux:

```shell
# Create filesystem image file of 1 GB
dd if=/dev/zero of=carol.img bs=4k count=250000

# Create an EXT4 filesystem on the image
mkfs.ext4 carol.img

# Mount the filesystem
mkdir /var/cache/carol
mount -o loop carol.img /var/cache/carol

# Now you can use "/var/cache/carol" path as a storage path for Carol manager
# When the storage will fill up, adding new files will evict old ones according to your specified policy
```

## Roadmap

- [x] Proper storage eviction
- [x] Advisory locks for stored files
- [ ] CLI interface for storage manager

[1]: <https://github.com/gevulotnetwork/carol/tree/main/carol-reqwest-middleware>
[2]: <https://crates.io/crates/reqwest>
[3]: <https://docs.kernel.org/filesystems/quota.html>
