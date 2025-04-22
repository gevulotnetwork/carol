use std::fmt;

use serde::{Deserialize, Serialize};

/// Storage eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum EvictionPolicy {
    /// Least Recently Used.
    ///
    /// Files with earliest "last used" timestamps will be removed.
    #[default]
    Lru,

    /// First-in-first-out.
    ///
    /// Files with earliest "created" timestamps will be removed.
    Fifo,

    /// Random selection.
    ///
    /// Randomly picked file will be removed.
    Random,
}

impl fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Lru => "LRU",
            Self::Fifo => "FIFO",
            Self::Random => "Random",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
/// Storage configuration.
pub struct StorageConfig {
    /// Eviction policy of the storage.
    ///
    /// The size of the storage is always considered unlimited and may take all available disk
    /// space. It is recommended for users to manually restrict the size of the storage, e.g. by
    /// placing it on a separate filesystem.
    ///
    /// When "no space left" error occures, storage manager will invoke this policy in attemp to
    /// free space.
    ///
    /// Default eviction policy is [`EvictionPolicy::Lru`].
    pub eviction_policy: EvictionPolicy,
}
