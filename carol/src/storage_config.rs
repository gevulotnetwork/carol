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

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
/// Storage configuration.
pub struct StorageConfig {
    /// Eviction policy of the storage.
    ///
    /// The size of the storage is always considered unlimited and may take all available disk
    /// space. It is recommended for users to manually restrict the size of the storage, e.g. by
    /// placing it on a separate filesystem.
    ///
    /// When "no space left" error occurs, storage manager will invoke this policy in attempt to
    /// free space.
    ///
    /// Default eviction policy is [`EvictionPolicy::Lru`].
    pub eviction_policy: EvictionPolicy,
}
