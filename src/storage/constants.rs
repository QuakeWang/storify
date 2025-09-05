// Buffer related constants
pub const DEFAULT_BUFFER_SIZE: usize = 8192;
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

// Progress related constants
// Controls how often progress is printed (in multiples of buffer size)
pub const PROGRESS_UPDATE_INTERVAL: u64 = 100;

// Filesystem default
pub const DEFAULT_FS_ROOT: &str = "./storage";
pub const CAT_CONFIRM_SIZE_THRESHOLD: u64 = 10 * 1024 * 1024;
