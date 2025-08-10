/// Format file size in human-readable format, using 1024 base and units B,K,M,G,T.
pub fn format_size(size: u64) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
    const THRESHOLD: u64 = 1024;
    if size < THRESHOLD {
        return format!("{size}B");
    }
    let mut size_f = size as f64;
    let mut unit_index = 0;
    while size_f >= THRESHOLD as f64 && unit_index < UNITS.len() - 1 {
        size_f /= THRESHOLD as f64;
        unit_index += 1;
    }
    format!("{size_f:.1}{}", UNITS[unit_index])
}
