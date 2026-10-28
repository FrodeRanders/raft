use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
};

/// Publish one complete record, then make the directory entry durable. Callers
/// serialize mutations so an older state cannot overwrite a newer state.
pub(crate) fn atomic_replace(path: &Path, data: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let temp = path.with_file_name(format!(
        "{}.tmp",
        path.file_name().unwrap().to_string_lossy()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp, path)?;
    File::open(parent)?.sync_all()
}

pub(crate) fn persist(path: &Path, data: &[u8]) {
    if let Err(error) = atomic_replace(path, data) {
        // Store traits are infallible. Unwinding only a Tokio task would leave
        // the shared node usable with unpersisted state, so terminate the process.
        eprintln!("Raft persistence failed for {}: {error}", path.display());
        std::process::abort();
    }
}
