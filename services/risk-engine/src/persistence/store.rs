//! Atomic, rotated snapshot files on local disk.

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use tracing::warn;

use super::{PersistenceError, Result, Snapshot, SnapshotView};

const SNAPSHOT_PREFIX: &str = "snapshot-";
const SNAPSHOT_SUFFIX: &str = ".bin";
const TMP_DIR: &str = "tmp";
/// Snapshots retained after rotation; the previous image is the corruption fallback.
const RETAINED_SNAPSHOTS: usize = 2;

/// Directory of canonical snapshot files with atomic replace and rotation.
///
/// File names lead with a store-assigned write ordinal zero-padded to 20 digits, so
/// lexicographic order equals write order; recency never depends on snapshot content
/// (the engine epoch restarts every process lifetime and the high-water mark can stall,
/// so neither is a recency signal). Writes go to a `tmp/` sibling, are fsynced, renamed
/// into place, and the directory itself is fsynced, so a torn write can never shadow a
/// good snapshot.
#[derive(Debug)]
pub struct SnapshotStore {
    directory: PathBuf,
}

impl SnapshotStore {
    /// Opens (creating if needed) a snapshot directory.
    ///
    /// Partial files a crash stranded in `tmp/` are deleted; they were never renamed into
    /// place, so nothing references them.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or its `tmp/` sibling cannot be created or
    /// stale staged files cannot be removed.
    pub fn open(directory: impl Into<PathBuf>) -> Result<Self> {
        let directory = directory.into();
        fs::create_dir_all(&directory).map_err(|error| io_error(&directory, &error))?;
        let tmp = directory.join(TMP_DIR);
        fs::create_dir_all(&tmp).map_err(|error| io_error(&tmp, &error))?;
        for entry in fs::read_dir(&tmp).map_err(|error| io_error(&tmp, &error))? {
            let staged = entry.map_err(|error| io_error(&tmp, &error))?.path();
            fs::remove_file(&staged).map_err(|error| io_error(&staged, &error))?;
        }
        Ok(Self { directory })
    }

    /// Atomically writes one snapshot and rotates old files.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding fails or any filesystem step cannot complete.
    pub fn write(&self, snapshot: &Snapshot) -> Result<PathBuf> {
        self.write_view(&snapshot.view())
    }

    /// Atomically streams one snapshot view to disk and rotates old files.
    ///
    /// The view encodes straight into the staged file with the digest computed on the fly,
    /// so writing never clones the fold or builds the full image in memory.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding fails or any filesystem step cannot complete.
    pub fn write_view(&self, view: &SnapshotView<'_>) -> Result<PathBuf> {
        let name = format!(
            "{SNAPSHOT_PREFIX}{:020}-{:020}-{:020}{SNAPSHOT_SUFFIX}",
            self.next_ordinal()?,
            view.version_high_water_mark,
            view.engine_epoch
        );
        let staged = self.directory.join(TMP_DIR).join(&name);
        let file = File::create(&staged).map_err(|error| io_error(&staged, &error))?;
        let mut writer = BufWriter::new(file);
        view.encode_to(&mut writer)?;
        let file = writer
            .into_inner()
            .map_err(|error| io_error(&staged, &error.into_error()))?;
        file.sync_all().map_err(|error| io_error(&staged, &error))?;
        drop(file);

        let target = self.directory.join(&name);
        // Store-assigned ordinals never repeat, so an existing target means the directory
        // holds a file this store did not write; refuse to destroy it.
        if target.exists() {
            return Err(PersistenceError::Io {
                path: target.display().to_string(),
                reason: "refusing to overwrite an existing snapshot file".to_owned(),
            });
        }
        fs::rename(&staged, &target).map_err(|error| io_error(&target, &error))?;
        self.sync_directory()?;
        self.rotate()?;
        Ok(target)
    }

    /// Loads the newest snapshot that passes digest verification.
    ///
    /// Returns `Ok(None)` when the directory holds no snapshot files at all — a first-ever
    /// boot. Corrupt candidates are skipped with a warning and the previous snapshot is
    /// tried instead.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError::NoValidSnapshot`] when snapshot files exist but every one
    /// fails verification, and an io error when the directory cannot be read.
    pub fn load_latest(&self) -> Result<Option<Snapshot>> {
        let candidates = self.candidates()?;
        if candidates.is_empty() {
            return Ok(None);
        }
        let mut rejected = 0;
        for path in &candidates {
            let loaded = fs::read(path)
                .map_err(|error| io_error(path, &error))
                .and_then(|bytes| Snapshot::decode(&bytes));
            match loaded {
                Ok(snapshot) => return Ok(Some(snapshot)),
                Err(error) => {
                    warn!(path = %path.display(), %error, "skipping unreadable snapshot");
                    rejected += 1;
                }
            }
        }
        Err(PersistenceError::NoValidSnapshot { rejected })
    }

    /// One past the highest write ordinal among the retained snapshot files.
    fn next_ordinal(&self) -> Result<u64> {
        let newest = self
            .candidates()?
            .first()
            .and_then(|path| path.file_name()?.to_str().and_then(parse_ordinal))
            .unwrap_or(0);
        newest
            .checked_add(1)
            .ok_or(PersistenceError::OrdinalSpaceExhausted)
    }

    /// Store-written snapshot file paths, newest first.
    ///
    /// Only names matching the full store-written shape participate: a foreign file whose
    /// name merely resembles a snapshot must never influence ordinal assignment, recovery
    /// order, or rotation (rotation deletes, and a garbage name that sorted as newest
    /// would both reset ordinals and shadow real snapshots).
    fn candidates(&self) -> Result<Vec<PathBuf>> {
        let entries =
            fs::read_dir(&self.directory).map_err(|error| io_error(&self.directory, &error))?;
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| io_error(&self.directory, &error))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if parse_ordinal(name).is_some() {
                names.push(name.to_owned());
            } else if name.starts_with(SNAPSHOT_PREFIX) && name.ends_with(SNAPSHOT_SUFFIX) {
                warn!(name, "ignoring foreign file with a snapshot-like name");
            }
        }
        names.sort_unstable_by(|left, right| right.cmp(left));
        Ok(names
            .into_iter()
            .map(|name| self.directory.join(name))
            .collect())
    }

    fn rotate(&self) -> Result<()> {
        for stale in self.candidates()?.into_iter().skip(RETAINED_SNAPSHOTS) {
            fs::remove_file(&stale).map_err(|error| io_error(&stale, &error))?;
        }
        Ok(())
    }

    fn sync_directory(&self) -> Result<()> {
        File::open(&self.directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error(&self.directory, &error))
    }
}

/// The write ordinal of one store-written snapshot file name.
///
/// Returns `None` unless the name is exactly
/// `snapshot-<ordinal>-<high-water-mark>-<epoch>.bin` with all three numbers zero-padded
/// to 20 digits — the only shape this store ever writes.
fn parse_ordinal(name: &str) -> Option<u64> {
    let numbers = name
        .strip_prefix(SNAPSHOT_PREFIX)?
        .strip_suffix(SNAPSHOT_SUFFIX)?;
    let mut parts = numbers.split('-');
    let ordinal = parts.next()?;
    let high_water_mark = parts.next()?;
    let epoch = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    for part in [ordinal, high_water_mark, epoch] {
        if part.len() != 20 || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    ordinal.parse().ok()
}

fn io_error(path: &Path, error: &std::io::Error) -> PersistenceError {
    PersistenceError::Io {
        path: path.display().to_string(),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn test_directory(name: &str) -> PathBuf {
        let directory = std::env::temp_dir()
            .join("risk-engine-persistence-tests")
            .join(format!("{}-{name}", std::process::id()));
        if directory.exists() {
            fs::remove_dir_all(&directory).expect("clean test directory");
        }
        directory
    }

    fn snapshot(high_water_mark: u64, epoch: u64) -> Snapshot {
        Snapshot {
            engine_epoch: epoch,
            cursors: BTreeMap::from([("prices".to_owned(), epoch)]),
            version_high_water_mark: high_water_mark,
            generation_version: high_water_mark,
            account_subjects: BTreeMap::new(),
            prices: BTreeMap::new(),
            accounts: BTreeMap::new(),
            removed: BTreeMap::new(),
            ledger: BTreeMap::new(),
        }
    }

    #[test]
    fn empty_directory_loads_none() {
        let store = SnapshotStore::open(test_directory("empty")).expect("open");
        assert!(store.load_latest().expect("load").is_none());
    }

    #[test]
    fn newest_snapshot_wins_and_rotation_keeps_two() {
        let directory = test_directory("rotation");
        let store = SnapshotStore::open(&directory).expect("open");
        for epoch in 1..=4 {
            store.write(&snapshot(epoch, epoch * 10)).expect("write");
        }

        let loaded = store.load_latest().expect("load").expect("snapshot");
        assert_eq!(loaded.version_high_water_mark, 4);

        let retained = fs::read_dir(&directory)
            .expect("read dir")
            .filter_map(|entry| {
                let name = entry.expect("entry").file_name();
                let name = name.to_str().expect("utf-8").to_owned();
                name.starts_with(SNAPSHOT_PREFIX).then_some(name)
            })
            .count();
        assert_eq!(retained, RETAINED_SNAPSHOTS);
    }

    #[test]
    fn recency_follows_write_order_not_snapshot_content() {
        let directory = test_directory("write-order");
        let store = SnapshotStore::open(&directory).expect("open");

        // A pre-crash lifetime leaves a file with a high engine epoch; the post-restart
        // lifetime restarts its epoch near zero without advancing the high-water mark.
        // The later write must still be the recovery candidate and must survive rotation.
        store.write(&snapshot(5, 1000)).expect("pre-crash");
        store.write(&snapshot(5, 10)).expect("post-restart");
        store.write(&snapshot(5, 11)).expect("post-restart again");

        let loaded = store.load_latest().expect("load").expect("snapshot");
        assert_eq!(loaded.engine_epoch, 11);

        // Identical name inputs never collide: every write produced a distinct file, and
        // rotation kept the two newest by write order.
        let mut retained: Vec<String> = fs::read_dir(&directory)
            .expect("read dir")
            .filter_map(|entry| {
                let name = entry.expect("entry").file_name();
                let name = name.to_str().expect("utf-8").to_owned();
                name.starts_with(SNAPSHOT_PREFIX).then_some(name)
            })
            .collect();
        retained.sort_unstable();
        assert_eq!(retained.len(), RETAINED_SNAPSHOTS);
        let epochs: Vec<u64> = retained
            .iter()
            .map(|name| {
                let snapshot = Snapshot::decode(&fs::read(directory.join(name)).expect("read"))
                    .expect("decode");
                snapshot.engine_epoch
            })
            .collect();
        assert_eq!(epochs, vec![10, 11]);

        // A store reopened after the crash continues the ordinal past the survivors.
        let reopened = SnapshotStore::open(&directory).expect("reopen");
        reopened.write(&snapshot(5, 12)).expect("write");
        let loaded = reopened.load_latest().expect("load").expect("snapshot");
        assert_eq!(loaded.engine_epoch, 12);
    }

    #[test]
    fn write_view_streams_byte_identical_output() {
        let store = SnapshotStore::open(test_directory("streamed")).expect("open");
        let image = snapshot(3, 7);
        let path = store.write_view(&image.view()).expect("write view");

        let streamed = fs::read(&path).expect("read streamed");
        assert_eq!(streamed, image.encode().expect("encode"));
        let loaded = store.load_latest().expect("load").expect("snapshot");
        assert_eq!(loaded, image);
    }

    #[test]
    fn foreign_snapshot_named_files_are_ignored_and_never_deleted() {
        let directory = test_directory("foreign-names");
        let store = SnapshotStore::open(&directory).expect("open");
        store.write(&snapshot(1, 10)).expect("first");

        // A foreign file whose name sorts after every store-written name must not reset
        // ordinal assignment, must not shadow real snapshots, and must survive rotation.
        let foreign = directory.join("snapshot-zzzzz.bin");
        fs::write(&foreign, b"not a snapshot").expect("plant foreign file");

        for epoch in 2..=4 {
            store.write(&snapshot(epoch, epoch * 10)).expect("write");
        }
        let loaded = store.load_latest().expect("load").expect("snapshot");
        assert_eq!(loaded.version_high_water_mark, 4);
        assert!(foreign.exists(), "rotation must not delete foreign files");
    }

    #[test]
    fn open_removes_stale_staged_files() {
        let directory = test_directory("stale-staged");
        drop(SnapshotStore::open(&directory).expect("open"));
        let stranded = directory.join(TMP_DIR).join("snapshot-partial.bin");
        fs::write(&stranded, b"torn write").expect("strand file");

        drop(SnapshotStore::open(&directory).expect("reopen"));
        assert!(!stranded.exists());
    }

    #[test]
    fn corrupt_newest_falls_back_to_previous() {
        let directory = test_directory("fallback");
        let store = SnapshotStore::open(&directory).expect("open");
        store.write(&snapshot(1, 10)).expect("first");
        let newest = store.write(&snapshot(2, 20)).expect("second");

        let mut bytes = fs::read(&newest).expect("read newest");
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xff;
        fs::write(&newest, &bytes).expect("corrupt newest");

        let loaded = store.load_latest().expect("load").expect("snapshot");
        assert_eq!(loaded.version_high_water_mark, 1);

        // Truncation is also caught by the trailer digest.
        fs::write(&newest, &bytes[..middle]).expect("truncate newest");
        let loaded = store.load_latest().expect("load").expect("snapshot");
        assert_eq!(loaded.version_high_water_mark, 1);
    }

    #[test]
    fn all_snapshots_corrupt_is_an_error_not_a_fresh_boot() {
        let directory = test_directory("all-corrupt");
        let store = SnapshotStore::open(&directory).expect("open");
        let first = store.write(&snapshot(1, 10)).expect("first");
        let second = store.write(&snapshot(2, 20)).expect("second");
        for path in [&first, &second] {
            fs::write(path, b"garbage").expect("corrupt");
        }

        assert!(matches!(
            store.load_latest(),
            Err(PersistenceError::NoValidSnapshot { rejected: 2 })
        ));
    }
}
