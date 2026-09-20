//! The filesystem content a case expects, built independently of the daemon.
//!
//! A case writes a [`Tree`] through a mount, mutates it with ordinary file operations,
//! and then requires a recovered disk to hold exactly the tree it kept in mind. The
//! expectation is therefore the case's own model of what it wrote, rather than
//! anything derived from the daemon: nothing here replays a journal or applies a
//! chunk, so a recovery which agrees with a tree agrees with the client that wrote it.

use disk_daemon::BLOCK_SIZE;

/// Bytes of a disk block, which is the granularity of everything the daemon encodes.
pub const BLOCK: usize = BLOCK_SIZE as usize;

/// Files a case wrote through a mount, and the content each must read back.
///
/// Paths are relative to the directory the tree was written into, so a case which
/// writes one tree into `<mount>/data` leaves the rest of the filesystem — including
/// `lost+found` and any scratch a case makes — outside of what the tree claims.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tree {
    files: std::collections::BTreeMap<String, Vec<u8>>,
}

impl Tree {
    pub fn empty() -> Self {
        Self::default()
    }

    /// The tree of `generation`, whose content exercises the chunk encoding: a file
    /// shorter than a block, one of exactly one block, one which is entirely zeroes,
    /// one large enough to span records, and one a directory down.
    ///
    /// Every generation differs from every other in every file, so a disk which
    /// recovered the wrong generation is not merely missing a file.
    pub fn generation(generation: u8) -> Self {
        Self::empty()
            .with("small", pattern(generation, 11))
            .with("one-block", pattern(generation.wrapping_add(1), BLOCK))
            .with("all-zeroes", vec![0; 3 * BLOCK])
            .with(
                "large",
                pattern(generation.wrapping_add(2), 3 * (1 << 20) + 17),
            )
            .with(
                "nested/deep",
                pattern(generation.wrapping_add(3), BLOCK + 7),
            )
    }

    pub fn with(mut self, path: &str, content: Vec<u8>) -> Self {
        _ = self.files.insert(path.to_string(), content);
        self
    }

    pub fn without(mut self, path: &str) -> Self {
        self.files
            .remove(path)
            .unwrap_or_else(|| panic!("{path} is not in this tree"));
        self
    }

    pub fn renamed(mut self, from: &str, to: &str) -> Self {
        let content = self
            .files
            .remove(from)
            .unwrap_or_else(|| panic!("{from} is not in this tree"));

        self.with(to, content)
    }

    pub fn truncated(mut self, path: &str, len: usize) -> Self {
        let content = self
            .files
            .get_mut(path)
            .unwrap_or_else(|| panic!("{path} is not in this tree"));

        content.resize(len, 0);
        self
    }

    pub fn content(&self, path: &str) -> &[u8] {
        self.files
            .get(path)
            .unwrap_or_else(|| panic!("{path} is not in this tree"))
    }

    /// Write every file of this tree into `dir`, replacing whatever is there under the
    /// same name. Files `dir` holds which this tree does not are left alone, so a case
    /// deletes and renames for itself.
    pub fn write(&self, dir: &std::path::Path) {
        for (path, content) in &self.files {
            let path = dir.join(path);

            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .unwrap_or_else(|err| panic!("creating {parent:?}: {err}"));
            }
            std::fs::write(&path, content).unwrap_or_else(|err| panic!("writing {path:?}: {err}"));
        }
    }

    /// Require `dir` to hold exactly this tree.
    pub fn assert_matches(&self, dir: &std::path::Path) {
        if let Some(diff) = self.diff(dir) {
            panic!("{diff}");
        }
    }

    /// How `dir` differs from this tree, or `None` when it does not.
    ///
    /// This helper compares regular files. Cases covering empty directories assert
    /// their presence and absence separately.
    pub fn diff(&self, dir: &std::path::Path) -> Option<String> {
        let found = read_tree(dir);
        let mut differences = Vec::new();

        for (path, expect) in &self.files {
            match found.get(path) {
                None => differences.push(format!("{path} is missing")),
                Some(content) if content == expect => (),
                Some(content) => differences.push(format!(
                    "{path} holds {} bytes rather than {}, first differing at {:?}",
                    content.len(),
                    expect.len(),
                    first_difference(content, expect),
                )),
            }
        }
        for path in found.keys().filter(|path| !self.files.contains_key(*path)) {
            differences.push(format!("{path} is not part of this tree"));
        }

        if differences.is_empty() {
            return None;
        }
        Some(format!(
            "{dir:?} differs from what was written:\n  {}",
            differences.join("\n  "),
        ))
    }
}

/// Every file under `dir`, keyed by its path relative to `dir`.
fn read_tree(dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut found = std::collections::BTreeMap::new();
    let mut pending = vec![(String::new(), dir.to_path_buf())];

    while let Some((prefix, path)) = pending.pop() {
        let listing = match std::fs::read_dir(&path) {
            Ok(listing) => listing,
            // A tree nothing wrote is a tree of no files, which the diff reports as
            // every file missing rather than as an unreadable directory.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => panic!("reading {path:?}: {err}"),
        };

        for entry in listing {
            let entry = entry.unwrap_or_else(|err| panic!("reading {path:?}: {err}"));
            let name = entry.file_name().to_string_lossy().into_owned();

            let relative = match prefix.is_empty() {
                true => name,
                false => format!("{prefix}/{name}"),
            };
            let kind = entry.file_type().expect("the type of a directory entry");

            if kind.is_dir() {
                pending.push((relative, entry.path()));
                continue;
            }
            assert!(
                kind.is_file(),
                "expected a regular file at {:?}",
                entry.path()
            );
            let content = std::fs::read(entry.path())
                .unwrap_or_else(|err| panic!("reading {:?}: {err}", entry.path()));

            _ = found.insert(relative, content);
        }
    }
    found
}

/// Offset at which two contents first differ, which names where a recovery went wrong
/// instead of dumping either file.
fn first_difference(left: &[u8], right: &[u8]) -> Option<usize> {
    (0..std::cmp::min(left.len(), right.len())).find(|&index| left[index] != right[index])
}

/// Content in which every third block is entirely zero. Both trailing-zero trimming
/// and empty-data chunks then occur.
pub fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            if (index / BLOCK) % 3 == 2 {
                0
            } else {
                seed.wrapping_add((index % 251) as u8)
            }
        })
        .collect()
}
