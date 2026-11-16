/// Expected regular files and their contents, by path relative to the directory the
/// tree is written into. Cases use `<mount>/data`, which keeps `lost+found` and
/// scratch files out of comparisons.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tree {
    files: std::collections::BTreeMap<String, Vec<u8>>,
}

impl Tree {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Short, one-block, all-zero, multi-record, and nested files. Content differs by
    /// generation, so a recovery of stale content is visible.
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

    /// Write this tree's files into `dir`, overwriting same-named files. Other files
    /// are left alone.
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

    /// Require `dir` to hold exactly this tree's files. Empty directories are ignored.
    pub fn assert_matches(&self, dir: &std::path::Path) {
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

        assert!(
            differences.is_empty(),
            "{dir:?} differs from what was written:\n  {}",
            differences.join("\n  "),
        );
    }
}

/// Bytes of a disk block.
pub const BLOCK: usize = disk_daemon::BLOCK_SIZE as usize;

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

/// Every file under `dir`, keyed by its path relative to `dir`.
fn read_tree(dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut found = std::collections::BTreeMap::new();
    let mut pending = vec![(String::new(), dir.to_path_buf())];

    while let Some((prefix, path)) = pending.pop() {
        let listing = match std::fs::read_dir(&path) {
            Ok(listing) => listing,
            // A missing directory is an empty tree, so the diff lists each file missing.
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

/// Offset at which two contents first differ.
fn first_difference(left: &[u8], right: &[u8]) -> Option<usize> {
    (0..std::cmp::min(left.len(), right.len())).find(|&index| left[index] != right[index])
}
