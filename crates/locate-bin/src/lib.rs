use anyhow::Context;
use std::path::{Path, PathBuf};

/// Locate the absolute path of `binary`.
///
/// The binary is first sought alongside the currently-running program,
/// and otherwise is resolved from the `$PATH`.
pub fn locate(binary: &str) -> anyhow::Result<PathBuf> {
    // Look for the binary alongside this program.
    let this_program = std::env::args().next().unwrap();

    locate_inner(Path::new(&this_program), binary, |binary| {
        Ok(which::which(binary)?)
    })
}

// `locate_inner` factors the resolution policy out of the ambient process
// environment (the running program's path and the `$PATH` lookup) so that it
// can be exercised deterministically by unit tests.
fn locate_inner(
    this_program: &Path,
    binary: &str,
    on_path: impl FnOnce(&str) -> anyhow::Result<PathBuf>,
) -> anyhow::Result<PathBuf> {
    tracing::debug!(this_program = %this_program.display(), "attempting to find '{binary}'");
    let mut bin = this_program.parent().unwrap().join(binary);

    // If `bin` doesn't resolve to a file, then fall back to the $PATH.
    // Note a sibling *directory* named like `binary` must not shadow the lookup.
    if !bin.is_file() {
        bin = on_path(binary).with_context(|| {
            format!(
                "failed to locate '{binary}' alongside '{}' or on the $PATH",
                this_program.display()
            )
        })?;
    } else {
        bin = bin.canonicalize().unwrap();
    }
    tracing::debug!(executable = %bin.display(), "resolved {binary}");
    Ok(bin)
}

/// Locate the absolute path of `binary` as `locate` does, but pass over any
/// candidate which is a dynamically linked ELF executable.
///
/// Use this for a binary which runs within a foreign root filesystem, such as
/// `flow-connector-init` within a connector's container or VMM guest, where a
/// dynamic loader and libc are whatever that image happens to hold.
pub fn locate_static(binary: &str) -> anyhow::Result<PathBuf> {
    let this_program = std::env::args().next().unwrap();

    // `which_all` errs when nothing is found, which is reported below.
    locate_static_inner(Path::new(&this_program), binary, |binary| {
        which::which_all(binary)
            .map(Iterator::collect)
            .unwrap_or_default()
    })
}

fn locate_static_inner(
    this_program: &Path,
    binary: &str,
    on_path: impl FnOnce(&str) -> Vec<PathBuf>,
) -> anyhow::Result<PathBuf> {
    tracing::debug!(this_program = %this_program.display(), "attempting to find a static '{binary}'");
    let sibling = this_program.parent().unwrap().join(binary);
    let sibling = sibling.is_file().then(|| sibling.canonicalize().unwrap());

    // As with `locate`, the $PATH is consulted only if the sibling won't do.
    let candidates = sibling
        .into_iter()
        .chain(std::iter::once_with(|| on_path(binary)).flatten());

    let mut passed_over = Vec::new();
    for candidate in candidates {
        if !needs_interpreter(&candidate) {
            tracing::debug!(executable = %candidate.display(), "resolved {binary}");
            return Ok(candidate);
        }
        tracing::debug!(candidate = %candidate.display(), "passing over dynamically linked {binary}");
        passed_over.push(candidate);
    }

    if passed_over.is_empty() {
        anyhow::bail!(
            "failed to locate '{binary}' alongside '{}' or on the $PATH",
            this_program.display()
        );
    }
    anyhow::bail!(
        "found only dynamically linked '{binary}' alongside '{}' or on the $PATH: {passed_over:?}",
        this_program.display()
    )
}

// Whether `path` is an ELF executable naming a program interpreter (PT_INTERP):
// the dynamic loader, which the kernel opens from the root filesystem the
// program runs within. Static and static-pie builds name none. Only 64-bit
// little-endian ELF is recognized, as every Flow target is. Anything else,
// including a script or an unreadable file, is not identified as dynamically
// linked, so `locate_static` accepts it as `locate` would.
fn needs_interpreter(path: &Path) -> bool {
    use std::io::{Read, Seek};
    const PT_INTERP: u32 = 3;
    // sizeof(Elf64_Phdr), which also bounds the read below.
    const PHENTSIZE: usize = 56;

    let read = || -> std::io::Result<bool> {
        let mut file = std::fs::File::open(path)?;
        let mut header = [0u8; 64];
        file.read_exact(&mut header)?;

        // Magic, ELFCLASS64, ELFDATA2LSB.
        if header[..6] != *b"\x7fELF\x02\x01" {
            return Ok(false);
        }
        let phoff = u64::from_le_bytes(header[0x20..0x28].try_into().unwrap());
        let phentsize = u16::from_le_bytes(header[0x36..0x38].try_into().unwrap()) as usize;
        let phnum = u16::from_le_bytes(header[0x38..0x3a].try_into().unwrap()) as usize;
        if phentsize != PHENTSIZE {
            return Ok(false);
        }

        let mut headers = vec![0u8; PHENTSIZE * phnum];
        file.seek(std::io::SeekFrom::Start(phoff))?;
        file.read_exact(&mut headers)?;

        Ok(headers
            .chunks_exact(PHENTSIZE)
            .any(|header| header[..4] == PT_INTERP.to_le_bytes()))
    };
    read().unwrap_or(false)
}

#[cfg(test)]
mod test {
    use super::{locate_inner, locate_static_inner};
    use std::path::{Path, PathBuf};

    // Stands in for an empty `$PATH`.
    fn not_on_path(binary: &str) -> anyhow::Result<PathBuf> {
        anyhow::bail!("'{binary}' is not on the $PATH")
    }

    #[test]
    fn resolves_a_sibling_file() {
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl-go");
        let sibling = dir.path().join("sops");
        std::fs::write(&sibling, b"#!/bin/sh\ntrue\n").unwrap();

        // The sibling file is resolved (and canonicalized) without ever
        // consulting the $PATH.
        let located = locate_inner(&this_program, "sops", not_on_path).unwrap();
        assert_eq!(located, sibling.canonicalize().unwrap());
    }

    #[test]
    fn sibling_directory_does_not_shadow_the_path() {
        // Regression: a sibling *directory* sharing the binary's name (e.g. a
        // `go/` source tree next to the program) must not be returned in place
        // of the actual `go` executable found on the $PATH.
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl-go");
        std::fs::create_dir(dir.path().join("go")).unwrap();

        let from_path = PathBuf::from("/usr/local/bin/go");
        let located = locate_inner(&this_program, "go", |_| Ok(from_path.clone())).unwrap();
        assert_eq!(located, from_path);
    }

    #[test]
    fn missing_everywhere_is_a_contextual_error() {
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl-go");

        let err = locate_inner(&this_program, "nonesuch", not_on_path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failed to locate 'nonesuch'") && msg.contains("$PATH"),
            "unexpected error: {msg}",
        );
    }

    const PT_LOAD: u32 = 1;
    const PT_DYNAMIC: u32 = 2;
    const PT_INTERP: u32 = 3;
    const PT_PHDR: u32 = 6;

    // A 64-bit little-endian ELF header and its program headers, of which only
    // the types matter here.
    fn write_elf(path: &Path, types: &[u32]) {
        let mut elf = vec![0u8; 64];
        elf[..6].copy_from_slice(b"\x7fELF\x02\x01");
        elf[0x20..0x28].copy_from_slice(&64u64.to_le_bytes()); // e_phoff
        elf[0x36..0x38].copy_from_slice(&56u16.to_le_bytes()); // e_phentsize
        elf[0x38..0x3a].copy_from_slice(&(types.len() as u16).to_le_bytes()); // e_phnum
        for ty in types {
            let mut header = [0u8; 56];
            header[..4].copy_from_slice(&ty.to_le_bytes());
            elf.extend_from_slice(&header);
        }
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, elf).unwrap();
    }

    fn write_glibc(path: &Path) {
        write_elf(path, &[PT_PHDR, PT_INTERP, PT_LOAD, PT_DYNAMIC]);
    }

    fn write_static_pie(path: &Path) {
        write_elf(path, &[PT_LOAD, PT_DYNAMIC]);
    }

    fn never_on_path(binary: &str) -> Vec<PathBuf> {
        panic!("'{binary}' was sought on the $PATH")
    }

    #[test]
    fn a_dynamically_linked_sibling_and_path_entry_are_passed_over() {
        // Regression: `cargo run -p flowctl` under mise, with a glibc build of
        // the workspace beside flowctl and first on the $PATH, and the musl
        // build later on it. Connector images with an older glibc than the
        // host's could not load the glibc build.
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("debug/flowctl");
        let glibc = dir.path().join("debug/flow-connector-init");
        let musl = dir
            .path()
            .join("x86_64-unknown-linux-musl/debug/flow-connector-init");
        write_glibc(&glibc);
        write_static_pie(&musl);

        let located = locate_static_inner(&this_program, "flow-connector-init", |_| {
            vec![glibc.clone(), musl.clone()]
        })
        .unwrap();
        assert_eq!(located, musl);
    }

    #[test]
    fn a_static_sibling_is_resolved_without_the_path() {
        // As in the reactor image, where the static build is installed beside
        // flowctl-go.
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl-go");
        let sibling = dir.path().join("flow-connector-init");
        write_static_pie(&sibling);

        let located = locate_static_inner(&this_program, "flow-connector-init", never_on_path);
        assert_eq!(located.unwrap(), sibling.canonicalize().unwrap());
    }

    #[test]
    fn a_candidate_which_is_not_elf_is_accepted() {
        // Only what is identified as dynamically linked is passed over, so a
        // stand-in script resolves as it does through `locate`.
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl");
        let script = dir.path().join("flow-connector-init");
        std::fs::write(&script, b"#!/bin/sh\nexit 1\n").unwrap();

        let located = locate_static_inner(&this_program, "flow-connector-init", never_on_path);
        assert_eq!(located.unwrap(), script.canonicalize().unwrap());
    }

    #[test]
    fn only_dynamically_linked_candidates_is_a_contextual_error() {
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl");
        let glibc = dir.path().join("flow-connector-init");
        write_glibc(&glibc);

        let err = locate_static_inner(&this_program, "flow-connector-init", |_| vec![])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("found only dynamically linked 'flow-connector-init'")
                && err.contains(glibc.to_str().unwrap()),
            "unexpected error: {err}",
        );

        let err = locate_static_inner(&this_program, "nonesuch", |_| vec![])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("failed to locate 'nonesuch'") && err.contains("$PATH"),
            "unexpected error: {err}",
        );
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn a_linked_glibc_executable_needs_an_interpreter() {
        // Checks the header parsing against a real linker's output, which this
        // test binary is.
        assert!(super::needs_interpreter(&std::env::current_exe().unwrap()));
    }
}
