use std::collections::hash_map::DefaultHasher;
use std::ffi::OsStr;
use std::fs;
use std::hash::Hasher;
use std::io;
use std::path::{Path, PathBuf};

fn main() -> io::Result<()> {
    // These Python files are embedded into the binary via `include_str!` in src/.
    // Cargo does not track files pulled in by `include_str!`, so without these
    // lines, editing them would NOT trigger a rebuild (the stale text stays baked
    // in). Re-run the build whenever they change.
    println!("cargo:rerun-if-changed=../../python/mipyre/_bootstrap.py");
    println!("cargo:rerun-if-changed=../../python/mipyre/_async_engine.py");

    let id = format!("{}+{:016x}", env!("CARGO_PKG_VERSION"), source_tree_hash()?);
    println!("cargo:rustc-env=MIPYRE_BUILD_ID={id}");
    Ok(())
}

/// Hashes every source file of the Rust workspace and of the `mipyre` package, so that two
/// builds of different sources carry different ids even at the same package version.
///
/// Each file is a rerun trigger of its own: a directory trigger would also fire on the
/// extension that `maturin develop` writes into `python/mipyre`, rebuilding on every build.
fn source_tree_hash() -> io::Result<u64> {
    let root = Path::new("../..");
    let mut files = vec![root.join("Cargo.toml"), root.join("Cargo.lock")];
    collect_sources(&root.join("crates"), &mut files)?;
    collect_sources(&root.join("python/mipyre"), &mut files)?;
    files.sort();

    let mut hasher = DefaultHasher::new();
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
        let bytes = fs::read(file).map_err(|e| with_path(e, file))?;
        hasher.write(file.as_os_str().as_encoded_bytes());
        hasher.write_usize(bytes.len());
        hasher.write(&bytes);
    }
    Ok(hasher.finish())
}

fn collect_sources(dir: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    let skipped = ["target", "__pycache__"].map(OsStr::new);
    for entry in fs::read_dir(dir).map_err(|e| with_path(e, dir))? {
        let path = entry.map_err(|e| with_path(e, dir))?.path();
        if path.is_dir() {
            if !path.file_name().is_some_and(|n| skipped.contains(&n)) {
                collect_sources(&path, files)?;
            }
        } else if matches!(path.extension().and_then(OsStr::to_str), Some("rs" | "py" | "toml")) {
            files.push(path);
        }
    }
    Ok(())
}

fn with_path(e: io::Error, path: &Path) -> io::Error {
    io::Error::new(e.kind(), format!("{}: {e}", path.display()))
}
