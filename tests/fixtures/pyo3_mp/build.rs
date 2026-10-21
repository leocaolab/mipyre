// A cargo-built cdylib that is a Python extension must leave the interpreter's symbols to the loader. Maturin
// passes this on macOS; a plain `cargo build` (which the isolate tests run) does not.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-undefined,dynamic_lookup");
    }
}
