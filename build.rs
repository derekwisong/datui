use clap::CommandFactory;
use clap_mangen::Man;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

/// Embed a Windows VERSIONINFO resource in `datui.exe`.
///
/// Without this the binary ships with an empty resource directory: no product
/// name, company, or description in the file's Properties dialog. Reputation
/// based scanners treat anonymous binaries as lower trust, and users have no
/// way to confirm what they downloaded.
#[cfg(windows)]
fn embed_version_resource() -> io::Result<()> {
    // FileVersion and ProductVersion are derived from CARGO_PKG_VERSION.
    let mut res = winresource::WindowsResource::new();
    res.set("ProductName", "datui")
        .set("FileDescription", env!("CARGO_PKG_DESCRIPTION"))
        .set("CompanyName", "Derek Wisong")
        .set("LegalCopyright", "Copyright (c) 2026 Derek Wisong")
        .set("InternalName", "datui")
        .set("OriginalFilename", "datui.exe");
    res.compile()
}

#[cfg(not(windows))]
fn embed_version_resource() -> io::Result<()> {
    Ok(())
}

fn main() -> io::Result<()> {
    let cmd = datui_cli::Args::command();
    let man = Man::new(cmd);
    let mut buffer: Vec<u8> = Default::default();
    man.render(&mut buffer)?;

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let dest_path = out_dir.join("datui.1");
    fs::write(&dest_path, &buffer)?;

    if env::var("PROFILE").unwrap_or_default() == "release" {
        if let Some(release_dir) = out_dir.ancestors().nth(3) {
            let release_manpage = release_dir.join("datui.1");
            fs::write(&release_manpage, &buffer)?;
        }
    }

    embed_version_resource()?;

    Ok(())
}
