use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

fn main() -> io::Result<()> {
    // Generate manpage using clap_mangen
    let cmd = datui::Args::command();
    let man = clap_mangen::Man::new(cmd);
    let mut buffer: Vec<u8> = Default::default();
    man.render(&mut buffer)?;

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    // Always write to OUT_DIR
    let dest_path = out_dir.join("datui.1");
    fs::write(&dest_path, &buffer)?;

    // In release mode, also write to target/release/ for easy access in CI
    // OUT_DIR is typically target/release/build/xxx/out
    // Going up 3 levels gets us to target/release/
    if env::var("PROFILE").unwrap_or_default() == "release" {
        if let Some(release_dir) = out_dir.ancestors().nth(3) {
            let release_manpage = release_dir.join("datui.1");
            fs::write(&release_manpage, &buffer)?;
        }
    }

    Ok(())
}
