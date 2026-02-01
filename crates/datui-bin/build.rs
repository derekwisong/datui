use clap::CommandFactory;
use clap_mangen::Man;
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

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

    Ok(())
}
