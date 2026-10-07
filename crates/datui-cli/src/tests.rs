use super::*;

/// Every example parses as a command line, as written: its flags exist and take
/// what it gives them. The runner checks that each does what it says.
#[test]
fn every_example_parses_as_written() {
    let examples = examples();
    assert!(examples.len() >= 4);
    for example in &examples {
        assert!(!example.description.is_empty(), "{example:?}");
        assert!(
            matches!(
                example.expect.as_deref(),
                None | Some("rows" | "screen" | "exit")
            ),
            "{example:?}"
        );
        // Every datui command in it: after a pipe or `&&`, behind `VAR=value`,
        // up to a redirection.
        let mut any = false;
        for part in example.command.split("&&").flat_map(|p| p.split(" | ")) {
            let mut words = shell_words(part);
            while words
                .first()
                .is_some_and(|w| w.contains('=') && !w.starts_with('-'))
            {
                words.remove(0);
            }
            if let Some(at) = words.iter().position(|w| w == ">") {
                words.truncate(at);
            }
            if words.first().map(String::as_str) != Some("datui") {
                continue;
            }
            any = true;
            if let Err(e) = Args::try_parse_from(&words) {
                panic!("{}: {e}", example.command);
            }
        }
        assert!(any, "no datui command: {example:?}");
    }
    assert!(examples_help().contains("| datui"), "the help shows a pipe");
}

/// Split a command line as a shell does, for the quoting the examples use.
fn shell_words(line: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote: Option<char> = None;
    let mut any = false;
    for c in line.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => word.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                any = true;
            }
            (None, c) if c.is_whitespace() => {
                if any || !word.is_empty() {
                    words.push(std::mem::take(&mut word));
                    any = false;
                }
            }
            (None, c) => word.push(c),
        }
    }
    if any || !word.is_empty() {
        words.push(word);
    }
    words
}

/// `-` is a path like any other to the parser: standard input, with the reading
/// flags beside it.
#[test]
fn a_dash_names_standard_input() {
    let args = Args::try_parse_from(["datui", "-", "--format", "jsonl"]).unwrap();
    assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
    assert_eq!(args.format, Some(FormatChoice::Builtin(FileFormat::Jsonl)));
    let args = Args::try_parse_from(["datui", "--delimiter", ";", "-"]).unwrap();
    assert_eq!(args.paths, vec![std::path::PathBuf::from("-")]);
    assert_eq!(args.delimiter, Some(b';'));
}

/// `--format` takes a built-in format, a spec's namespaced name, or a spec's file:
/// a path only with a `/` or a `.toml` ending.
#[test]
fn a_format_is_built_in_a_spec_name_or_a_spec_file() {
    let format = |value: &str| {
        Args::try_parse_from(["datui", "x", "--format", value])
            .map(|a| a.format.unwrap())
            .map_err(|e| e.to_string())
    };
    assert_eq!(
        format("acme.l2feed"),
        Ok(FormatChoice::Spec("acme.l2feed".into()))
    );
    assert_eq!(format("CSV"), Ok(FormatChoice::Builtin(FileFormat::Csv)));
    assert_eq!(format("./acme"), Ok(FormatChoice::File("./acme".into())));
    assert_eq!(
        format("acme.toml"),
        Ok(FormatChoice::File("acme.toml".into()))
    );
    assert_eq!(
        format("specs/acme.TOML"),
        Ok(FormatChoice::File("specs/acme.TOML".into()))
    );
    let refused = format("cvs").unwrap_err().to_string();
    assert!(
        refused.contains("a spec's name") && refused.contains("ending .toml"),
        "{refused}"
    );
    // No spec of that name ships, so the user-facing text names none.
    assert!(!refused.contains("acme"), "{refused}");
    let args = Args::try_parse_from(["datui", "x", "-F", "parquet"]).unwrap();
    assert_eq!(
        args.format,
        Some(FormatChoice::Builtin(FileFormat::Parquet))
    );
}

/// A flag whose value is optional takes it only after `=`, so the path after it
/// stays a path.
#[test]
fn an_optional_value_needs_equals() {
    let args = Args::try_parse_from(["datui", "--infer-types", "data.csv"]).unwrap();
    assert_eq!(args.infer_types, Some(InferTypes::All));
    assert_eq!(args.paths, vec![std::path::PathBuf::from("data.csv")]);
    let args = Args::try_parse_from(["datui", "--infer-types=off", "d.csv"]).unwrap();
    assert_eq!(args.infer_types, Some(InferTypes::Off));
    let args = Args::try_parse_from(["datui", "--infer-types=a, b,a", "d.csv"]).unwrap();
    assert_eq!(
        args.infer_types,
        Some(InferTypes::Columns(vec!["a".into(), "b".into()]))
    );
    for flag in [
        "--row-numbers",
        "--mouse",
        "--skip-initial-space",
        "--ignore-errors",
    ] {
        let args = Args::try_parse_from(["datui", flag, "data.csv"]).unwrap();
        assert_eq!(
            args.paths,
            vec![std::path::PathBuf::from("data.csv")],
            "{flag}"
        );
        let off = Args::try_parse_from(["datui", &format!("{flag}=false"), "d.csv"]).unwrap();
        assert_eq!(off.paths.len(), 1, "{flag}");
    }
    let args = Args::try_parse_from(["datui", "--mouse=false"]).unwrap();
    assert_eq!(args.mouse, Some(false));
    let args = Args::try_parse_from(["datui", "--row-numbers"]).unwrap();
    assert_eq!(args.row_numbers, Some(true));
}

#[test]
fn a_delimiter_is_written_as_people_write_it() {
    for (text, byte) in [
        (";", b';'),
        ("tab", b'\t'),
        ("\\t", b'\t'),
        ("0x1f", 0x1f),
        ("|", b'|'),
    ] {
        assert_eq!(parse_delimiter(text), Ok(byte), "{text}");
    }
    for refused in ["59", "ab", "é", "0xzz", "\""] {
        assert!(parse_delimiter(refused).is_err(), "{refused}");
    }
}

/// Every subcommand parses; any other first word is still a path.
#[test]
fn every_command_parses_and_paths_stay_paths() {
    let command = |argv: &[&str]| Args::try_parse_from(argv).unwrap().command;
    assert!(matches!(
        command(&["datui", "formats"]),
        Some(Command::Formats { action: None })
    ));
    let Some(Command::Formats {
        action: Some(FormatsAction::Check { spec, file }),
    }) = command(&["datui", "formats", "check", "a.b", "f.bin"])
    else {
        panic!("a check");
    };
    assert_eq!((spec.as_str(), file), ("a.b", Some("f.bin".into())));
    assert!(matches!(
        command(&["datui", "config", "init", "--force"]),
        Some(Command::Config {
            action: ConfigAction::Init { force: true }
        })
    ));
    assert!(matches!(
        command(&["datui", "config", "path"]),
        Some(Command::Config {
            action: ConfigAction::Path
        })
    ));
    assert!(matches!(
        command(&["datui", "config", "keys"]),
        Some(Command::Config {
            action: ConfigAction::Keys
        })
    ));
    assert!(matches!(
        command(&["datui", "theme", "list"]),
        Some(Command::Theme {
            action: ThemeAction::List
        })
    ));
    assert!(matches!(
        command(&["datui", "theme", "show", "night-market"]),
        Some(Command::Theme {
            action: ThemeAction::Show { name }
        }) if name == "night-market"
    ));
    assert!(matches!(
        command(&["datui", "cache", "clear"]),
        Some(Command::Cache {
            action: CacheAction::Clear { recents: false }
        })
    ));
    assert!(matches!(
        command(&["datui", "cache", "clear", "--recents"]),
        Some(Command::Cache {
            action: CacheAction::Clear { recents: true }
        })
    ));
    assert!(matches!(
        command(&["datui", "views", "list"]),
        Some(Command::Views {
            action: ViewsAction::List
        })
    ));
    assert!(matches!(
        command(&["datui", "views", "rm", "daily"]),
        Some(Command::Views {
            action: ViewsAction::Rm { name }
        }) if name == "daily"
    ));
    assert!(matches!(
        command(&["datui", "views", "clear"]),
        Some(Command::Views {
            action: ViewsAction::Clear
        })
    ));
    let args = Args::try_parse_from(["datui", "data.csv", "--format", "./s.toml"]).unwrap();
    assert_eq!(args.paths, vec![std::path::PathBuf::from("data.csv")]);
    assert!(args.command.is_none());
}

/// Every shell's script names the commands and the flags.
#[test]
fn completions_cover_commands_and_flags() {
    use clap::ValueEnum;
    for shell in clap_complete::Shell::value_variants() {
        let script = completions(*shell);
        assert!(!script.is_empty(), "{shell}");
        assert!(script.contains("config"), "{shell}: a subcommand");
        assert!(script.contains("infer-types"), "{shell}: a flag");
    }
    let args = Args::try_parse_from(["datui", "completions", "fish"]).unwrap();
    assert!(matches!(
        args.command,
        Some(Command::Completions {
            shell: clap_complete::Shell::Fish
        })
    ));
    assert!(Args::try_parse_from(["datui", "completions", "tcsh"]).is_err());
}

/// Removed flags are gone, not hidden: the config key or command replaces each.
#[test]
fn removed_flags_are_refused() {
    for flag in [
        "--generate-config",
        "--clear-cache",
        "--clear-recents",
        "--remove-templates",
        "--s3-endpoint-url=x",
        "--s3-region=x",
        "--workaround-pivot-date-index=true",
        "--debug",
        "--sheet=x",
        "--variant=x",
        "--spec=x",
        "--fix-dict=x",
        "--dbc=x",
        "--parse-dates",
        "--parse-strings",
        "--no-parse-strings",
        "--polars-streaming",
        "--pages-lookahead=3",
        "--row-start-index=0",
        "--column-colors",
        "--align-numeric-right",
        "--cloud-discover=all",
        "--normalize",
        "--single-spine-schema",
        "--decompress-in-memory",
        "--template=x",
        "--skip-tail-rows=1",
        "--comment-char=#",
        "--infer-schema-length=9",
        "--null-value=x",
        "--record-size=4",
    ] {
        assert!(Args::try_parse_from(["datui", flag]).is_err(), "{flag}");
    }
}

/// Python's keywords are the registry's: one name each, and each open option's
/// flag is a flag of `Args`.
#[test]
fn python_keywords_are_unique_and_open_options_are_flags() {
    let cmd = Args::command();
    let mut names: Vec<&str> = settings::OPEN.iter().map(|o| o.kwarg).collect();
    names.extend(settings::SETTINGS.iter().filter_map(|s| s.kwarg));
    let mut sorted = names.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), names.len(), "a keyword names two options");
    assert!(!names.contains(&"config"), "config is the dict of any key");
    for open in settings::OPEN {
        assert!(
            cmd.get_arguments().any(|a| a.get_long() == Some(open.flag)),
            "--{}",
            open.flag
        );
    }
}

/// Each registered flag is a flag of `Args`, and its help is the key's doc; no flag
/// of `Args` claims a key the registry does not give it.
#[test]
fn registered_flags_are_args_and_share_the_doc() {
    let cmd = Args::command();
    for setting in settings::SETTINGS {
        let Some(flag) = setting.flag else { continue };
        let arg = cmd
            .get_arguments()
            .find(|a| a.get_long() == Some(flag))
            .unwrap_or_else(|| panic!("--{flag} is registered for {} but not an arg", setting.key));
        let help = arg.get_help().map(|h| h.to_string()).unwrap_or_default();
        assert!(
            help.contains(setting.doc) && help.contains(setting.key),
            "--{flag}: {help}"
        );
    }
    for arg in cmd.get_arguments() {
        let help = arg.get_help().map(|h| h.to_string()).unwrap_or_default();
        if help.contains("[config: ") {
            let flag = arg.get_long().unwrap_or_default();
            assert!(settings::by_flag(flag).is_some(), "--{flag}");
        }
    }
}

#[test]
fn test_compression_detection() {
    assert_eq!(
        CompressionFormat::from_extension(Path::new("file.csv.gz")),
        Some(CompressionFormat::Gzip)
    );
    assert_eq!(
        CompressionFormat::from_extension(Path::new("file.csv.zst")),
        Some(CompressionFormat::Zstd)
    );
    assert_eq!(
        CompressionFormat::from_extension(Path::new("file.csv.bz2")),
        Some(CompressionFormat::Bzip2)
    );
    assert_eq!(
        CompressionFormat::from_extension(Path::new("file.csv.xz")),
        Some(CompressionFormat::Xz)
    );
    assert_eq!(
        CompressionFormat::from_extension(Path::new("file.csv")),
        None
    );
    assert_eq!(CompressionFormat::from_extension(Path::new("file")), None);
}

#[test]
fn test_compression_extension() {
    assert_eq!(CompressionFormat::Gzip.extension(), "gz");
    assert_eq!(CompressionFormat::Zstd.extension(), "zst");
    assert_eq!(CompressionFormat::Bzip2.extension(), "bz2");
    assert_eq!(CompressionFormat::Xz.extension(), "xz");
}

#[test]
fn test_file_format_from_path() {
    assert_eq!(
        FileFormat::from_path(Path::new("data.parquet")),
        Some(FileFormat::Parquet)
    );
    assert_eq!(
        FileFormat::from_path(Path::new("data.csv")),
        Some(FileFormat::Csv)
    );
    assert_eq!(
        FileFormat::from_path(Path::new("file.jsonl")),
        Some(FileFormat::Jsonl)
    );
    assert_eq!(FileFormat::from_path(Path::new("noext")), None);
    assert_eq!(
        FileFormat::from_path(Path::new("file.NDJSON")),
        Some(FileFormat::Jsonl)
    );
    assert_eq!(
        FileFormat::from_path(Path::new("model.gguf")),
        Some(FileFormat::Gguf)
    );
    assert_eq!(
        FileFormat::from_path(Path::new("model-00001-of-00002.safetensors")),
        Some(FileFormat::Safetensors)
    );
    assert_eq!(
        FileFormat::from_path(Path::new("model.safetensors.index.json")),
        Some(FileFormat::Safetensors),
        "an index is read as the shards it names"
    );
    assert_eq!(
        FileFormat::from_path(Path::new("song.MID")),
        Some(FileFormat::Midi)
    );
    assert_eq!(
        FileFormat::from_path(Path::new("karaoke.kar")),
        Some(FileFormat::Midi)
    );
    assert_eq!(
        FileFormat::from_path(Path::new("dump.vcd")),
        Some(FileFormat::Vcd)
    );
    for name in ["lib.sdf", "lib.SD"] {
        assert_eq!(
            FileFormat::from_path(Path::new(name)),
            Some(FileFormat::Sdf),
            "{name}"
        );
    }
    assert_eq!(
        FileFormat::from_path(Path::new("config.json")),
        Some(FileFormat::Json)
    );
    for name in [
        "take.wav",
        "take.BWF",
        "mix.rf64",
        "loop.aif",
        "loop.aiff",
        "x.aifc",
    ] {
        assert_eq!(
            FileFormat::from_path(Path::new(name)),
            Some(FileFormat::Audio),
            "{name}"
        );
    }
}
