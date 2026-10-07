use super::*;

fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let pairs: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name| {
        pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }
}

#[test]
fn text_goes_to_the_editor_the_environment_names() {
    assert_eq!(
        program_for(false, env(&[("EDITOR", "code --wait"), ("PAGER", "less")])),
        Program::Wait(vec!["code".into(), "--wait".into()])
    );
    assert_eq!(
        program_for(false, env(&[("VISUAL", "vim"), ("EDITOR", "nano")])),
        Program::Wait(vec!["vim".into()])
    );
    assert_eq!(
        program_for(false, env(&[("EDITOR", " "), ("PAGER", "most")])),
        Program::Wait(vec!["most".into()]),
        "blank is unset"
    );
    let none = program_for(false, env(&[]));
    if cfg!(windows) {
        assert_eq!(none, Program::Opener(opener()));
    } else {
        assert_eq!(none, Program::Wait(vec!["less".into()]));
    }
    // An image goes to the system's viewer, whatever the editor.
    assert_eq!(
        program_for(true, env(&[("EDITOR", "vim")])),
        Program::Opener(opener())
    );
}

#[test]
fn names_are_safe_and_files_read_only() {
    assert_eq!(
        file_name("payload json", 3, "json"),
        "payload_json-row3.json"
    );
    assert_eq!(file_name("../x", 1, "txt"), "_x-row1.txt");
    assert_eq!(file_name("", 1, "bin"), "value-row1.bin");
    let dir = tempfile::tempdir().unwrap();
    let path = write_value(dir.path(), "a.txt", b"one").unwrap();
    if cfg!(unix) {
        assert!(std::fs::metadata(&path).unwrap().permissions().readonly());
    }
    let again = write_value(dir.path(), "a.txt", b"two").unwrap();
    assert_ne!(again, path);
    assert_eq!(std::fs::read(again).unwrap(), b"two");
}
