//! The exit statuses datui ends with: the binary returns these, and every manpage's
//! EXIT STATUS section is rendered from [`STATUSES`].

/// It did what was asked: the session was quit, or a command succeeded.
pub const SUCCESS: i32 = 0;
/// An error: a path that is not there, a configuration that cannot be used, nothing
/// on standard input for `-`, or a command that failed.
pub const FAILURE: i32 = 1;
/// The command line was wrong: an unknown option, or a value an option does not take.
/// clap's own status for a usage error.
pub const USAGE: i32 = 2;
/// The session was ended by SIGHUP: 128 + 1, as if it had not been caught.
pub const HANGUP: i32 = 129;
/// The session was ended by SIGINT: 128 + 2.
pub const INTERRUPTED: i32 = 130;
/// The session was ended by SIGTERM: 128 + 15.
pub const TERMINATED: i32 = 143;

/// One status and what it means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub code: i32,
    pub means: &'static str,
    /// Only `datui` itself ends this way, not a command such as `datui config`.
    pub session_only: bool,
}

const fn status(code: i32, means: &'static str, session_only: bool) -> Status {
    Status {
        code,
        means,
        session_only,
    }
}

/// Every status, in order.
pub const STATUSES: &[Status] = &[
    status(
        SUCCESS,
        "Success: the session was quit (q, Ctrl+Q or Ctrl+C), or the command did what it was asked.",
        false,
    ),
    status(
        FAILURE,
        "An error, printed to standard error: a PATH that is not there, a configuration file that cannot be used, nothing piped in for -, or a command that failed (datui formats check on a spec with errors).",
        false,
    ),
    status(
        USAGE,
        "A usage error: an unknown option or command, or a value an option does not take.",
        false,
    ),
    status(
        HANGUP,
        "The terminal hung up (SIGHUP). The screen is restored and temporary files removed first.",
        true,
    ),
    status(
        INTERRUPTED,
        "Interrupted by SIGINT, as from kill -INT. The screen is restored and temporary files removed first.",
        true,
    ),
    status(
        TERMINATED,
        "Ended by SIGTERM. The screen is restored and temporary files removed first.",
        true,
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// clap ends a usage error with the status the pages document.
    #[test]
    fn a_usage_error_is_clap_s_status() {
        let error = crate::Args::command()
            .try_get_matches_from(["datui", "--no-such-option"])
            .unwrap_err();
        assert_eq!(error.exit_code(), USAGE);
        let help = crate::Args::command()
            .try_get_matches_from(["datui", "--help"])
            .unwrap_err();
        assert_eq!(help.exit_code(), SUCCESS);
    }

    #[test]
    fn statuses_are_distinct_and_in_order() {
        for pair in STATUSES.windows(2) {
            assert!(pair[0].code < pair[1].code, "{pair:?}");
        }
    }
}
