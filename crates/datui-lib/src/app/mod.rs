//! The app around the table: the event loop and the terminal, background jobs, the
//! overlay and the dialogs it opens, and the keys of the screens without a directory of
//! their own.

pub mod applied;
pub(crate) mod background;
pub mod context_menu;
pub mod event_pump;
pub(crate) mod feedback;
pub(crate) mod footer_state;
pub mod form;
pub mod help;
pub mod hex_view;
pub(crate) mod jobs;
pub(crate) mod keys;
pub mod link_open;
pub mod modals;
pub(crate) mod overlay;
pub mod pointer;
pub(crate) mod run;
pub mod startup;
pub mod table_switch;
pub(crate) mod terminal;
pub(crate) mod terminal_color;
pub mod terminal_input;
