//! The component kit: the pieces every surface is built from.
//!
//! One Surface per modal or sidebar, FormRows inside it, a Picker for pick-one
//! lists, a HintBar for the keys, a SectionRule to divide space without borders.
//! The canon these implement is the `ui-style` skill; a screen that hand-rolls
//! one of these is a migration target.

mod form_row;
mod hintbar;
mod picker;
mod section_rule;
mod surface;

pub use form_row::{FormRow, FormValue};
pub use hintbar::HintBar;
pub use picker::{Picker, PickerState};
pub use section_rule::SectionRule;
pub use surface::Surface;
