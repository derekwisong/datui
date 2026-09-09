//! Measures what the render-time control-character sweep costs.
//!
//! Run with: cargo run --release -p datui-lib --example sanitize_cost

use datui_lib::sanitize::sanitize_buffer;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use std::time::Instant;

fn filled(w: u16, h: u16, sample: &[&str]) -> Buffer {
    let mut buf = Buffer::empty(Rect::new(0, 0, w, h));
    for (i, cell) in buf.content.iter_mut().enumerate() {
        cell.set_symbol(sample[i % sample.len()]);
    }
    buf
}

fn bench(name: &str, w: u16, h: u16, sample: &[&str]) {
    let template = filled(w, h, sample);
    let cells = (w as usize) * (h as usize);

    // Warm up, so the first run's page faults are not in the measurement.
    for _ in 0..20 {
        let mut b = template.clone();
        sanitize_buffer(&mut b);
    }

    const FRAMES: u32 = 2000;
    let start = Instant::now();
    for _ in 0..FRAMES {
        let mut b = template.clone();
        sanitize_buffer(&mut b);
    }
    let total = start.elapsed();

    // Clone cost is measured separately and subtracted: the real render path
    // sweeps a buffer it already owns, so cloning is this harness, not the fix.
    let start = Instant::now();
    for _ in 0..FRAMES {
        let b = template.clone();
        std::hint::black_box(&b);
    }
    let clone_only = start.elapsed();

    let per_frame = total.saturating_sub(clone_only) / FRAMES;
    println!(
        "{name:<34} {w}x{h} = {cells:>6} cells   {:>9.1?} per frame",
        per_frame
    );
}

fn main() {
    // Ordinary content: one plain character per cell, nothing to replace. This
    // is what essentially every real frame looks like.
    let plain = ["a", "b", " ", "1", "x", "·"];
    // Wide characters and combining marks, to check the grapheme path.
    let unicode = ["日", "é", "🦀", "a\u{0301}", " ", "─"];
    // Every cell hostile. Not a realistic frame; an upper bound.
    let hostile = ["\x1b", "a\x1b[2J", "\x07", "b\u{009b}", "\x7f", "c"];

    println!("plain content (the realistic case)");
    bench("  80x24 laptop split pane", 80, 24, &plain);
    bench("  200x50 full screen", 200, 50, &plain);
    bench("  400x100 large monitor", 400, 100, &plain);

    println!("\nunicode content");
    bench("  200x50 full screen", 200, 50, &unicode);

    println!("\nevery cell hostile (upper bound)");
    bench("  200x50 full screen", 200, 50, &hostile);
}
