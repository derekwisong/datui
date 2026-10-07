use super::*;

/// A mean reads as the table writes a float, not to the last digit, with the
/// table's grouping; a whole number reads plainly.
#[test]
fn readout_numbers_read_as_the_table_writes_them() {
    let plain = AxisNumbers::default();
    assert_eq!(plain.write(19.434782608695652), "19.434783");
    assert_eq!(plain.write(-0.5), "-0.5");
    let grouped = AxisNumbers {
        format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
        whole: false,
    };
    assert_eq!(grouped.write(12345.678901234), "12,345.678901");
    let whole = AxisNumbers {
        whole: true,
        ..grouped
    };
    assert_eq!(whole.write(1234.0), "1,234");
}

fn place(width: u16) -> PlotPlace {
    PlotPlace {
        graph: Rect::new(10, 0, width, 10),
        x_bounds: [0.0, 100.0],
        sub: 1,
    }
}

/// Sparse points step one by one; dense ones a column at a time, never past a
/// column that has a point.
#[test]
fn the_cursor_steps_point_to_point_and_column_to_column() {
    let sparse = xs(&[vec![(0.0, 1.0), (50.0, 2.0), (100.0, 3.0)]]);
    let p = place(101);
    assert_eq!(step(&sparse, &p, 0.0, Move::Right), Some(50.0));
    assert_eq!(step(&sparse, &p, 50.0, Move::Right), Some(100.0));
    assert_eq!(step(&sparse, &p, 100.0, Move::Right), Some(100.0), "stays");
    assert_eq!(step(&sparse, &p, 50.0, Move::Left), Some(0.0));
    assert_eq!(step(&sparse, &p, 0.0, Move::Left), Some(0.0), "stays");

    // A thousand points across eleven columns: ten-odd a column.
    let dense: Vec<f64> = (0..1000).map(|i| f64::from(i) / 10.0).collect();
    let p = place(11);
    let mut at = 0.0;
    let mut columns = vec![p.column(at)];
    while let Some(next) = step(&dense, &p, at, Move::Right).filter(|&n| n != at) {
        at = next;
        columns.push(p.column(at));
    }
    assert_eq!(columns, (10..21).collect::<Vec<u16>>());
    // And back the same way, on the first point of each column.
    let back = step(&dense, &p, at, Move::Left).unwrap();
    assert_eq!(p.column(back), 19);
    assert_eq!(step(&dense, &p, back, Move::Right), Some(at));
    assert_eq!(step(&dense, &p, 42.0, Move::First), Some(0.0));
    assert_eq!(step(&dense, &p, 42.0, Move::Last), Some(99.9));
}

/// A click lands on the point drawn nearest it.
#[test]
fn a_click_lands_on_the_nearest_point() {
    let sparse = xs(&[vec![(0.0, 1.0), (50.0, 2.0)], vec![(100.0, 3.0)]]);
    let p = place(101);
    assert_eq!(at_column(&sparse, &p, 10), Some(0.0));
    assert_eq!(at_column(&sparse, &p, 40), Some(50.0));
    assert_eq!(at_column(&sparse, &p, 300), Some(100.0));
    assert_eq!(nearest(&sparse, 80.0), Some(100.0));
    assert_eq!(nearest(&[], 80.0), None);
}

/// Each series' value at the cursor, none where it has a gap there.
#[test]
fn values_at_the_cursor() {
    let series = vec![vec![(1.0, 10.0), (2.0, 20.0)], vec![(1.0, -1.0)]];
    assert_eq!(values_at(&series, 2.0), [Some(20.0), None]);
    assert_eq!(values_at(&series, 1.0), [Some(10.0), Some(-1.0)]);
}

/// Dates read in full, numbers as the table writes them.
#[test]
fn the_readout_writes_x_in_full() {
    let plain = AxisNumbers::default();
    assert_eq!(
        format_x(19_783.0, XAxisTemporalKind::Date, &plain),
        "2024-03-01"
    );
    assert_eq!(
        format_x(1_709_294_400_000.0, XAxisTemporalKind::DatetimeMs, &plain),
        "2024-03-01 12:00:00"
    );
    assert_eq!(format_x(2.5, XAxisTemporalKind::Numeric, &plain), "2.5");
    assert_eq!(format_x(3.0, XAxisTemporalKind::Numeric, &plain), "3");
}

fn entry(name: &str, value: &str) -> Entry {
    Entry {
        name: name.to_string(),
        name_style: Style::default(),
        value: value.to_string(),
        value_style: Style::default(),
    }
}

fn text(lines: &[Line<'_>]) -> Vec<String> {
    lines.iter().map(|l| l.to_string()).collect()
}

/// Entries flow onto as many lines as they take, never cut while a line holds one.
#[test]
fn the_readout_flows_onto_lines() {
    let g = crate::glyphs::unicode();
    let entries = [
        entry("date", "2024-03-01"),
        entry("temperature", "21.5"),
        entry("humidity", "44"),
    ];
    assert_eq!(
        text(&readout_lines(&entries, 80, g)),
        ["date: 2024-03-01   temperature: 21.5   humidity: 44"]
    );
    assert_eq!(
        text(&readout_lines(&entries, 40, g)),
        ["date: 2024-03-01   temperature: 21.5", "humidity: 44"]
    );
    assert_eq!(
        text(&readout_lines(&entries, 12, g)),
        ["date: 2024-…", "temperature:", "humidity: 44"]
    );
}

/// The cursor's line runs down the plot under the series, and its tick sits on
/// the axis.
#[test]
fn the_cursor_line_keeps_the_series_marks() {
    for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
        let area = Rect::new(0, 0, 20, 6);
        let mut buf = Buffer::empty(area);
        let p = PlotPlace {
            graph: Rect::new(2, 0, 18, 5),
            x_bounds: [0.0, 17.0],
            sub: 1,
        };
        for x in 0..20 {
            buf[(x, 5)].set_symbol(g.plot.axis.horizontal);
        }
        buf[(7, 2)].set_symbol("x");
        draw(&mut buf, &p, 5.0, Style::default(), g);
        let column: String = (0..6).map(|y| buf[(7, y)].symbol().to_string()).collect();
        let v = g.plot.axis.vertical;
        assert_eq!(column, format!("{v}{v}x{v}{v}{}", g.plot.tick_x));
    }
}
