//! A chart's PDF, written from the outlines usvg set: every mark and every glyph a
//! filled or stroked path in sRGB, on a page the figure's size in points. A chart
//! is only ever solid fills and strokes, so this is all of PDF it needs.

use std::fmt::Write as _;
use std::io::Write as _;

use color_eyre::Result;
use resvg::tiny_skia::{PathSegment, Point, Transform};
use resvg::usvg;

/// The PDF of `tree`, a figure `width` by `height` px at `dpi`.
pub fn write(tree: &usvg::Tree, width: u32, height: u32, dpi: f32) -> Result<Vec<u8>> {
    let scale = 72.0 / f64::from(dpi);
    let (w_pt, h_pt) = (f64::from(width) * scale, f64::from(height) * scale);
    let mut page = Page::default();
    // From the SVG's pixels, y down, to the page's points, y up.
    writeln!(
        page.content,
        "{} 0 0 {} 0 {} cm",
        num(scale),
        num(-scale),
        num(h_pt)
    )?;
    page.group(tree.root(), 1.0)?;

    let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    z.write_all(page.content.as_bytes())?;
    let stream = z.finish()?;

    let mut states = String::new();
    for (i, (fill, stroke)) in page.states.iter().enumerate() {
        write!(
            states,
            "/G{i} << /ca {} /CA {} >> ",
            num(*fill),
            num(*stroke)
        )?;
    }
    let mut out: Vec<u8> = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n".to_vec();
    let mut offsets = Vec::new();
    let mut object = |out: &mut Vec<u8>, body: &[u8]| {
        offsets.push(out.len());
        out.extend_from_slice(format!("{} 0 obj\n", offsets.len()).as_bytes());
        out.extend_from_slice(body);
        out.extend_from_slice(b"\nendobj\n");
    };
    object(&mut out, b"<< /Type /Catalog /Pages 2 0 R >>");
    object(&mut out, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>");
    object(
        &mut out,
        format!(
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {} {}] \
             /Resources << /ExtGState << {states}>> >> /Contents 4 0 R >>",
            num(w_pt),
            num(h_pt)
        )
        .as_bytes(),
    );
    let mut body = format!(
        "<< /Length {} /Filter /FlateDecode >>\nstream\n",
        stream.len()
    )
    .into_bytes();
    body.extend_from_slice(&stream);
    body.extend_from_slice(b"\nendstream");
    object(&mut out, &body);
    object(&mut out, b"<< /Producer (datui) >>");
    let xref = out.len();
    let mut table = format!("xref\n0 {}\n0000000000 65535 f \n", offsets.len() + 1);
    for offset in &offsets {
        writeln!(table, "{offset:010} 00000 n ")?;
    }
    write!(
        table,
        "trailer\n<< /Size {} /Root 1 0 R /Info 5 0 R >>\nstartxref\n{xref}\n%%EOF\n",
        offsets.len() + 1
    )?;
    out.extend_from_slice(table.as_bytes());
    Ok(out)
}

/// A number as PDF writes it: short, and never in exponent form.
fn num(v: f64) -> String {
    let s = format!("{:.4}", if v.abs() < 1e-9 { 0.0 } else { v });
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s == "-0" {
        "0".to_string()
    } else {
        s.to_string()
    }
}

/// The page's content stream, and the opacities it sets, by name `G0`, `G1`, ...
#[derive(Default)]
struct Page {
    content: String,
    states: Vec<(f64, f64)>,
}

impl Page {
    /// The state setting fill and stroke opacity, added the first time it is asked.
    fn state(&mut self, fill: f64, stroke: f64) -> usize {
        let at = self
            .states
            .iter()
            .position(|&(f, s)| (f - fill).abs() < 1e-4 && (s - stroke).abs() < 1e-4);
        at.unwrap_or_else(|| {
            self.states.push((fill, stroke));
            self.states.len() - 1
        })
    }

    fn group(&mut self, group: &usvg::Group, opacity: f64) -> Result<()> {
        let opacity = opacity * f64::from(group.opacity().get());
        for node in group.children() {
            match node {
                usvg::Node::Group(g) => self.group(g, opacity)?,
                usvg::Node::Path(p) => self.path(p, opacity)?,
                usvg::Node::Text(t) => self.group(t.flattened(), opacity)?,
                // A chart draws no images.
                usvg::Node::Image(_) => {}
            }
        }
        Ok(())
    }

    fn path(&mut self, path: &usvg::Path, opacity: f64) -> Result<()> {
        if !path.is_visible() {
            return Ok(());
        }
        let color = |paint: &usvg::Paint| match paint {
            usvg::Paint::Color(c) => Some(*c),
            // A chart paints with plain colors only.
            _ => None,
        };
        let fill = path
            .fill()
            .and_then(|f| Some((color(f.paint())?, f.opacity().get(), f.rule())));
        let stroke = path.stroke().and_then(|s| {
            Some((
                color(s.paint())?,
                s.opacity().get(),
                s.width().get(),
                s.linecap(),
                s.linejoin(),
            ))
        });
        if fill.is_none() && stroke.is_none() {
            return Ok(());
        }
        let fill_alpha = fill.as_ref().map_or(1.0, |f| f64::from(f.1)) * opacity;
        let stroke_alpha = stroke.as_ref().map_or(1.0, |s| f64::from(s.1)) * opacity;
        let state = self.state(fill_alpha, stroke_alpha);
        let c = &mut self.content;
        let Transform {
            sx,
            ky,
            kx,
            sy,
            tx,
            ty,
        } = path.abs_transform();
        writeln!(c, "q /G{state} gs")?;
        writeln!(
            c,
            "{} {} {} {} {} {} cm",
            num(sx.into()),
            num(ky.into()),
            num(kx.into()),
            num(sy.into()),
            num(tx.into()),
            num(ty.into())
        )?;
        if let Some((color, ..)) = &fill {
            writeln!(c, "{} rg", rgb(color))?;
        }
        if let Some((color, _, width, cap, join)) = &stroke {
            let cap = match cap {
                usvg::LineCap::Butt => 0,
                usvg::LineCap::Round => 1,
                usvg::LineCap::Square => 2,
            };
            let join = match join {
                usvg::LineJoin::Round => 1,
                usvg::LineJoin::Bevel => 2,
                _ => 0,
            };
            writeln!(
                c,
                "{} RG {} w {cap} J {join} j",
                rgb(color),
                num((*width).into())
            )?;
        }
        let mut last = Point::zero();
        for segment in path.data().segments() {
            match segment {
                PathSegment::MoveTo(p) => {
                    writeln!(c, "{} {} m", num(p.x.into()), num(p.y.into()))?;
                    last = p;
                }
                PathSegment::LineTo(p) => {
                    writeln!(c, "{} {} l", num(p.x.into()), num(p.y.into()))?;
                    last = p;
                }
                PathSegment::QuadTo(q, p) => {
                    // A quadratic as the cubic through the same curve.
                    let c1 = (
                        last.x + 2.0 / 3.0 * (q.x - last.x),
                        last.y + 2.0 / 3.0 * (q.y - last.y),
                    );
                    let c2 = (p.x + 2.0 / 3.0 * (q.x - p.x), p.y + 2.0 / 3.0 * (q.y - p.y));
                    writeln!(
                        c,
                        "{} {} {} {} {} {} c",
                        num(c1.0.into()),
                        num(c1.1.into()),
                        num(c2.0.into()),
                        num(c2.1.into()),
                        num(p.x.into()),
                        num(p.y.into())
                    )?;
                    last = p;
                }
                PathSegment::CubicTo(a, b, p) => {
                    writeln!(
                        c,
                        "{} {} {} {} {} {} c",
                        num(a.x.into()),
                        num(a.y.into()),
                        num(b.x.into()),
                        num(b.y.into()),
                        num(p.x.into()),
                        num(p.y.into())
                    )?;
                    last = p;
                }
                PathSegment::Close => writeln!(c, "h")?,
            }
        }
        let even_odd = fill
            .as_ref()
            .is_some_and(|f| f.2 == usvg::FillRule::EvenOdd);
        let op = match (fill.is_some(), stroke.is_some(), even_odd) {
            (true, true, false) => "B",
            (true, true, true) => "B*",
            (true, false, false) => "f",
            (true, false, true) => "f*",
            _ => "S",
        };
        writeln!(c, "{op} Q")?;
        Ok(())
    }
}

fn rgb(c: &usvg::Color) -> String {
    let part = |v: u8| num(f64::from(v) / 255.0);
    format!("{} {} {}", part(c.red), part(c.green), part(c.blue))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_are_short_and_plain() {
        assert_eq!(num(1.0), "1");
        assert_eq!(num(0.75), "0.75");
        assert_eq!(num(-0.00001), "0");
        assert_eq!(num(1e-12), "0");
        assert_eq!(num(1234.56789), "1234.5679");
    }

    /// A page the figure's size in points, its marks in the content stream, and an
    /// xref whose offsets point at the objects.
    #[test]
    fn a_page_of_paths() {
        let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" width="300" height="200">
            <rect x="10" y="10" width="50" height="40" fill="#2a78d6" fill-opacity="0.5"/>
            <polyline points="0,0 100,100 200,50" fill="none" stroke="#eb6834" stroke-width="2"/>
        </svg>"##;
        let tree = usvg::Tree::from_str(svg, &usvg::Options::default()).unwrap();
        let pdf = write(&tree, 300, 200, 144.0).unwrap();
        let text = String::from_utf8_lossy(&pdf);
        assert!(pdf.starts_with(b"%PDF-1.4"));
        assert!(text.contains("/MediaBox [0 0 150 100]"), "{text}");
        assert!(text.contains("/ca 0.5"), "{text}");
        assert!(text.trim_end().ends_with("%%EOF"));
        // Offsets are bytes: read them off the bytes, not the lossy text.
        let find = |needle: &[u8]| pdf.windows(needle.len()).rposition(|w| w == needle);
        let at = find(b"startxref\n").unwrap() + b"startxref\n".len();
        let end = at + pdf[at..].iter().position(|&b| b == b'\n').unwrap();
        let xref: usize = std::str::from_utf8(&pdf[at..end]).unwrap().parse().unwrap();
        assert!(
            pdf[xref..].starts_with(b"xref"),
            "startxref points at the table"
        );
        let table = std::str::from_utf8(&pdf[xref..]).unwrap();
        for (i, line) in table.lines().skip(3).take(5).enumerate() {
            let offset: usize = line[..10].parse().unwrap();
            assert!(
                pdf[offset..].starts_with(format!("{} 0 obj", i + 1).as_bytes()),
                "object {} at {offset}",
                i + 1
            );
        }
    }
}
