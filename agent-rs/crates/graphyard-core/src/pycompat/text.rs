//! Python string helpers: `str.isspace`, `str.strip()`, `str.split()` and
//! `repr(str)`.

use std::fmt::{self, Write};

/// `str.isspace()` for one character: Unicode `White_Space` plus the ASCII
/// separators U+001C..U+001F, which Python also treats as whitespace.
pub fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// `str.strip()` without arguments.
pub fn py_strip(text: &str) -> &str {
    text.trim_matches(py_isspace)
}

/// `str.split()` without arguments: runs of whitespace separate, leading and
/// trailing whitespace yields no empty items.
pub fn py_split_whitespace(text: &str) -> Vec<&str> {
    text.split(py_isspace)
        .filter(|part| !part.is_empty())
        .collect()
}

/// Whether `repr` escapes a non-ASCII character. CPython escapes what
/// `str.isprintable()` rejects (categories Cc, Cf, Cs, Co, Cn, Zl, Zp and Zs
/// except the space). Without CPython's Unicode database Rust recognises the
/// controls and the whitespace separators; other non-printable code points
/// (format characters, unassigned, private use) are kept as they are (P2,
/// PLAN.md 2.3).
fn escapes_non_ascii(c: char) -> bool {
    c.is_control() || c.is_whitespace()
}

#[cfg(test)]
thread_local! {
    pub(crate) static REPR_INPUT_CHARS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// `repr(text)` for a `str`: single quotes unless the text contains a single
/// quote and no double quote; `\\`, `\t`, `\n`, `\r` and the quote escaped;
/// other ASCII controls and DEL as `\xNN`; non-ASCII characters kept unless
/// non-printable (`\xNN`, `\uNNNN`, `\UNNNNNNNN`).
pub fn py_str_repr(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    let _ = write_repr(&mut out, text);
    out
}

/// The first `limit` characters of `repr(text)`, possibly ending inside
/// an ASCII escape. The full input still determines the opening quote.
pub(super) fn py_str_repr_prefix(text: &str, limit: usize) -> String {
    let mut prefix = ReprPrefix {
        out: String::new(),
        remaining: limit,
    };
    let _ = write_repr(&mut prefix, text);
    prefix.out
}

struct ReprPrefix {
    out: String,
    remaining: usize,
}

impl Write for ReprPrefix {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let count = text.chars().count();
        if count >= self.remaining {
            let end = text
                .char_indices()
                .nth(self.remaining)
                .map_or(text.len(), |(index, _)| index);
            self.out.push_str(&text[..end]);
            // Stop the formatter once the prefix is complete.
            return Err(fmt::Error);
        }
        self.out.push_str(text);
        self.remaining -= count;
        Ok(())
    }
}

fn write_repr(out: &mut impl Write, text: &str) -> fmt::Result {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    out.write_char(quote)?;
    for c in text.chars() {
        #[cfg(test)]
        REPR_INPUT_CHARS.with(|count| count.set(count.get() + 1));
        match c {
            '\\' => out.write_str("\\\\")?,
            '\t' => out.write_str("\\t")?,
            '\n' => out.write_str("\\n")?,
            '\r' => out.write_str("\\r")?,
            c if c == quote => {
                out.write_char('\\')?;
                out.write_char(c)?;
            }
            ' '..='~' => out.write_char(c)?,
            c if c.is_ascii() || escapes_non_ascii(c) => {
                let code = u32::from(c);
                if code <= 0xff {
                    write!(out, "\\x{code:02x}")
                } else if code <= 0xffff {
                    write!(out, "\\u{code:04x}")
                } else {
                    write!(out, "\\U{code:08x}")
                }?;
            }
            c => out.write_char(c)?,
        }
    }
    out.write_char(quote)
}

/// Unicode 15.0 decimal zeroes, excluding ASCII. Each begins a run of
/// ten code points in numeric order. This is the database used by Python 3.12.
pub const DECIMAL_ZEROES: &[u32] = &[
    0x660, 0x6f0, 0x7c0, 0x966, 0x9e6, 0xa66, 0xae6, 0xb66, 0xbe6, 0xc66, 0xce6, 0xd66, 0xde6,
    0xe50, 0xed0, 0xf20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90, 0x1b50,
    0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0, 0xff10,
    0x104a0, 0x10d30, 0x11066, 0x110f0, 0x11136, 0x111d0, 0x112f0, 0x11450, 0x114d0, 0x11650,
    0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50, 0x11d50, 0x11da0, 0x11f50, 0x16a60, 0x16ac0,
    0x16b50, 0x1d7ce, 0x1d7d8, 0x1d7e2, 0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e4f0, 0x1e950,
    0x1fbf0,
];

/// The decimal value of a character according to Python 3.12.
pub fn decimal_digit(c: char) -> Option<u32> {
    if c.is_ascii() {
        return c.to_digit(10);
    }
    let code = u32::from(c);
    let position = DECIMAL_ZEROES.partition_point(|&zero| zero <= code);
    let digit = code - DECIMAL_ZEROES[position.checked_sub(1)?];
    (digit < 10).then_some(digit)
}
