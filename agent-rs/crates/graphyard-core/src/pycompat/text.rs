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
    pub(super) static REPR_INPUT_CHARS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
