//! Python string helpers: `str.isspace`, `str.strip()`, `str.split()` and
//! `repr(str)`.

use std::fmt::Write as _;

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

/// `repr(text)` for a `str`: single quotes unless the text contains a single
/// quote and no double quote; `\\`, `\t`, `\n`, `\r` and the quote escaped;
/// other ASCII controls and DEL as `\xNN`; non-ASCII characters kept unless
/// non-printable (`\xNN`, `\uNNNN`, `\UNNNNNNNN`).
pub fn py_str_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            ' '..='~' => out.push(c),
            c if c.is_ascii() || escapes_non_ascii(c) => {
                let code = u32::from(c);
                let _ = if code <= 0xff {
                    write!(out, "\\x{code:02x}")
                } else if code <= 0xffff {
                    write!(out, "\\u{code:04x}")
                } else {
                    write!(out, "\\U{code:08x}")
                };
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}
