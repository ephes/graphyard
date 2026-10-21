//! `json.loads` for stored and fetched JSON documents.
//!
//! `serde_json` with `arbitrary_precision` passes a number token to
//! `Value`'s deserializer as a one-entry object under the private key
//! `$serde_json::private::Number`. `serde_json::from_str::<Value>` therefore
//! turns a real object whose first key is that token
//! (`{"$serde_json::private::Number": "5"}`) into the number 5 and rejects
//! such an object with more keys, where Python keeps a `dict`; the same holds
//! for `$serde_json::private::RawValue` (feature `raw_value`).
//! [`json_loads`] decodes a document that may hold such a key without
//! `Value`'s deserializer: after a syntax check of the whole document, every
//! object and array is walked through borrowed `RawValue` slices, so an
//! object always stays an object and a number is decoded from its own token.

use std::fmt;

use serde::de::{self, Deserializer as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;
use serde_json::{Map, Number, Value};

/// The prefix of the private keys `serde_json` gives special meaning in
/// `Value`'s deserializer (`Number` with `arbitrary_precision`, `RawValue`
/// with `raw_value`).
const PRIVATE_TOKEN_PREFIX: &str = "$serde_json::private::";

/// `serde_json`'s nesting limit: 128 levels are rejected, 127 accepted.
const MAX_DEPTH: usize = 128;

/// Decode a JSON document like Python's `json.loads` (objects keep their
/// key order, a repeated key keeps its first position and its last value,
/// numbers keep their token, see [`super::py_number`]).
///
/// The accepted documents are those of `serde_json::from_str::<Value>`
/// (RFC 8259; no `NaN`, lone surrogate escapes or nesting beyond 127
/// levels; PLAN.md D-4, D-11), plus the objects that spell a private
/// token, which `Value` misreads.
///
/// # Errors
/// The `serde_json` error of a rejected document. A document that may spell a
/// private token in a key (literally or with `\u` escapes) is checked by the
/// walk, whose depth and string errors carry no or slice-relative positions
/// (the error text is P2).
pub fn json_loads(text: &str) -> Result<Value, serde_json::Error> {
    if !text.contains(PRIVATE_TOKEN_PREFIX) && !text.contains("\\u") {
        return serde_json::from_str(text);
    }
    serde_json::from_str::<IgnoredAny>(text)?;
    let raw: &RawValue = serde_json::from_str(text)?;
    from_raw(raw, 0)
}

fn from_raw(raw: &RawValue, depth: usize) -> Result<Value, serde_json::Error> {
    let text = raw.get();
    let nested = || {
        if depth + 1 >= MAX_DEPTH {
            Err(de::Error::custom("recursion limit exceeded"))
        } else {
            Ok(depth + 1)
        }
    };
    match text.as_bytes().first() {
        Some(b'{') => serde_json::Deserializer::from_str(text)
            .deserialize_map(ObjectVisitor { depth: nested()? }),
        Some(b'[') => serde_json::Deserializer::from_str(text)
            .deserialize_seq(ArrayVisitor { depth: nested()? }),
        Some(b'"') => serde_json::from_str::<String>(text).map(Value::String),
        Some(b't' | b'f') => serde_json::from_str::<bool>(text).map(Value::Bool),
        Some(b'n') => Ok(Value::Null),
        _ => serde_json::from_str::<Number>(text).map(Value::Number),
    }
}

struct ObjectVisitor {
    depth: usize,
}

impl<'de> Visitor<'de> for ObjectVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut map = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            let raw: &RawValue = access.next_value()?;
            let value = from_raw(raw, self.depth).map_err(de::Error::custom)?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    }
}

struct ArrayVisitor {
    depth: usize,
}

impl<'de> Visitor<'de> for ArrayVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(raw) = access.next_element::<&RawValue>()? {
            items.push(from_raw(raw, self.depth).map_err(de::Error::custom)?);
        }
        Ok(Value::Array(items))
    }
}
