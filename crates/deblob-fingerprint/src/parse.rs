//! Hand-rolled recursive-descent JSON parser over `&[u8]`. Deliberately does
//! NOT delegate to `serde_json`: we need duplicate-key rejection, exact
//! number-text preservation (never round-tripped through `f64`), and limit
//! enforcement *during* the walk rather than after building an unbounded
//! tree. Spec §4.

use deblob_core::error::QuarantineReason;

use crate::limits::Limits;

/// A parsed JSON value. Numbers keep their original source text (never
/// parsed through `f64`) so canonicalization downstream is exact.
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    /// `bool` is `true` when elements beyond `max_array_inspect` were
    /// skipped rather than parsed.
    Array(Vec<Node>, bool),
    Object(Vec<(String, Node)>),
}

/// Parse `bytes` into a [`Node`] tree, enforcing every bound in `limits`
/// during the walk. Never panics, allocates unboundedly, or recurses past
/// `limits.max_depth`; every failure mode is a [`QuarantineReason`].
pub fn parse_bounded(bytes: &[u8], limits: &Limits) -> Result<Node, QuarantineReason> {
    if bytes.len() > limits.max_bytes {
        return Err(QuarantineReason::SizeExceeded);
    }
    let mut parser = Parser {
        bytes,
        pos: 0,
        limits,
    };
    let node = parser.parse_value(0)?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return Err(QuarantineReason::ParseError);
    }
    Ok(node)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    limits: &'a Limits,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn parse_value(&mut self, depth: u32) -> Result<Node, QuarantineReason> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_object(depth),
            Some(b'[') => self.parse_array(depth),
            Some(b'"') => self.parse_string().map(Node::String),
            Some(b't') => self.parse_literal(b"true", Node::Bool(true)),
            Some(b'f') => self.parse_literal(b"false", Node::Bool(false)),
            Some(b'n') => self.parse_literal(b"null", Node::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            _ => Err(QuarantineReason::ParseError),
        }
    }

    /// Depth-guarded skip: walks past a value without building a `Node`,
    /// used for array elements beyond `max_array_inspect`.
    fn skip_value(&mut self, depth: u32) -> Result<(), QuarantineReason> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.skip_object(depth),
            Some(b'[') => self.skip_array(depth),
            Some(b'"') => self.parse_string().map(|_| ()),
            Some(b't') => self.skip_literal(b"true"),
            Some(b'f') => self.skip_literal(b"false"),
            Some(b'n') => self.skip_literal(b"null"),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number().map(|_| ()),
            _ => Err(QuarantineReason::ParseError),
        }
    }

    fn parse_object(&mut self, depth: u32) -> Result<Node, QuarantineReason> {
        let next_depth = depth + 1;
        if next_depth > self.limits.max_depth {
            return Err(QuarantineReason::DepthExceeded);
        }
        self.bump(); // consume '{'
        let mut fields: Vec<(String, Node)> = Vec::new();
        let mut seen_keys: Vec<String> = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(Node::Object(fields));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(QuarantineReason::ParseError);
            }
            let key = self.parse_key()?;
            match seen_keys.binary_search(&key) {
                Ok(_) => return Err(QuarantineReason::DuplicateKey),
                Err(idx) => seen_keys.insert(idx, key.clone()),
            }
            self.skip_ws();
            if self.bump() != Some(b':') {
                return Err(QuarantineReason::ParseError);
            }
            let value = self.parse_value(next_depth)?;
            fields.push((key, value));
            if fields.len() > self.limits.max_fields_per_object {
                return Err(QuarantineReason::FieldCountExceeded);
            }
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b'}') => break,
                _ => return Err(QuarantineReason::ParseError),
            }
        }
        Ok(Node::Object(fields))
    }

    fn skip_object(&mut self, depth: u32) -> Result<(), QuarantineReason> {
        let next_depth = depth + 1;
        if next_depth > self.limits.max_depth {
            return Err(QuarantineReason::DepthExceeded);
        }
        self.bump(); // consume '{'
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(());
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(QuarantineReason::ParseError);
            }
            self.parse_key()?;
            self.skip_ws();
            if self.bump() != Some(b':') {
                return Err(QuarantineReason::ParseError);
            }
            self.skip_value(next_depth)?;
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b'}') => break,
                _ => return Err(QuarantineReason::ParseError),
            }
        }
        Ok(())
    }

    fn parse_array(&mut self, depth: u32) -> Result<Node, QuarantineReason> {
        let next_depth = depth + 1;
        if next_depth > self.limits.max_depth {
            return Err(QuarantineReason::DepthExceeded);
        }
        self.bump(); // consume '['
        let mut items = Vec::new();
        let mut truncated = false;
        let mut index: usize = 0;
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(Node::Array(items, false));
        }
        loop {
            self.skip_ws();
            if index < self.limits.max_array_inspect {
                items.push(self.parse_value(next_depth)?);
            } else {
                self.skip_value(next_depth)?;
                truncated = true;
            }
            index += 1;
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b']') => break,
                _ => return Err(QuarantineReason::ParseError),
            }
        }
        Ok(Node::Array(items, truncated))
    }

    fn skip_array(&mut self, depth: u32) -> Result<(), QuarantineReason> {
        let next_depth = depth + 1;
        if next_depth > self.limits.max_depth {
            return Err(QuarantineReason::DepthExceeded);
        }
        self.bump(); // consume '['
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(());
        }
        loop {
            self.skip_value(next_depth)?;
            self.skip_ws();
            match self.bump() {
                Some(b',') => continue,
                Some(b']') => break,
                _ => return Err(QuarantineReason::ParseError),
            }
        }
        Ok(())
    }

    /// Object key: same grammar as a string, bounded by `max_key_len`.
    fn parse_key(&mut self) -> Result<String, QuarantineReason> {
        let s = self.scan_raw_string()?;
        if s.len() > self.limits.max_key_len {
            return Err(QuarantineReason::KeyLengthExceeded);
        }
        Ok(s)
    }

    /// String value, bounded by `max_string_len`. Reuses `SizeExceeded`
    /// since a value string that outgrows its bound is, semantically, a
    /// size violation (the dedicated `KeyLengthExceeded` reason is
    /// deliberately reserved for object keys only).
    fn parse_string(&mut self) -> Result<String, QuarantineReason> {
        let s = self.scan_raw_string()?;
        if s.len() > self.limits.max_string_len {
            return Err(QuarantineReason::SizeExceeded);
        }
        Ok(s)
    }

    /// Scans a JSON string literal (opening `"` through closing `"`),
    /// decoding escapes and validating the result as UTF-8. Raw control
    /// bytes (`< 0x20`) are rejected per the JSON grammar.
    fn scan_raw_string(&mut self) -> Result<String, QuarantineReason> {
        self.bump(); // consume opening '"'
        let mut buf: Vec<u8> = Vec::new();
        loop {
            match self.bump() {
                None => return Err(QuarantineReason::ParseError),
                Some(b'"') => break,
                Some(b'\\') => match self.bump() {
                    Some(b'"') => buf.push(b'"'),
                    Some(b'\\') => buf.push(b'\\'),
                    Some(b'/') => buf.push(b'/'),
                    Some(b'b') => buf.push(0x08),
                    Some(b'f') => buf.push(0x0C),
                    Some(b'n') => buf.push(b'\n'),
                    Some(b'r') => buf.push(b'\r'),
                    Some(b't') => buf.push(b'\t'),
                    Some(b'u') => {
                        let ch = self.read_unicode_escape()?;
                        let mut tmp = [0u8; 4];
                        buf.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                    }
                    _ => return Err(QuarantineReason::ParseError),
                },
                Some(c) if c < 0x20 => return Err(QuarantineReason::ParseError),
                Some(c) => buf.push(c),
            }
        }
        String::from_utf8(buf).map_err(|_| QuarantineReason::Utf8Error)
    }

    /// Reads a `\uXXXX` escape (already past the `u`), combining a
    /// surrogate pair into one scalar when present.
    fn read_unicode_escape(&mut self) -> Result<char, QuarantineReason> {
        let hi = self.read_hex4()?;
        if (0xD800..=0xDBFF).contains(&hi) {
            if self.bump() != Some(b'\\') || self.bump() != Some(b'u') {
                return Err(QuarantineReason::ParseError);
            }
            let lo = self.read_hex4()?;
            if !(0xDC00..=0xDFFF).contains(&lo) {
                return Err(QuarantineReason::ParseError);
            }
            let scalar = 0x10000 + ((hi as u32 - 0xD800) << 10) + (lo as u32 - 0xDC00);
            char::from_u32(scalar).ok_or(QuarantineReason::Utf8Error)
        } else {
            char::from_u32(hi as u32).ok_or(QuarantineReason::Utf8Error)
        }
    }

    fn read_hex4(&mut self) -> Result<u16, QuarantineReason> {
        let mut v: u16 = 0;
        for _ in 0..4 {
            let d = match self.bump() {
                Some(c @ b'0'..=b'9') => c - b'0',
                Some(c @ b'a'..=b'f') => c - b'a' + 10,
                Some(c @ b'A'..=b'F') => c - b'A' + 10,
                _ => return Err(QuarantineReason::ParseError),
            };
            v = v * 16 + d as u16;
        }
        Ok(v)
    }

    /// Captures the raw source text of a JSON number after validating it
    /// against the JSON number grammar. Never parsed through `f64`, so the
    /// exact source digits (and thus float precision) survive unchanged.
    fn parse_number(&mut self) -> Result<Node, QuarantineReason> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.bump();
        }
        match self.bump() {
            Some(b'0') => {}
            Some(c) if c.is_ascii_digit() => {
                while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                    self.bump();
                }
            }
            _ => return Err(QuarantineReason::ParseError),
        }
        if self.peek() == Some(b'.') {
            self.bump();
            if !matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                return Err(QuarantineReason::ParseError);
            }
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.bump();
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.bump();
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.bump();
            }
            if !matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                return Err(QuarantineReason::ParseError);
            }
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.bump();
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| QuarantineReason::Utf8Error)?
            .to_string();
        Ok(Node::Number(text))
    }

    fn parse_literal(&mut self, kw: &'static [u8], node: Node) -> Result<Node, QuarantineReason> {
        self.skip_literal(kw)?;
        Ok(node)
    }

    fn skip_literal(&mut self, kw: &'static [u8]) -> Result<(), QuarantineReason> {
        if self.bytes[self.pos..].starts_with(kw) {
            self.pos += kw.len();
            Ok(())
        } else {
            Err(QuarantineReason::ParseError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::error::QuarantineReason as Q;

    #[test]
    fn rejects_duplicate_keys() {
        assert_eq!(
            parse_bounded(br#"{"a":1,"a":2}"#, &Limits::default()).unwrap_err(),
            Q::DuplicateKey
        );
    }
    #[test]
    fn rejects_depth_over_limit() {
        let deep = format!("{}1{}", "[".repeat(40), "]".repeat(40));
        let l = Limits {
            max_depth: 32,
            ..Default::default()
        };
        assert_eq!(
            parse_bounded(deep.as_bytes(), &l).unwrap_err(),
            Q::DepthExceeded
        );
    }
    #[test]
    fn rejects_oversize_before_parse() {
        let l = Limits {
            max_bytes: 8,
            ..Default::default()
        };
        assert_eq!(
            parse_bounded(br#"{"aaaa": 1}"#, &l).unwrap_err(),
            Q::SizeExceeded
        );
    }
    #[test]
    fn preserves_number_text_exactly() {
        let n = parse_bounded(br#"{"x": 0.30000000000000004}"#, &Limits::default()).unwrap();
        let Node::Object(fields) = n else { panic!() };
        let Node::Number(t) = &fields[0].1 else {
            panic!()
        };
        assert_eq!(t, "0.30000000000000004"); // no f64 round-trip (§4)
    }
    #[test]
    #[allow(clippy::useless_vec)] // verbatim brief test body
    fn array_over_inspect_limit_marks_truncated() {
        let big = format!("[{}]", vec!["1"; 10].join(","));
        let l = Limits {
            max_array_inspect: 3,
            ..Default::default()
        };
        let Node::Array(items, truncated) = parse_bounded(big.as_bytes(), &l).unwrap() else {
            panic!()
        };
        assert_eq!(items.len(), 3);
        assert!(truncated);
    }
    #[test]
    fn rejects_key_over_length() {
        let payload = format!("{{\"{}\": 1}}", "k".repeat(300));
        assert_eq!(
            parse_bounded(payload.as_bytes(), &Limits::default()).unwrap_err(),
            Q::KeyLengthExceeded
        );
    }
    #[test]
    fn rejects_field_count_over_limit() {
        let l = Limits {
            max_fields_per_object: 2,
            ..Default::default()
        };
        assert_eq!(
            parse_bounded(br#"{"a":1,"b":2,"c":3}"#, &l).unwrap_err(),
            Q::FieldCountExceeded
        );
    }
}
