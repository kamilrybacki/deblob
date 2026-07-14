//! Bounds enforced by the recursive-descent parser. Spec §4.

/// Hard ceilings applied while walking untrusted JSON bytes. Every field is
/// checked *during* the walk, before the corresponding allocation happens,
/// so a caller-supplied `Limits` value fully determines the worst-case
/// memory/CPU envelope of `parse_bounded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum size, in bytes, of the input document. Checked first, before
    /// any allocation.
    pub max_bytes: usize,
    /// Maximum nesting depth of arrays/objects (checked before recursing).
    pub max_depth: u32,
    /// Maximum number of fields accepted in a single object.
    pub max_fields_per_object: usize,
    /// Maximum byte length of an object key.
    pub max_key_len: usize,
    /// Maximum byte length of a decoded string value.
    pub max_string_len: usize,
    /// Maximum number of array elements actually parsed into `Node`s;
    /// remaining elements are skipped and the array is marked truncated.
    pub max_array_inspect: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024,
            max_depth: 32,
            max_fields_per_object: 1024,
            max_key_len: 256,
            max_string_len: 64 * 1024,
            max_array_inspect: 4096,
        }
    }
}
