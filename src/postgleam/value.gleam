/// Dynamic value type representing PostgreSQL values in binary wire format.
/// Users pattern-match on these to extract typed values.

import gleam/bit_array
import gleam/option.{type Option}

/// A PostgreSQL value
pub type Value {
  /// NULL
  Null
  /// Boolean
  Boolean(Bool)
  /// Integer (int2, int4, int8)
  Integer(Int)
  /// Float (float4, float8) - normal finite values
  Float(Float)
  /// Positive infinity (float4, float8, timestamp, timestamptz)
  PosInfinity
  /// Negative infinity (float4, float8, timestamp, timestamptz)
  NegInfinity
  /// NaN (float4, float8, numeric)
  NaN
  /// Text string (text, varchar, char, bpchar, name, citext, unknown, enum)
  Text(String)
  /// Raw bytes (bytea)
  Bytea(BitArray)
  /// UUID as 16-byte binary
  Uuid(BitArray)
  /// OID (oid, regclass, regtype, xid, cid, etc.)
  Oid(Int)
  /// Void (void type, no data)
  Void
  /// Array of values
  Array(List(Option(Value)))
  /// Date as days since 2000-01-01 (negative for dates before)
  Date(Int)
  /// Time as microseconds since midnight
  Time(Int)
  /// TimeTZ as microseconds since midnight + timezone offset in seconds
  TimeTz(microseconds: Int, tz_offset: Int)
  /// Timestamp as microseconds since 2000-01-01 00:00:00
  Timestamp(Int)
  /// Timestamptz as microseconds since 2000-01-01 00:00:00 UTC
  Timestamptz(Int)
  /// Interval: microseconds, days, months
  Interval(microseconds: Int, days: Int, months: Int)
  /// JSON string (already serialized)
  Json(String)
  /// JSONB string (already serialized)
  Jsonb(String)
  /// Numeric as string representation (preserving precision)
  Numeric(String)
  /// Point (x, y)
  Point(x: Float, y: Float)
  /// Inet/CIDR address: family (2=ipv4, 3=ipv6), address bytes, netmask
  Inet(family: Int, address: BitArray, netmask: Int)
  /// MAC address (6 bytes)
  Macaddr(BitArray)
  /// MAC address EUI-64 (8 bytes)
  Macaddr8(BitArray)
  /// Money amount in smallest currency unit (e.g., cents)
  Money(Int)
  /// XML document
  Xml(String)
  /// JSONPath expression
  Jsonpath(String)
  /// Bit string with explicit bit count (bit/varbit)
  BitString(bit_count: Int, data: BitArray)
  /// Line: Ax + By + C = 0
  Line(a: Float, b: Float, c: Float)
  /// Line segment: two endpoints
  Lseg(x1: Float, y1: Float, x2: Float, y2: Float)
  /// Box: upper-right and bottom-left corners
  Box(x1: Float, y1: Float, x2: Float, y2: Float)
  /// Path: closed/open flag + list of (x, y) points
  Path(closed: Bool, points: List(#(Float, Float)))
  /// Polygon: list of (x, y) vertices
  Polygon(vertices: List(#(Float, Float)))
  /// Circle: center (x, y) + radius
  Circle(x: Float, y: Float, radius: Float)
}

/// Parse a UUID string into a Uuid value.
/// Accepts formats: "550e8400-e29b-41d4-a716-446655440000"
/// or "550e8400e29b41d4a716446655440000" (with or without dashes).
pub fn uuid_from_string(s: String) -> Result(Value, Nil) {
  let bytes = bit_array.from_string(s)
  case bytes {
    // Hyphenated: 8-4-4-4-12 (36 chars)
    <<
      a1, a2, a3, a4, a5, a6, a7, a8, 0x2D,
      b1, b2, b3, b4, 0x2D,
      c1, c2, c3, c4, 0x2D,
      d1, d2, d3, d4, 0x2D,
      e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12,
    >> ->
      decode_hex_bytes(<<
        a1, a2, a3, a4, a5, a6, a7, a8,
        b1, b2, b3, b4,
        c1, c2, c3, c4,
        d1, d2, d3, d4,
        e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12,
      >>, <<>>)
    _ ->
      // Try without dashes (32 chars)
      case bit_array.byte_size(bytes) {
        32 -> decode_hex_bytes(bytes, <<>>)
        _ -> Error(Nil)
      }
  }
}

/// Format a Uuid value as a hyphenated string.
/// Returns "550e8400-e29b-41d4-a716-446655440000" format.
pub fn uuid_to_string(val: Value) -> Result(String, Nil) {
  case val {
    Uuid(<<a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p>>) -> {
      let result = <<
        hex_hi(a), hex_lo(a), hex_hi(b), hex_lo(b),
        hex_hi(c), hex_lo(c), hex_hi(d), hex_lo(d),
        0x2D,
        hex_hi(e), hex_lo(e), hex_hi(f), hex_lo(f),
        0x2D,
        hex_hi(g), hex_lo(g), hex_hi(h), hex_lo(h),
        0x2D,
        hex_hi(i), hex_lo(i), hex_hi(j), hex_lo(j),
        0x2D,
        hex_hi(k), hex_lo(k), hex_hi(l), hex_lo(l),
        hex_hi(m), hex_lo(m), hex_hi(n), hex_lo(n),
        hex_hi(o), hex_lo(o), hex_hi(p), hex_lo(p),
      >>
      case bit_array.to_string(result) {
        Ok(s) -> Ok(s)
        Error(_) -> Error(Nil)
      }
    }
    _ -> Error(Nil)
  }
}

fn hex_hi(b: Int) -> Int {
  hex_nibble(b / 16)
}

fn hex_lo(b: Int) -> Int {
  hex_nibble(b % 16)
}

fn hex_nibble(n: Int) -> Int {
  case n < 10 {
    True -> n + 0x30
    False -> n + 0x57
  }
}

fn decode_hex_bytes(
  input: BitArray,
  acc: BitArray,
) -> Result(Value, Nil) {
  case input {
    <<>> ->
      case bit_array.byte_size(acc) {
        16 -> Ok(Uuid(acc))
        _ -> Error(Nil)
      }
    <<hi, lo, rest:bytes>> ->
      case hex_byte(hi), hex_byte(lo) {
        Ok(h), Ok(l) -> decode_hex_bytes(rest, <<acc:bits, { h * 16 + l }>>)
        _, _ -> Error(Nil)
      }
    _ -> Error(Nil)
  }
}

fn hex_byte(b: Int) -> Result(Int, Nil) {
  case b {
    _ if b >= 0x30 && b <= 0x39 -> Ok(b - 0x30)
    _ if b >= 0x61 && b <= 0x66 -> Ok(b - 0x61 + 10)
    _ if b >= 0x41 && b <= 0x46 -> Ok(b - 0x41 + 10)
    _ -> Error(Nil)
  }
}

