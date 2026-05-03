/// PostgreSQL numeric codec - binary format
/// Wire: variable length - ndigits(int16) + weight(int16) + sign(uint16) + scale(int16) + digits(uint16[])
/// Special: NaN = sign 0xC000, +Inf = sign 0xD000, -Inf = sign 0xF000
///
/// We represent numeric as a string to preserve arbitrary precision.

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, NaN, NegInfinity, Numeric, PosInfinity}

pub const oid = 1700

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "numeric",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "numeric", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    NaN -> Ok(<<0:16, 0:16, 0xC000:16, 0:16>>)
    PosInfinity -> Ok(<<0:16, 0:16, 0xD000:16, 0:16>>)
    NegInfinity -> Ok(<<0:16, 0:16, 0xF000:16, 0:16>>)
    Numeric(s) -> encode_numeric_string(s)
    _ -> Error("numeric codec: expected Numeric, NaN, PosInfinity, or NegInfinity")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<_ndigits:16, _weight:16-signed, 0xC000:16, _scale:16>> -> Ok(NaN)
    <<_ndigits:16, _weight:16-signed, 0xD000:16, _scale:16>> -> Ok(PosInfinity)
    <<_ndigits:16, _weight:16-signed, 0xF000:16, _scale:16>> -> Ok(NegInfinity)
    <<ndigits:16, weight:16-signed, sign:16, scale:16, rest:bits>> ->
      decode_numeric(ndigits, weight, sign, scale, rest)
    _ -> Error("numeric codec: invalid data")
  }
}

fn decode_numeric(
  ndigits: Int,
  weight: Int,
  sign: Int,
  scale: Int,
  data: BitArray,
) -> Result(Value, String) {
  let digits = read_digits(data, ndigits, [])
  let str = digits_to_string(digits, weight, scale, sign)
  Ok(Numeric(str))
}

fn read_digits(data: BitArray, count: Int, acc: List(Int)) -> List(Int) {
  case count {
    0 -> list_reverse(acc)
    _ ->
      case data {
        <<d:16, rest:bits>> -> read_digits(rest, count - 1, [d, ..acc])
        _ -> list_reverse(acc)
      }
  }
}

fn digits_to_string(
  digits: List(Int),
  weight: Int,
  scale: Int,
  sign: Int,
) -> String {
  let prefix = case sign {
    0x4000 -> "-"
    _ -> ""
  }
  case digits {
    [] ->
      case scale > 0 {
        True -> prefix <> "0." <> repeat_char("0", scale)
        False -> prefix <> "0"
      }
    _ -> {
      // Build integer and fractional parts
      let int_groups = weight + 1
      let #(int_digits, frac_digits) = split_at(digits, int_groups)
      let int_str = int_digits_to_string(int_digits, True)
      let int_str = case int_str {
        "" -> "0"
        s -> s
      }
      case scale > 0 {
        True -> {
          let frac_str = frac_digits_to_string(frac_digits, scale)
          prefix <> int_str <> "." <> frac_str
        }
        False -> prefix <> int_str
      }
    }
  }
}

fn int_digits_to_string(digits: List(Int), is_first: Bool) -> String {
  int_digits_to_bytes(digits, is_first, <<>>)
}

fn int_digits_to_bytes(
  digits: List(Int),
  is_first: Bool,
  acc: BitArray,
) -> String {
  case digits {
    [] ->
      case bit_array.to_string(acc) {
        Ok(s) -> s
        Error(_) -> ""
      }
    [d, ..rest] -> {
      let s = case is_first {
        True -> int_to_string(d)
        False -> pad_digit(d)
      }
      int_digits_to_bytes(rest, False, <<acc:bits, s:utf8>>)
    }
  }
}

fn frac_digits_to_string(digits: List(Int), scale: Int) -> String {
  let raw = frac_raw(digits)
  // Pad or truncate to exact scale
  let len = string_length(raw)
  case len >= scale {
    True -> string_slice(raw, 0, scale)
    False -> raw <> repeat_char("0", scale - len)
  }
}

fn frac_raw(digits: List(Int)) -> String {
  frac_raw_bytes(digits, <<>>)
}

fn frac_raw_bytes(digits: List(Int), acc: BitArray) -> String {
  case digits {
    [] ->
      case bit_array.to_string(acc) {
        Ok(s) -> s
        Error(_) -> ""
      }
    [d, ..rest] -> {
      let s = pad_digit(d)
      frac_raw_bytes(rest, <<acc:bits, s:utf8>>)
    }
  }
}

fn pad_digit(d: Int) -> String {
  let s = int_to_string(d)
  let len = string_length(s)
  case len {
    1 -> "000" <> s
    2 -> "00" <> s
    3 -> "0" <> s
    _ -> s
  }
}

fn split_at(l: List(a), n: Int) -> #(List(a), List(a)) {
  split_at_loop(l, n, [])
}

fn split_at_loop(l: List(a), n: Int, acc: List(a)) -> #(List(a), List(a)) {
  case n <= 0 {
    True -> #(list_reverse(acc), l)
    False ->
      case l {
        [] -> #(list_reverse(acc), [])
        [x, ..rest] -> split_at_loop(rest, n - 1, [x, ..acc])
      }
  }
}

fn list_reverse(l: List(a)) -> List(a) {
  list_reverse_loop(l, [])
}

fn list_reverse_loop(l: List(a), acc: List(a)) -> List(a) {
  case l {
    [] -> acc
    [x, ..rest] -> list_reverse_loop(rest, [x, ..acc])
  }
}

/// Encode a numeric string to PostgreSQL binary format
fn encode_numeric_string(s: String) -> Result(BitArray, String) {
  // Use the Erlang FFI to encode - this is complex enough to warrant it
  Ok(encode_numeric_ffi(s))
}

@external(erlang, "postgleam_ffi", "encode_numeric")
fn encode_numeric_ffi(s: String) -> BitArray

fn repeat_char(c: String, n: Int) -> String {
  case n <= 0 {
    True -> ""
    False -> string_repeat(c, n)
  }
}

@external(erlang, "erlang", "integer_to_binary")
fn int_to_string(n: Int) -> String

@external(erlang, "string", "length")
fn string_length(s: String) -> Int

@external(erlang, "binary", "part")
fn string_slice(s: String, start: Int, len: Int) -> String

@external(erlang, "binary", "copy")
fn string_repeat(s: String, n: Int) -> String
