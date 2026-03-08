/// PostgreSQL jsonpath codec - binary format
/// Wire: 1 byte version (0x01) + UTF-8 string

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Jsonpath}

pub const oid = 4072

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "jsonpath",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "jsonpath",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Jsonpath(s) -> Ok(<<1, s:utf8>>)
    _ -> Error("jsonpath codec: expected Jsonpath value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<1, rest:bits>> ->
      case bit_array.to_string(rest) {
        Ok(s) -> Ok(Jsonpath(s))
        Error(_) -> Error("jsonpath codec: invalid UTF-8")
      }
    _ -> Error("jsonpath codec: missing version byte or invalid data")
  }
}
