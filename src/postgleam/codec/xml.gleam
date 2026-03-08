/// PostgreSQL xml codec - binary format
/// Wire: UTF-8 bytes

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Xml}

pub const oid = 142

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "xml",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "xml",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Xml(s) -> Ok(bit_array.from_string(s))
    _ -> Error("xml codec: expected Xml value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case bit_array.to_string(data) {
    Ok(s) -> Ok(Xml(s))
    Error(_) -> Error("xml codec: invalid UTF-8")
  }
}
