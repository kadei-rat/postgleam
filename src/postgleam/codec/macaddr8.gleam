/// PostgreSQL macaddr8 codec - binary format
/// Wire: 8 bytes (EUI-64)

import gleam/bit_array
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Macaddr8}

pub const oid = 774

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "macaddr8",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "macaddr8",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Macaddr8(data) ->
      case bit_array.byte_size(data) {
        8 -> Ok(data)
        _ -> Error("macaddr8 codec: expected 8-byte MAC address")
      }
    _ -> Error("macaddr8 codec: expected Macaddr8 value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case bit_array.byte_size(data) {
    8 -> Ok(Macaddr8(data))
    _ -> Error("macaddr8 codec: expected 8 bytes")
  }
}
