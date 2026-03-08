/// PostgreSQL money codec - binary format
/// Wire: 8 bytes signed int64 big-endian

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Money}

pub const oid = 790

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "money",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "money",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Money(n) -> Ok(<<n:64-big>>)
    _ -> Error("money codec: expected Money value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<n:64-big-signed>> -> Ok(Money(n))
    _ -> Error("money codec: expected 8 bytes")
  }
}
