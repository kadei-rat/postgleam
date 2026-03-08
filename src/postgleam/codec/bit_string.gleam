/// PostgreSQL bit/varbit codec - binary format
/// Wire: 4-byte uint32 bit_count + ceil(bit_count/8) bytes of data

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value}

pub const bit_oid = 1560

pub const varbit_oid = 1562

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "bit",
    oids: [bit_oid, varbit_oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "bit",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    value.BitString(bit_count, data) -> Ok(<<bit_count:32-big, data:bits>>)
    _ -> Error("bit codec: expected BitString value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<bit_count:32-big, rest:bits>> -> Ok(value.BitString(bit_count, rest))
    _ -> Error("bit codec: expected at least 4 bytes")
  }
}
