/// PostgreSQL line codec - binary format
/// Wire: 24 bytes = 3×float64 (A, B, C coefficients of line Ax + By + C = 0)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Line}

pub const oid = 628

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "line",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "line", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Line(a, b, c) -> Ok(<<a:float-64-big, b:float-64-big, c:float-64-big>>)
    _ -> Error("line codec: expected Line value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<a:float-64-big, b:float-64-big, c:float-64-big>> -> Ok(Line(a, b, c))
    _ -> Error("line codec: expected 24 bytes")
  }
}
