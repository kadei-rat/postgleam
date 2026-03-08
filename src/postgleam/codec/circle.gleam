/// PostgreSQL circle codec - binary format
/// Wire: 24 bytes = center x (float64) + center y (float64) + radius (float64)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Circle}

pub const oid = 718

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "circle",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "circle",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Circle(x, y, radius) ->
      Ok(<<x:float-64-big, y:float-64-big, radius:float-64-big>>)
    _ -> Error("circle codec: expected Circle value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<x:float-64-big, y:float-64-big, radius:float-64-big>> ->
      Ok(Circle(x, y, radius))
    _ -> Error("circle codec: expected 24 bytes")
  }
}
