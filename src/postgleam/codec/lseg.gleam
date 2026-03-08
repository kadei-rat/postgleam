/// PostgreSQL lseg (line segment) codec - binary format
/// Wire: 32 bytes = 4×float64 (two endpoints x1,y1 and x2,y2)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Lseg}

pub const oid = 601

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "lseg",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "lseg", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Lseg(x1, y1, x2, y2) ->
      Ok(<<x1:float-64-big, y1:float-64-big, x2:float-64-big, y2:float-64-big>>)
    _ -> Error("lseg codec: expected Lseg value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<x1:float-64-big, y1:float-64-big, x2:float-64-big, y2:float-64-big>> ->
      Ok(Lseg(x1, y1, x2, y2))
    _ -> Error("lseg codec: expected 32 bytes")
  }
}
