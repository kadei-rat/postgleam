/// PostgreSQL box codec - binary format
/// Wire: 32 bytes = 4×float64 (upper-right x1,y1 then bottom-left x2,y2)

import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Box}

pub const oid = 603

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "box",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "box", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Box(x1, y1, x2, y2) ->
      Ok(<<x1:float-64-big, y1:float-64-big, x2:float-64-big, y2:float-64-big>>)
    _ -> Error("box codec: expected Box value")
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<x1:float-64-big, y1:float-64-big, x2:float-64-big, y2:float-64-big>> ->
      Ok(Box(x1, y1, x2, y2))
    _ -> Error("box codec: expected 32 bytes")
  }
}
