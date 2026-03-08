/// PostgreSQL polygon codec - binary format
/// Wire: 4 bytes npts (int32) + npts × 16 bytes (each vertex is 2×float64)

import gleam/list
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Polygon}

pub const oid = 604

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "polygon",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(
    type_name: "polygon",
    oid: type_oid,
    format: Binary,
    encode: encode,
    decode: decode,
  )
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Polygon(vertices) -> {
      let npts = list.length(vertices)
      let header = <<npts:32-big>>
      let body = encode_points(vertices, <<>>)
      Ok(<<header:bits, body:bits>>)
    }
    _ -> Error("polygon codec: expected Polygon value")
  }
}

fn encode_points(points: List(#(Float, Float)), acc: BitArray) -> BitArray {
  case points {
    [] -> acc
    [#(x, y), ..rest] ->
      encode_points(rest, <<acc:bits, x:float-64-big, y:float-64-big>>)
  }
}

pub fn decode(data: BitArray) -> Result(Value, String) {
  case data {
    <<npts:32-big, rest:bits>> -> {
      case decode_points(rest, npts, []) {
        Ok(vertices) -> Ok(Polygon(vertices))
        Error(e) -> Error(e)
      }
    }
    _ -> Error("polygon codec: invalid header")
  }
}

fn decode_points(
  data: BitArray,
  count: Int,
  acc: List(#(Float, Float)),
) -> Result(List(#(Float, Float)), String) {
  case count {
    0 -> Ok(list.reverse(acc))
    _ ->
      case data {
        <<x:float-64-big, y:float-64-big, rest:bits>> ->
          decode_points(rest, count - 1, [#(x, y), ..acc])
        _ -> Error("polygon codec: insufficient vertex data")
      }
  }
}
