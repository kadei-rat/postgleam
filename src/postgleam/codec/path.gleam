/// PostgreSQL path codec - binary format
/// Wire: 1 byte (1=closed, 0=open) + 4 bytes npts (int32) + npts × 16 bytes (each point is 2×float64)

import gleam/list
import gleam/option
import postgleam/codec.{type Codec, type CodecMatcher, Binary, Codec, CodecMatcher}
import postgleam/value.{type Value, Path}

pub const oid = 602

pub fn matcher() -> CodecMatcher {
  CodecMatcher(
    type_name: "path",
    oids: [oid],
    send: option.None,
    format: Binary,
    build: build,
  )
}

fn build(type_oid: Int) -> Codec {
  Codec(type_name: "path", oid: type_oid, format: Binary, encode: encode, decode: decode)
}

pub fn encode(val: Value) -> Result(BitArray, String) {
  case val {
    Path(closed, points) -> {
      let closed_byte = case closed {
        True -> 1
        False -> 0
      }
      let npts = list.length(points)
      let header = <<closed_byte, npts:32-big>>
      let body = encode_points(points, <<>>)
      Ok(<<header:bits, body:bits>>)
    }
    _ -> Error("path codec: expected Path value")
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
    <<closed_byte, npts:32-big, rest:bits>> -> {
      let closed = case closed_byte {
        1 -> True
        _ -> False
      }
      case decode_points(rest, npts, []) {
        Ok(points) -> Ok(Path(closed, points))
        Error(e) -> Error(e)
      }
    }
    _ -> Error("path codec: invalid header")
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
        _ -> Error("path codec: insufficient point data")
      }
  }
}
