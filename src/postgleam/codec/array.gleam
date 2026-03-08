/// PostgreSQL array codec - binary format
/// Wire: ndim(int32) + has_null(int32) + elem_oid(int32)
///       + per-dim: length(int32) + lower_bound(int32)
///       + per-element: -1(int32) for NULL | byte_len(int32) + data
///
/// Only supports 1-dimensional arrays.

import gleam/bit_array
import gleam/list
import gleam/option.{type Option, None, Some}
import postgleam/codec.{type Codec, Binary, Codec}
import postgleam/codec/registry.{type Registry}
import postgleam/value.{type Value, Array}

/// Build an array codec that wraps an element codec.
/// The element codec is captured in closures for encode/decode.
pub fn build_array_codec(element_codec: Codec, array_oid: Int) -> Codec {
  let elem_oid = element_codec.oid
  let elem_encode = element_codec.encode
  let elem_decode = element_codec.decode
  Codec(
    type_name: "array",
    oid: array_oid,
    format: Binary,
    encode: fn(val) { encode_array(val, elem_oid, elem_encode) },
    decode: fn(data) { decode_array(data, elem_decode) },
  )
}

/// Hardcoded array OID -> element OID mapping for common types
pub fn array_oid_map() -> List(#(Int, Int)) {
  [
    #(1000, 16),     // bool[]
    #(1005, 21),     // int2[]
    #(1007, 23),     // int4[]
    #(1016, 20),     // int8[]
    #(1021, 700),    // float4[]
    #(1022, 701),    // float8[]
    #(1009, 25),     // text[]
    #(1015, 1043),   // varchar[]
    #(1001, 17),     // bytea[]
    #(2951, 2950),   // uuid[]
    #(199, 114),     // json[]
    #(3807, 3802),   // jsonb[]
    #(1231, 1700),   // numeric[]
    #(1182, 1082),   // date[]
    #(1183, 1083),   // time[]
    #(1270, 1266),   // timetz[]
    #(1115, 1114),   // timestamp[]
    #(1185, 1184),   // timestamptz[]
    #(1187, 1186),   // interval[]
    #(1028, 26),     // oid[]
    #(1003, 19),     // name[]
    #(1041, 869),    // inet[]
    #(1040, 829),    // macaddr[]
    #(1017, 600),    // point[]
  ]
}

/// Register array codecs into an existing registry.
/// Looks up each element codec from the base registry, builds an array codec.
pub fn register_arrays(base: Registry) -> Registry {
  register_arrays_loop(array_oid_map(), base)
}

fn register_arrays_loop(
  mappings: List(#(Int, Int)),
  reg: Registry,
) -> Registry {
  case mappings {
    [] -> reg
    [#(array_oid, elem_oid), ..rest] -> {
      let reg = case registry.lookup(reg, elem_oid) {
        Ok(elem_codec) -> {
          let array_codec = build_array_codec(elem_codec, array_oid)
          insert_codec(array_oid, array_codec, reg)
        }
        Error(_) -> reg
      }
      register_arrays_loop(rest, reg)
    }
  }
}

// --- Encode ---

fn encode_array(
  val: Value,
  elem_oid: Int,
  elem_encode: fn(Value) -> Result(BitArray, String),
) -> Result(BitArray, String) {
  case val {
    Array(elements) -> {
      let len = list.length(elements)
      case len {
        0 ->
          // Empty array: ndim=0, has_null=0, elem_oid
          Ok(<<0:32-big, 0:32-big, elem_oid:32-big>>)
        _ -> {
          let has_null = case list.any(elements, fn(e) { e == None }) {
            True -> 1
            False -> 0
          }
          // Header: ndim=1, has_null, elem_oid, length, lower_bound=1
          let header = <<
            1:32-big, has_null:32-big, elem_oid:32-big,
            len:32-big, 1:32-big,
          >>
          case encode_elements(elements, elem_encode, <<>>) {
            Ok(body) -> Ok(<<header:bits, body:bits>>)
            Error(e) -> Error(e)
          }
        }
      }
    }
    _ -> Error("array codec: expected Array value")
  }
}

fn encode_elements(
  elements: List(Option(Value)),
  elem_encode: fn(Value) -> Result(BitArray, String),
  acc: BitArray,
) -> Result(BitArray, String) {
  case elements {
    [] -> Ok(acc)
    [None, ..rest] ->
      encode_elements(rest, elem_encode, <<acc:bits, 255, 255, 255, 255>>)
    [Some(val), ..rest] ->
      case elem_encode(val) {
        Ok(encoded) -> {
          let len = bit_array.byte_size(encoded)
          encode_elements(
            rest,
            elem_encode,
            <<acc:bits, len:32-big, encoded:bits>>,
          )
        }
        Error(e) -> Error("array codec: element encode failed: " <> e)
      }
  }
}

// --- Decode ---

fn decode_array(
  data: BitArray,
  elem_decode: fn(BitArray) -> Result(Value, String),
) -> Result(Value, String) {
  case data {
    // Empty array (ndim=0)
    <<0:32-big, _has_null:32-big, _elem_oid:32-big>> -> Ok(Array([]))
    // 1-dimensional array
    <<1:32-big, _has_null:32-big, _elem_oid:32-big,
      length:32-big, _lower_bound:32-big, rest:bits>> ->
      case decode_elements(rest, length, elem_decode, []) {
        Ok(elements) -> Ok(Array(list.reverse(elements)))
        Error(e) -> Error(e)
      }
    _ -> Error("array codec: unsupported array format")
  }
}

fn decode_elements(
  data: BitArray,
  remaining: Int,
  elem_decode: fn(BitArray) -> Result(Value, String),
  acc: List(Option(Value)),
) -> Result(List(Option(Value)), String) {
  case remaining {
    0 -> Ok(acc)
    _ ->
      case data {
        // NULL element
        <<-1:32-big-signed, rest:bits>> ->
          decode_elements(rest, remaining - 1, elem_decode, [None, ..acc])
        // Non-NULL element
        <<len:32-big, elem_data:bytes-size(len), rest:bits>> ->
          case elem_decode(elem_data) {
            Ok(val) ->
              decode_elements(rest, remaining - 1, elem_decode, [
                Some(val),
                ..acc
              ])
            Error(e) ->
              Error("array codec: element decode failed: " <> e)
          }
        _ -> Error("array codec: unexpected end of array data")
      }
  }
}

// --- Dict insert helper (avoids importing gleam/dict directly) ---

@external(erlang, "maps", "put")
fn insert_codec(key: Int, codec: Codec, reg: Registry) -> Registry
