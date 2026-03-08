/// Array codec tests
///
/// The PostgreSQL array binary wire format is:
///   <<ndim:32-big, has_null:32-big, elem_oid:32-big,
///     length_1:32-big, lower_bound_1:32-big, ...,   // per dimension
///     element_1_len:32-big, element_1_data:bytes,    // per element
///     ...>>
/// NULL elements are encoded as length = -1 (<<255, 255, 255, 255>>).
///
/// These tests build an int4 array codec via build_array_codec and exercise
/// the encode/decode through the Codec record's function fields.

import gleam/option.{None, Some}
import gleeunit/should
import postgleam/codec
import postgleam/codec/array
import postgleam/codec/int4
import postgleam/value

/// Helper: build an int4 array codec (int4[] OID = 1007, elem OID = 23)
fn int4_array_codec() -> codec.Codec {
  let elem_codec =
    codec.Codec(
      type_name: "int4",
      oid: 23,
      format: codec.Binary,
      encode: int4.encode,
      decode: int4.decode,
    )
  array.build_array_codec(elem_codec, 1007)
}

/// Empty array encode: ndim=0, has_null=0, elem_oid=23
pub fn empty_array_encode_test() {
  let c = int4_array_codec()
  let assert Ok(encoded) = c.encode(value.Array([]))
  should.equal(encoded, <<0:32-big, 0:32-big, 23:32-big>>)
}

/// Empty array decode
pub fn empty_array_decode_test() {
  let c = int4_array_codec()
  let wire = <<0:32-big, 0:32-big, 23:32-big>>
  c.decode(wire)
  |> should.equal(Ok(value.Array([])))
}

/// Single element [42]:
/// ndim=1, has_null=0, elem_oid=23, length=1, lower_bound=1,
/// elem_len=4, elem=42:32-big
pub fn single_element_encode_test() {
  let c = int4_array_codec()
  let assert Ok(encoded) = c.encode(value.Array([Some(value.Integer(42))]))
  should.equal(
    encoded,
    <<
      1:32-big, 0:32-big, 23:32-big,
      1:32-big, 1:32-big,
      4:32-big, 42:32-big,
    >>,
  )
}

/// Single element decode [42]
pub fn single_element_decode_test() {
  let c = int4_array_codec()
  let wire = <<
    1:32-big, 0:32-big, 23:32-big,
    1:32-big, 1:32-big,
    4:32-big, 42:32-big,
  >>
  c.decode(wire)
  |> should.equal(Ok(value.Array([Some(value.Integer(42))])))
}

/// Multiple elements [1, 2, 3]
pub fn multi_element_roundtrip_test() {
  let c = int4_array_codec()
  let val =
    value.Array([
      Some(value.Integer(1)),
      Some(value.Integer(2)),
      Some(value.Integer(3)),
    ])
  let assert Ok(encoded) = c.encode(val)
  c.decode(encoded)
  |> should.equal(Ok(val))
}

/// Array with NULL: [NULL, 1]
pub fn array_with_null_encode_test() {
  let c = int4_array_codec()
  let val = value.Array([None, Some(value.Integer(1))])
  let assert Ok(encoded) = c.encode(val)
  // ndim=1, has_null=1, elem_oid=23, length=2, lower_bound=1,
  // null (-1 as int32), elem_len=4, 1:32-big
  should.equal(
    encoded,
    <<
      1:32-big, 1:32-big, 23:32-big,
      2:32-big, 1:32-big,
      255, 255, 255, 255,
      4:32-big, 1:32-big,
    >>,
  )
}

/// Array with NULL decode
pub fn array_with_null_decode_test() {
  let c = int4_array_codec()
  let wire = <<
    1:32-big, 1:32-big, 23:32-big,
    2:32-big, 1:32-big,
    255, 255, 255, 255,
    4:32-big, 1:32-big,
  >>
  c.decode(wire)
  |> should.equal(Ok(value.Array([None, Some(value.Integer(1))])))
}

/// Empty array roundtrip
pub fn empty_array_roundtrip_test() {
  let c = int4_array_codec()
  let val = value.Array([])
  let assert Ok(encoded) = c.encode(val)
  c.decode(encoded)
  |> should.equal(Ok(val))
}

/// Wrong type for encode (not an Array)
pub fn array_wrong_type_test() {
  let c = int4_array_codec()
  c.encode(value.Integer(42))
  |> should.be_error()
}

/// Truncated wire data should error
pub fn array_truncated_test() {
  let c = int4_array_codec()
  c.decode(<<0, 0>>)
  |> should.be_error()
}

/// Negative int4 values in array
pub fn array_negative_values_test() {
  let c = int4_array_codec()
  let val =
    value.Array([Some(value.Integer(-1)), Some(value.Integer(-100))])
  let assert Ok(encoded) = c.encode(val)
  c.decode(encoded)
  |> should.equal(Ok(val))
}
