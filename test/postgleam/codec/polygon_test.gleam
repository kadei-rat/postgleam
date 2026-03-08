import gleeunit/should
import postgleam/codec/polygon
import postgleam/value

pub fn polygon_encode_test() {
  // Triangle: 3 vertices
  let assert Ok(encoded) =
    polygon.encode(
      value.Polygon([#(0.0, 0.0), #(1.0, 0.0), #(0.0, 1.0)]),
    )
  should.equal(
    encoded,
    <<
      0, 0, 0, 3,
      0.0:float-64-big, 0.0:float-64-big,
      1.0:float-64-big, 0.0:float-64-big,
      0.0:float-64-big, 1.0:float-64-big,
    >>,
  )
}

pub fn polygon_decode_test() {
  polygon.decode(<<
    0, 0, 0, 3,
    0.0:float-64-big, 0.0:float-64-big,
    1.0:float-64-big, 0.0:float-64-big,
    0.0:float-64-big, 1.0:float-64-big,
  >>)
  |> should.equal(
    Ok(value.Polygon([#(0.0, 0.0), #(1.0, 0.0), #(0.0, 1.0)])),
  )
}

pub fn polygon_empty_test() {
  let val = value.Polygon([])
  let assert Ok(encoded) = polygon.encode(val)
  should.equal(encoded, <<0, 0, 0, 0>>)
  polygon.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn polygon_triangle_roundtrip_test() {
  let val =
    value.Polygon([#(1.0, 2.0), #(3.0, 4.0), #(5.0, 6.0)])
  let assert Ok(encoded) = polygon.encode(val)
  polygon.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn polygon_negative_coords_test() {
  let val =
    value.Polygon([#(-1.0, -2.0), #(3.5, -4.5), #(0.0, 7.25)])
  let assert Ok(encoded) = polygon.encode(val)
  polygon.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn polygon_wrong_type_test() {
  polygon.encode(value.Path(True, [#(0.0, 0.0)]))
  |> should.be_error()
}

pub fn polygon_truncated_data_test() {
  // Header says 2 vertices but only 1 vertex of data
  polygon.decode(<<0, 0, 0, 2, 0.0:float-64-big, 0.0:float-64-big>>)
  |> should.be_error()
}
