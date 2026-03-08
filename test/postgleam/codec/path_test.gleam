import gleeunit/should
import postgleam/codec/path
import postgleam/value

pub fn path_encode_closed_test() {
  // Closed path with 3 points: closed_byte=1, npts=3, then 3 points
  let assert Ok(encoded) =
    path.encode(
      value.Path(True, [#(0.0, 0.0), #(1.0, 0.0), #(0.0, 1.0)]),
    )
  should.equal(
    encoded,
    <<
      1, 0, 0, 0, 3,
      0.0:float-64-big, 0.0:float-64-big,
      1.0:float-64-big, 0.0:float-64-big,
      0.0:float-64-big, 1.0:float-64-big,
    >>,
  )
}

pub fn path_decode_closed_test() {
  path.decode(<<
    1, 0, 0, 0, 3,
    0.0:float-64-big, 0.0:float-64-big,
    1.0:float-64-big, 0.0:float-64-big,
    0.0:float-64-big, 1.0:float-64-big,
  >>)
  |> should.equal(
    Ok(value.Path(True, [#(0.0, 0.0), #(1.0, 0.0), #(0.0, 1.0)])),
  )
}

pub fn path_encode_open_test() {
  // Open path with 2 points: closed_byte=0
  let assert Ok(encoded) =
    path.encode(value.Path(False, [#(0.0, 0.0), #(1.0, 1.0)]))
  should.equal(
    encoded,
    <<
      0, 0, 0, 0, 2,
      0.0:float-64-big, 0.0:float-64-big,
      1.0:float-64-big, 1.0:float-64-big,
    >>,
  )
}

pub fn path_decode_open_test() {
  path.decode(<<
    0, 0, 0, 0, 2,
    0.0:float-64-big, 0.0:float-64-big,
    1.0:float-64-big, 1.0:float-64-big,
  >>)
  |> should.equal(
    Ok(value.Path(False, [#(0.0, 0.0), #(1.0, 1.0)])),
  )
}

pub fn path_empty_closed_test() {
  let val = value.Path(True, [])
  let assert Ok(encoded) = path.encode(val)
  should.equal(encoded, <<1, 0, 0, 0, 0>>)
  path.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn path_empty_open_test() {
  let val = value.Path(False, [])
  let assert Ok(encoded) = path.encode(val)
  should.equal(encoded, <<0, 0, 0, 0, 0>>)
  path.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn path_roundtrip_test() {
  let val =
    value.Path(True, [#(-1.5, 2.5), #(3.0, -4.0), #(0.0, 0.0)])
  let assert Ok(encoded) = path.encode(val)
  path.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn path_wrong_type_test() {
  path.encode(value.Polygon([#(0.0, 0.0)]))
  |> should.be_error()
}

pub fn path_truncated_data_test() {
  // Header says 2 points but only 1 point of data
  path.decode(<<1, 0, 0, 0, 2, 0.0:float-64-big, 0.0:float-64-big>>)
  |> should.be_error()
}
