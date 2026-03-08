import gleeunit/should
import postgleam/codec/box
import postgleam/value

pub fn box_encode_test() {
  let assert Ok(encoded) = box.encode(value.Box(1.0, 1.0, 0.0, 0.0))
  should.equal(
    encoded,
    <<1.0:float-64-big, 1.0:float-64-big, 0.0:float-64-big, 0.0:float-64-big>>,
  )
}

pub fn box_decode_test() {
  box.decode(<<
    1.0:float-64-big, 1.0:float-64-big, 0.0:float-64-big, 0.0:float-64-big,
  >>)
  |> should.equal(Ok(value.Box(1.0, 1.0, 0.0, 0.0)))
}

pub fn box_roundtrip_test() {
  let val = value.Box(10.5, 20.25, -5.0, -10.0)
  let assert Ok(encoded) = box.encode(val)
  box.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn box_zero_area_test() {
  // Degenerate box: same upper-right and bottom-left
  let val = value.Box(3.0, 3.0, 3.0, 3.0)
  let assert Ok(encoded) = box.encode(val)
  box.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn box_wrong_type_test() {
  box.encode(value.Lseg(0.0, 0.0, 1.0, 1.0))
  |> should.be_error()
}

pub fn box_wrong_size_test() {
  box.decode(<<1.0:float-64-big>>)
  |> should.be_error()
}
