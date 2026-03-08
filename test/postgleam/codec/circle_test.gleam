import gleeunit/should
import postgleam/codec/circle
import postgleam/value

pub fn circle_encode_test() {
  let assert Ok(encoded) = circle.encode(value.Circle(1.0, 2.0, 3.0))
  should.equal(
    encoded,
    <<1.0:float-64-big, 2.0:float-64-big, 3.0:float-64-big>>,
  )
}

pub fn circle_decode_test() {
  circle.decode(<<1.0:float-64-big, 2.0:float-64-big, 3.0:float-64-big>>)
  |> should.equal(Ok(value.Circle(1.0, 2.0, 3.0)))
}

pub fn circle_roundtrip_test() {
  let val = value.Circle(-5.5, 10.25, 7.0)
  let assert Ok(encoded) = circle.encode(val)
  circle.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn circle_zero_radius_test() {
  let val = value.Circle(0.0, 0.0, 0.0)
  let assert Ok(encoded) = circle.encode(val)
  circle.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn circle_wrong_type_test() {
  circle.encode(value.Point(1.0, 2.0))
  |> should.be_error()
}

pub fn circle_wrong_size_test() {
  circle.decode(<<1.0:float-64-big, 2.0:float-64-big>>)
  |> should.be_error()
}
