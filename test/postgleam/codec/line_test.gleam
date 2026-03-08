import gleeunit/should
import postgleam/codec/line
import postgleam/value

pub fn line_encode_test() {
  let assert Ok(encoded) = line.encode(value.Line(1.0, 2.0, 3.0))
  should.equal(encoded, <<1.0:float-64-big, 2.0:float-64-big, 3.0:float-64-big>>)
}

pub fn line_decode_test() {
  line.decode(<<1.0:float-64-big, 2.0:float-64-big, 3.0:float-64-big>>)
  |> should.equal(Ok(value.Line(1.0, 2.0, 3.0)))
}

pub fn line_roundtrip_negative_test() {
  let val = value.Line(-1.5, 0.0, 4.25)
  let assert Ok(encoded) = line.encode(val)
  line.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn line_zero_coefficients_test() {
  let val = value.Line(0.0, 0.0, 0.0)
  let assert Ok(encoded) = line.encode(val)
  line.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn line_wrong_type_test() {
  line.encode(value.Integer(1))
  |> should.be_error()
}

pub fn line_wrong_size_test() {
  line.decode(<<1.0:float-64-big, 2.0:float-64-big>>)
  |> should.be_error()
}
