import gleeunit/should
import postgleam/codec/lseg
import postgleam/value

pub fn lseg_encode_test() {
  let assert Ok(encoded) = lseg.encode(value.Lseg(0.0, 0.0, 1.0, 1.0))
  should.equal(
    encoded,
    <<0.0:float-64-big, 0.0:float-64-big, 1.0:float-64-big, 1.0:float-64-big>>,
  )
}

pub fn lseg_decode_test() {
  lseg.decode(<<
    0.0:float-64-big, 0.0:float-64-big, 1.0:float-64-big, 1.0:float-64-big,
  >>)
  |> should.equal(Ok(value.Lseg(0.0, 0.0, 1.0, 1.0)))
}

pub fn lseg_roundtrip_test() {
  let val = value.Lseg(-3.5, 7.25, 10.0, -2.0)
  let assert Ok(encoded) = lseg.encode(val)
  lseg.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn lseg_zero_length_test() {
  // Degenerate segment: both endpoints the same
  let val = value.Lseg(5.0, 5.0, 5.0, 5.0)
  let assert Ok(encoded) = lseg.encode(val)
  lseg.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn lseg_wrong_type_test() {
  lseg.encode(value.Point(0.0, 0.0))
  |> should.be_error()
}

pub fn lseg_wrong_size_test() {
  lseg.decode(<<1.0:float-64-big, 2.0:float-64-big>>)
  |> should.be_error()
}
