import gleeunit/should
import postgleam/codec/numeric
import postgleam/value

pub fn numeric_decode_nan_test() {
  numeric.decode(<<0:16, 0:16, 0xC000:16, 0:16>>)
  |> should.equal(Ok(value.NaN))
}

pub fn numeric_decode_pos_infinity_test() {
  numeric.decode(<<0:16, 0:16, 0xD000:16, 0:16>>)
  |> should.equal(Ok(value.PosInfinity))
}

pub fn numeric_decode_neg_infinity_test() {
  numeric.decode(<<0:16, 0:16, 0xF000:16, 0:16>>)
  |> should.equal(Ok(value.NegInfinity))
}

pub fn numeric_encode_nan_test() {
  numeric.encode(value.NaN)
  |> should.equal(Ok(<<0:16, 0:16, 0xC000:16, 0:16>>))
}

pub fn numeric_encode_pos_infinity_test() {
  numeric.encode(value.PosInfinity)
  |> should.equal(Ok(<<0:16, 0:16, 0xD000:16, 0:16>>))
}

pub fn numeric_decode_zero_test() {
  // 0 digits, weight 0, sign positive, scale 0
  let assert Ok(value.Numeric(s)) =
    numeric.decode(<<0:16, 0:16, 0:16, 0:16>>)
  should.equal(s, "0")
}

pub fn numeric_roundtrip_simple_test() {
  let assert Ok(encoded) = numeric.encode(value.Numeric("42"))
  let assert Ok(value.Numeric(s)) = numeric.decode(encoded)
  should.equal(s, "42")
}

pub fn numeric_roundtrip_decimal_test() {
  let assert Ok(encoded) = numeric.encode(value.Numeric("3.14"))
  let assert Ok(value.Numeric(s)) = numeric.decode(encoded)
  should.equal(s, "3.14")
}

pub fn numeric_roundtrip_negative_test() {
  let assert Ok(encoded) = numeric.encode(value.Numeric("-99.99"))
  let assert Ok(value.Numeric(s)) = numeric.decode(encoded)
  should.equal(s, "-99.99")
}

pub fn numeric_roundtrip_large_test() {
  let assert Ok(encoded) = numeric.encode(value.Numeric("123456789"))
  let assert Ok(value.Numeric(s)) = numeric.decode(encoded)
  should.equal(s, "123456789")
}

pub fn numeric_wrong_type_test() {
  numeric.encode(value.Integer(1))
  |> should.be_error()
}

pub fn numeric_roundtrip_trailing_zero_fractional_test() {
  let assert Ok(encoded) = numeric.encode(value.Numeric("5.0"))
  let assert Ok(value.Numeric(s)) = numeric.decode(encoded)
  should.equal(s, "5.0")
}

pub fn numeric_roundtrip_zero_with_scale_test() {
  let assert Ok(encoded) = numeric.encode(value.Numeric("0.00"))
  let assert Ok(value.Numeric(s)) = numeric.decode(encoded)
  should.equal(s, "0.00")
}
