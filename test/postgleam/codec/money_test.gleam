import gleeunit/should
import postgleam/codec/money
import postgleam/value

pub fn money_encode_test() {
  // 12345 as int64 big-endian
  money.encode(value.Money(12345))
  |> should.equal(Ok(<<0, 0, 0, 0, 0, 0, 48, 57>>))
}

pub fn money_decode_test() {
  money.decode(<<0, 0, 0, 0, 0, 0, 48, 57>>)
  |> should.equal(Ok(value.Money(12345)))
}

pub fn money_zero_roundtrip_test() {
  let val = value.Money(0)
  let assert Ok(encoded) = money.encode(val)
  money.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn money_negative_encode_test() {
  // -100 as signed int64 big-endian
  let assert Ok(encoded) = money.encode(value.Money(-100))
  money.decode(encoded)
  |> should.equal(Ok(value.Money(-100)))
}

pub fn money_large_value_roundtrip_test() {
  let val = value.Money(999_999_999_999)
  let assert Ok(encoded) = money.encode(val)
  money.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn money_wrong_type_test() {
  money.encode(value.Integer(42))
  |> should.be_error()
}

pub fn money_wrong_size_test() {
  money.decode(<<0, 0, 0, 0>>)
  |> should.be_error()
}
