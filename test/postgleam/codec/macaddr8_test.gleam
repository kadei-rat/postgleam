import gleeunit/should
import postgleam/codec/macaddr8
import postgleam/value

pub fn macaddr8_encode_test() {
  let data = <<0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05>>
  macaddr8.encode(value.Macaddr8(data))
  |> should.equal(Ok(data))
}

pub fn macaddr8_decode_test() {
  let data = <<0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05>>
  macaddr8.decode(data)
  |> should.equal(Ok(value.Macaddr8(data)))
}

pub fn macaddr8_roundtrip_test() {
  let data = <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
  let val = value.Macaddr8(data)
  let assert Ok(encoded) = macaddr8.encode(val)
  macaddr8.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn macaddr8_wrong_size_test() {
  macaddr8.decode(<<1, 2, 3, 4, 5, 6>>)
  |> should.be_error()
}

pub fn macaddr8_wrong_type_test() {
  macaddr8.encode(value.Boolean(True))
  |> should.be_error()
}
