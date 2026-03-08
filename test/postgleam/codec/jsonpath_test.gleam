import gleeunit/should
import postgleam/codec/jsonpath
import postgleam/value

pub fn jsonpath_encode_test() {
  // Version byte 0x01 prefix + UTF-8 string
  jsonpath.encode(value.Jsonpath("$.store"))
  |> should.equal(Ok(<<1, "$.store":utf8>>))
}

pub fn jsonpath_decode_test() {
  jsonpath.decode(<<1, "$.store":utf8>>)
  |> should.equal(Ok(value.Jsonpath("$.store")))
}

pub fn jsonpath_roundtrip_test() {
  let val = value.Jsonpath("$.items[*].price")
  let assert Ok(encoded) = jsonpath.encode(val)
  jsonpath.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn jsonpath_wrong_version_byte_test() {
  jsonpath.decode(<<2, "foo":utf8>>)
  |> should.be_error()
}

pub fn jsonpath_empty_path_test() {
  let val = value.Jsonpath("$")
  let assert Ok(encoded) = jsonpath.encode(val)
  jsonpath.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn jsonpath_wrong_type_test() {
  jsonpath.encode(value.Text("$.store"))
  |> should.be_error()
}

pub fn jsonpath_missing_version_test() {
  // Empty data - no version byte
  jsonpath.decode(<<>>)
  |> should.be_error()
}
