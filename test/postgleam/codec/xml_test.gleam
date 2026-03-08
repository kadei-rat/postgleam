import gleam/bit_array
import gleeunit/should
import postgleam/codec/xml
import postgleam/value

pub fn xml_encode_test() {
  xml.encode(value.Xml("<root/>"))
  |> should.equal(Ok(bit_array.from_string("<root/>")))
}

pub fn xml_decode_test() {
  let data = bit_array.from_string("<root/>")
  xml.decode(data)
  |> should.equal(Ok(value.Xml("<root/>")))
}

pub fn xml_roundtrip_test() {
  let doc = "<person><name>Alice</name></person>"
  let val = value.Xml(doc)
  let assert Ok(encoded) = xml.encode(val)
  xml.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn xml_empty_string_test() {
  let val = value.Xml("")
  let assert Ok(encoded) = xml.encode(val)
  xml.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn xml_wrong_type_test() {
  xml.encode(value.Text("hello"))
  |> should.be_error()
}
