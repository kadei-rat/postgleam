import gleeunit/should
import postgleam/codec/bit_string
import postgleam/value

pub fn bit_string_encode_full_byte_test() {
  // 8 bits = 1 full byte
  bit_string.encode(value.BitString(8, <<0xFF>>))
  |> should.equal(Ok(<<0, 0, 0, 8, 0xFF>>))
}

pub fn bit_string_encode_partial_byte_test() {
  // 5 bits in one byte (top 5 bits set: 11111000 = 0xF8)
  bit_string.encode(value.BitString(5, <<0xF8>>))
  |> should.equal(Ok(<<0, 0, 0, 5, 0xF8>>))
}

pub fn bit_string_decode_full_byte_test() {
  bit_string.decode(<<0, 0, 0, 8, 0xFF>>)
  |> should.equal(Ok(value.BitString(8, <<0xFF>>)))
}

pub fn bit_string_decode_partial_byte_test() {
  bit_string.decode(<<0, 0, 0, 5, 0xF8>>)
  |> should.equal(Ok(value.BitString(5, <<0xF8>>)))
}

pub fn bit_string_roundtrip_full_byte_test() {
  let val = value.BitString(8, <<0xFF>>)
  let assert Ok(encoded) = bit_string.encode(val)
  bit_string.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn bit_string_roundtrip_multi_byte_test() {
  let val = value.BitString(16, <<0xAB, 0xCD>>)
  let assert Ok(encoded) = bit_string.encode(val)
  bit_string.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn bit_string_empty_test() {
  let val = value.BitString(0, <<>>)
  let assert Ok(encoded) = bit_string.encode(val)
  bit_string.decode(encoded)
  |> should.equal(Ok(val))
}

pub fn bit_string_wrong_type_test() {
  bit_string.encode(value.Bytea(<<0xFF>>))
  |> should.be_error()
}

pub fn bit_string_too_short_test() {
  // Less than 4 bytes - no room for bit_count header
  bit_string.decode(<<0, 0>>)
  |> should.be_error()
}
