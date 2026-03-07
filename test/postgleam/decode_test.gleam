import gleam/option.{None, Some}
import gleeunit/should
import postgleam/decode
import postgleam/value

// =============================================================================
// Value decoders
// =============================================================================

pub fn decode_int_test() {
  decode.int(Some(value.Integer(42)))
  |> should.equal(Ok(42))
}

pub fn decode_int_null_error_test() {
  decode.int(None)
  |> should.be_error()
}

pub fn decode_int_wrong_type_error_test() {
  decode.int(Some(value.Text("hello")))
  |> should.be_error()
}

pub fn decode_text_test() {
  decode.text(Some(value.Text("hello")))
  |> should.equal(Ok("hello"))
}

pub fn decode_text_null_error_test() {
  decode.text(None)
  |> should.be_error()
}

pub fn decode_text_wrong_type_error_test() {
  decode.text(Some(value.Integer(42)))
  |> should.be_error()
}

pub fn decode_bool_test() {
  decode.bool(Some(value.Boolean(True)))
  |> should.equal(Ok(True))

  decode.bool(Some(value.Boolean(False)))
  |> should.equal(Ok(False))
}

pub fn decode_bool_null_error_test() {
  decode.bool(None)
  |> should.be_error()
}

pub fn decode_float_test() {
  decode.float(Some(value.Float(3.14)))
  |> should.equal(Ok(3.14))
}

pub fn decode_float_null_error_test() {
  decode.float(None)
  |> should.be_error()
}

pub fn decode_bytea_test() {
  decode.bytea(Some(value.Bytea(<<1, 2, 3>>)))
  |> should.equal(Ok(<<1, 2, 3>>))
}

pub fn decode_uuid_test() {
  let bytes = <<0xA0, 0xEE, 0xBC, 0x99, 0x9C, 0x0B, 0x4E, 0xF8, 0xBB, 0x6D,
    0x6B, 0xB9, 0xBD, 0x38, 0x0A, 0x11>>
  decode.uuid(Some(value.Uuid(bytes)))
  |> should.equal(Ok(bytes))
}

pub fn decode_json_test() {
  decode.json(Some(value.Json("{\"a\":1}")))
  |> should.equal(Ok("{\"a\":1}"))
}

pub fn decode_jsonb_test() {
  decode.jsonb(Some(value.Jsonb("{\"b\":2}")))
  |> should.equal(Ok("{\"b\":2}"))
}

pub fn decode_numeric_test() {
  decode.numeric(Some(value.Numeric("123.456")))
  |> should.equal(Ok("123.456"))
}

pub fn decode_date_test() {
  decode.date(Some(value.Date(8780)))
  |> should.equal(Ok(8780))
}

pub fn decode_timestamp_test() {
  decode.timestamp(Some(value.Timestamp(1_000_000)))
  |> should.equal(Ok(1_000_000))
}

pub fn decode_timestamptz_test() {
  decode.timestamptz(Some(value.Timestamptz(2_000_000)))
  |> should.equal(Ok(2_000_000))
}

// =============================================================================
// Optional decoder
// =============================================================================

pub fn decode_optional_present_test() {
  decode.optional(decode.int)(Some(value.Integer(42)))
  |> should.equal(Ok(Some(42)))
}

pub fn decode_optional_null_test() {
  decode.optional(decode.int)(None)
  |> should.equal(Ok(None))
}

pub fn decode_optional_wrong_type_error_test() {
  decode.optional(decode.int)(Some(value.Text("hello")))
  |> should.be_error()
}

// =============================================================================
// Row decoder composition
// =============================================================================

pub fn decode_single_element_test() {
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  decode.run(decoder, [Some(value.Integer(42))])
  |> should.equal(Ok(42))
}

pub fn decode_two_elements_test() {
  let decoder = {
    use id <- decode.element(0, decode.int)
    use name <- decode.element(1, decode.text)
    decode.success(#(id, name))
  }

  decode.run(decoder, [Some(value.Integer(1)), Some(value.Text("alice"))])
  |> should.equal(Ok(#(1, "alice")))
}

pub fn decode_three_elements_test() {
  let decoder = {
    use id <- decode.element(0, decode.int)
    use name <- decode.element(1, decode.text)
    use active <- decode.element(2, decode.bool)
    decode.success(#(id, name, active))
  }

  decode.run(decoder, [
    Some(value.Integer(1)),
    Some(value.Text("bob")),
    Some(value.Boolean(True)),
  ])
  |> should.equal(Ok(#(1, "bob", True)))
}

pub fn decode_with_optional_element_test() {
  let decoder = {
    use id <- decode.element(0, decode.int)
    use email <- decode.element(1, decode.optional(decode.text))
    decode.success(#(id, email))
  }

  // With value
  decode.run(decoder, [Some(value.Integer(1)), Some(value.Text("a@b.com"))])
  |> should.equal(Ok(#(1, Some("a@b.com"))))

  // With NULL
  decode.run(decoder, [Some(value.Integer(1)), None])
  |> should.equal(Ok(#(1, None)))
}

pub fn decode_index_out_of_bounds_test() {
  let decoder = {
    use n <- decode.element(5, decode.int)
    decode.success(n)
  }

  decode.run(decoder, [Some(value.Integer(1))])
  |> should.be_error()
}

pub fn decode_wrong_type_in_row_test() {
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  decode.run(decoder, [Some(value.Text("not an int"))])
  |> should.be_error()
}

pub fn decode_empty_row_test() {
  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  decode.run(decoder, [])
  |> should.be_error()
}
