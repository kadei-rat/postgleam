/// Row decoder combinators for extracting typed values from query results.
///
/// ## Usage
///
/// ```gleam
/// import postgleam/decode
///
/// let decoder = {
///   use id <- decode.element(0, decode.int)
///   use name <- decode.element(1, decode.text)
///   use email <- decode.element(2, decode.optional(decode.text))
///   decode.success(User(id:, name:, email:))
/// }
/// ```

import gleam/int
import gleam/option.{type Option, None, Some}
import postgleam/error.{type Error}
import postgleam/value.{type Value}

/// A decoder that extracts a typed value from a row (list of columns).
pub opaque type RowDecoder(a) {
  RowDecoder(run: fn(List(Option(Value))) -> Result(a, Error))
}

/// A decoder that extracts a typed value from a single cell.
pub type ValueDecoder(a) =
  fn(Option(Value)) -> Result(a, Error)

/// Run a decoder on a row.
pub fn run(decoder: RowDecoder(a), row: List(Option(Value))) -> Result(a, Error) {
  decoder.run(row)
}

/// Decode the element at a given column index using a value decoder.
/// Designed for use with Gleam's `use` syntax.
///
/// ```gleam
/// let decoder = {
///   use id <- decode.element(0, decode.int)
///   use name <- decode.element(1, decode.text)
///   decode.success(#(id, name))
/// }
/// ```
pub fn element(
  index: Int,
  decoder: ValueDecoder(a),
  next: fn(a) -> RowDecoder(b),
) -> RowDecoder(b) {
  RowDecoder(fn(row) {
    case list_at(row, index) {
      Ok(cell) ->
        case decoder(cell) {
          Ok(val) -> run(next(val), row)
          Error(e) -> Error(e)
        }
      Error(_) ->
        Error(error.DecodeError(
          "Column index " <> int.to_string(index) <> " out of bounds",
        ))
    }
  })
}

/// Finalize a decoder chain with a value.
pub fn success(value: a) -> RowDecoder(a) {
  RowDecoder(fn(_row) { Ok(value) })
}

// =============================================================================
// Value decoders
// =============================================================================

/// Decode an integer value (int2, int4, int8).
pub fn int(val: Option(Value)) -> Result(Int, Error) {
  case val {
    Some(value.Integer(n)) -> Ok(n)
    Some(other) ->
      Error(error.DecodeError("Expected Integer, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Integer, got NULL"))
  }
}

/// Decode a text/string value.
pub fn text(val: Option(Value)) -> Result(String, Error) {
  case val {
    Some(value.Text(s)) -> Ok(s)
    Some(other) ->
      Error(error.DecodeError("Expected Text, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Text, got NULL"))
  }
}

/// Decode a boolean value.
pub fn bool(val: Option(Value)) -> Result(Bool, Error) {
  case val {
    Some(value.Boolean(b)) -> Ok(b)
    Some(other) ->
      Error(error.DecodeError(
        "Expected Boolean, got " <> value_type_name(other),
      ))
    None -> Error(error.DecodeError("Expected Boolean, got NULL"))
  }
}

/// Decode a float value (float4, float8).
pub fn float(val: Option(Value)) -> Result(Float, Error) {
  case val {
    Some(value.Float(f)) -> Ok(f)
    Some(other) ->
      Error(error.DecodeError("Expected Float, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Float, got NULL"))
  }
}

/// Decode a bytea (binary data) value.
pub fn bytea(val: Option(Value)) -> Result(BitArray, Error) {
  case val {
    Some(value.Bytea(b)) -> Ok(b)
    Some(other) ->
      Error(error.DecodeError("Expected Bytea, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Bytea, got NULL"))
  }
}

/// Decode a UUID value (16-byte binary).
pub fn uuid(val: Option(Value)) -> Result(BitArray, Error) {
  case val {
    Some(value.Uuid(u)) -> Ok(u)
    Some(other) ->
      Error(error.DecodeError("Expected Uuid, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Uuid, got NULL"))
  }
}

/// Decode a JSON value (string).
pub fn json(val: Option(Value)) -> Result(String, Error) {
  case val {
    Some(value.Json(s)) -> Ok(s)
    Some(other) ->
      Error(error.DecodeError("Expected Json, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Json, got NULL"))
  }
}

/// Decode a JSONB value (string).
pub fn jsonb(val: Option(Value)) -> Result(String, Error) {
  case val {
    Some(value.Jsonb(s)) -> Ok(s)
    Some(other) ->
      Error(error.DecodeError("Expected Jsonb, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Jsonb, got NULL"))
  }
}

/// Decode a numeric/decimal value (string representation).
pub fn numeric(val: Option(Value)) -> Result(String, Error) {
  case val {
    Some(value.Numeric(n)) -> Ok(n)
    Some(other) ->
      Error(error.DecodeError(
        "Expected Numeric, got " <> value_type_name(other),
      ))
    None -> Error(error.DecodeError("Expected Numeric, got NULL"))
  }
}

/// Decode a date value (days since 2000-01-01).
pub fn date(val: Option(Value)) -> Result(Int, Error) {
  case val {
    Some(value.Date(d)) -> Ok(d)
    Some(other) ->
      Error(error.DecodeError("Expected Date, got " <> value_type_name(other)))
    None -> Error(error.DecodeError("Expected Date, got NULL"))
  }
}

/// Decode a timestamp value (microseconds since 2000-01-01 00:00:00).
pub fn timestamp(val: Option(Value)) -> Result(Int, Error) {
  case val {
    Some(value.Timestamp(t)) -> Ok(t)
    Some(other) ->
      Error(error.DecodeError(
        "Expected Timestamp, got " <> value_type_name(other),
      ))
    None -> Error(error.DecodeError("Expected Timestamp, got NULL"))
  }
}

/// Decode a timestamptz value (microseconds since 2000-01-01 00:00:00 UTC).
pub fn timestamptz(val: Option(Value)) -> Result(Int, Error) {
  case val {
    Some(value.Timestamptz(t)) -> Ok(t)
    Some(other) ->
      Error(error.DecodeError(
        "Expected Timestamptz, got " <> value_type_name(other),
      ))
    None -> Error(error.DecodeError("Expected Timestamptz, got NULL"))
  }
}

/// Make any value decoder nullable (NULL-safe).
/// Returns `Ok(None)` for NULL, `Ok(Some(val))` for non-NULL.
pub fn optional(decoder: ValueDecoder(a)) -> ValueDecoder(Option(a)) {
  fn(val) {
    case val {
      None -> Ok(None)
      _ ->
        case decoder(val) {
          Ok(v) -> Ok(Some(v))
          Error(e) -> Error(e)
        }
    }
  }
}

// =============================================================================
// Helpers
// =============================================================================

fn list_at(l: List(a), index: Int) -> Result(a, Nil) {
  case l, index {
    [x, ..], 0 -> Ok(x)
    [_, ..rest], n if n > 0 -> list_at(rest, n - 1)
    _, _ -> Error(Nil)
  }
}

fn value_type_name(val: Value) -> String {
  case val {
    value.Null -> "Null"
    value.Boolean(_) -> "Boolean"
    value.Integer(_) -> "Integer"
    value.Float(_) -> "Float"
    value.PosInfinity -> "PosInfinity"
    value.NegInfinity -> "NegInfinity"
    value.NaN -> "NaN"
    value.Text(_) -> "Text"
    value.Bytea(_) -> "Bytea"
    value.Uuid(_) -> "Uuid"
    value.Oid(_) -> "Oid"
    value.Void -> "Void"
    value.Array(_) -> "Array"
    value.Date(_) -> "Date"
    value.Time(_) -> "Time"
    value.TimeTz(_, _) -> "TimeTz"
    value.Timestamp(_) -> "Timestamp"
    value.Timestamptz(_) -> "Timestamptz"
    value.Interval(_, _, _) -> "Interval"
    value.Json(_) -> "Json"
    value.Jsonb(_) -> "Jsonb"
    value.Numeric(_) -> "Numeric"
    value.Point(_, _) -> "Point"
    value.Inet(_, _, _) -> "Inet"
    value.Macaddr(_) -> "Macaddr"
  }
}
