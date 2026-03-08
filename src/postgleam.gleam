/// Postgleam - A native Gleam PostgreSQL driver implementing the wire protocol.
///
/// ## Quick Start
///
/// ```gleam
/// let assert Ok(conn) = postgleam.connect(config.default() |> config.database("mydb"))
/// let assert Ok(response) = postgleam.query_with(
///   conn,
///   "SELECT $1::int4, $2::text",
///   [postgleam.int(42), postgleam.text("hello")],
///   {
///     use id <- decode.element(0, decode.int)
///     use name <- decode.element(1, decode.text)
///     decode.success(#(id, name))
///   },
/// )
/// postgleam.disconnect(conn)
/// ```

import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import postgleam/config.{type Config}
import postgleam/connection.{
  type ExtendedQueryResult, type PreparedStatement, type SimpleQueryResult,
}
import postgleam/decode.{type RowDecoder}
import postgleam/error.{type Error}
import postgleam/internal/connection_actor.{type Message}
import postgleam/value.{type Value}

/// A query parameter value (nullable).
pub type Param =
  Option(Value)

/// Result from a decoded query.
pub type Response(a) {
  Response(rows: List(a), count: Int, tag: String)
}

/// An opaque connection handle backed by an OTP actor.
pub opaque type Connection {
  Connection(subject: Subject(Message), config: Config)
}

/// Connect to PostgreSQL and return a connection handle.
/// The connection is managed by an OTP actor process.
pub fn connect(config: Config) -> Result(Connection, Error) {
  case connection_actor.start(config) {
    Ok(started) -> Ok(Connection(subject: started.data, config: config))
    Error(actor.InitTimeout) ->
      Error(error.ConnectionError("Connection initialization timed out"))
    Error(actor.InitFailed(reason)) ->
      Error(error.ConnectionError(reason))
    Error(actor.InitExited(_)) ->
      Error(error.ConnectionError("Connection process exited during init"))
  }
}

/// Execute a parameterized query using the extended protocol (binary format).
/// Returns typed values decoded through the codec registry.
///
/// Panics if the actor does not respond within the configured timeout.
pub fn query(
  conn: Connection,
  sql: String,
  params: List(Param),
) -> Result(ExtendedQueryResult, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.Query(sql, params, reply)
  })
}

/// Execute a parameterized query and decode each row using the provided decoder.
pub fn query_with(
  conn: Connection,
  sql: String,
  params: List(Param),
  decoder: RowDecoder(a),
) -> Result(Response(a), Error) {
  case query(conn, sql, params) {
    Ok(result) ->
      case decode_rows(result.rows, decoder, []) {
        Ok(decoded) ->
          Ok(Response(
            rows: decoded,
            count: list.length(decoded),
            tag: result.tag,
          ))
        Error(e) -> Error(e)
      }
    Error(e) -> Error(e)
  }
}

/// Execute the same query with multiple param sets in a single pipeline.
/// All Bind+Execute messages are sent together with a single Sync,
/// eliminating N-1 round-trips compared to calling query() N times.
/// Returns one result per param set.
pub fn query_batch(
  conn: Connection,
  sql: String,
  param_sets: List(List(Param)),
) -> Result(List(ExtendedQueryResult), Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.BatchQuery(sql, param_sets, reply)
  })
}

/// Execute a query and return the first decoded row, or error if no rows.
pub fn query_one(
  conn: Connection,
  sql: String,
  params: List(Param),
  decoder: RowDecoder(a),
) -> Result(a, Error) {
  case query_with(conn, sql, params, decoder) {
    Ok(response) ->
      case response.rows {
        [first, ..] -> Ok(first)
        [] -> Error(error.DecodeError("Expected at least one row, got none"))
      }
    Error(e) -> Error(e)
  }
}

/// Execute a simple text query (no parameters).
/// Returns text-formatted results.
pub fn simple_query(
  conn: Connection,
  sql: String,
) -> Result(List(SimpleQueryResult), Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.SimpleQuery(sql, reply)
  })
}

/// Prepare a named statement for later execution.
pub fn prepare(
  conn: Connection,
  name: String,
  sql: String,
) -> Result(PreparedStatement, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.Prepare(name, sql, reply)
  })
}

/// Execute a previously prepared statement.
pub fn execute(
  conn: Connection,
  prepared: PreparedStatement,
  params: List(Param),
) -> Result(ExtendedQueryResult, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.ExecutePrepared(prepared, params, reply)
  })
}

/// Close a prepared statement.
pub fn close(conn: Connection, name: String) -> Result(Nil, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.CloseStatement(name, reply)
  })
}

/// Bulk insert data using the COPY protocol.
/// `sql` should be a COPY ... FROM STDIN statement.
/// `data` is a list of text-format row chunks (each ending with \n).
/// Returns the command tag (e.g., "COPY 1000").
pub fn copy_in(
  conn: Connection,
  sql: String,
  data: List(BitArray),
) -> Result(String, Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.CopyIn(sql, data, reply)
  })
}

/// Export data using the COPY protocol.
/// `sql` should be a COPY ... TO STDOUT statement.
/// Returns the raw data chunks from PostgreSQL.
pub fn copy_out(
  conn: Connection,
  sql: String,
) -> Result(List(BitArray), Error) {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.CopyOut(sql, reply)
  })
}

/// Disconnect from PostgreSQL and stop the actor.
pub fn disconnect(conn: Connection) -> Nil {
  process.call(conn.subject, conn.config.timeout, fn(reply) {
    connection_actor.Disconnect(reply)
  })
}

/// Execute a function within a transaction.
/// Automatically BEGINs, and COMMITs on Ok or ROLLBACKs on Error.
pub fn transaction(
  conn: Connection,
  f: fn(Connection) -> Result(a, Error),
) -> Result(a, Error) {
  case simple_query(conn, "BEGIN") {
    Error(e) -> Error(e)
    Ok(_) -> {
      case f(conn) {
        Ok(val) -> {
          case simple_query(conn, "COMMIT") {
            Ok(_) -> Ok(val)
            Error(e) -> Error(e)
          }
        }
        Error(e) -> {
          let _ = simple_query(conn, "ROLLBACK")
          Error(e)
        }
      }
    }
  }
}

/// Create an error value for use in transactions or other contexts.
pub fn query_error(message: String) -> Error {
  error.ConnectionError(message)
}

// =============================================================================
// Parameter constructors
// =============================================================================

/// Create an integer parameter (int2, int4, int8).
pub fn int(val: Int) -> Param {
  Some(value.Integer(val))
}

/// Create a float parameter (float4, float8).
pub fn float(val: Float) -> Param {
  Some(value.Float(val))
}

/// Create a text/string parameter.
pub fn text(val: String) -> Param {
  Some(value.Text(val))
}

/// Create a boolean parameter.
pub fn bool(val: Bool) -> Param {
  Some(value.Boolean(val))
}

/// Create a NULL parameter.
pub fn null() -> Param {
  None
}

/// Create a bytea (binary data) parameter.
pub fn bytea(val: BitArray) -> Param {
  Some(value.Bytea(val))
}

/// Create a UUID parameter (16-byte binary).
pub fn uuid(val: BitArray) -> Param {
  Some(value.Uuid(val))
}

/// Create a UUID parameter from a string like "550e8400-e29b-41d4-a716-446655440000".
/// Returns None (NULL) if the string is not a valid UUID.
pub fn uuid_string(val: String) -> Param {
  case value.uuid_from_string(val) {
    Ok(v) -> Some(v)
    Error(_) -> None
  }
}

/// Create a JSON parameter.
pub fn json(val: String) -> Param {
  Some(value.Json(val))
}

/// Create a JSONB parameter.
pub fn jsonb(val: String) -> Param {
  Some(value.Jsonb(val))
}

/// Create a numeric/decimal parameter (string representation).
pub fn numeric(val: String) -> Param {
  Some(value.Numeric(val))
}

/// Create a date parameter (days since 2000-01-01).
pub fn date(val: Int) -> Param {
  Some(value.Date(val))
}

/// Create a timestamp parameter (microseconds since 2000-01-01 00:00:00).
pub fn timestamp(val: Int) -> Param {
  Some(value.Timestamp(val))
}

/// Create a timestamptz parameter (microseconds since 2000-01-01 00:00:00 UTC).
pub fn timestamptz(val: Int) -> Param {
  Some(value.Timestamptz(val))
}

/// Create a time parameter (microseconds since midnight).
pub fn time(val: Int) -> Param {
  Some(value.Time(val))
}

/// Create a timetz parameter (microseconds since midnight, tz offset in seconds).
pub fn timetz(microseconds: Int, tz_offset: Int) -> Param {
  Some(value.TimeTz(microseconds, tz_offset))
}

/// Create an interval parameter (microseconds, days, months).
pub fn interval(microseconds: Int, days: Int, months: Int) -> Param {
  Some(value.Interval(microseconds, days, months))
}

/// Create an XML parameter.
pub fn xml(val: String) -> Param {
  Some(value.Xml(val))
}

/// Create a JSONPath parameter.
pub fn jsonpath(val: String) -> Param {
  Some(value.Jsonpath(val))
}

/// Create a money parameter (int64 cents).
pub fn money(val: Int) -> Param {
  Some(value.Money(val))
}

/// Create a point parameter (x, y).
pub fn point(x: Float, y: Float) -> Param {
  Some(value.Point(x, y))
}

/// Create a circle parameter (center x, center y, radius).
pub fn circle(x: Float, y: Float, radius: Float) -> Param {
  Some(value.Circle(x, y, radius))
}

/// Create a macaddr parameter (6-byte binary).
pub fn macaddr(val: BitArray) -> Param {
  Some(value.Macaddr(val))
}

/// Create a macaddr8 parameter (8-byte binary).
pub fn macaddr8(val: BitArray) -> Param {
  Some(value.Macaddr8(val))
}

/// Create an array parameter from a list of optional values.
///
/// ```gleam
/// postgleam.array([Some(value.Integer(1)), Some(value.Integer(2)), None])
/// ```
pub fn array(val: List(Option(Value))) -> Param {
  Some(value.Array(val))
}

/// Create a line parameter (Ax + By + C = 0).
pub fn line(a: Float, b: Float, c: Float) -> Param {
  Some(value.Line(a, b, c))
}

/// Create a line segment parameter (two endpoints).
pub fn lseg(x1: Float, y1: Float, x2: Float, y2: Float) -> Param {
  Some(value.Lseg(x1, y1, x2, y2))
}

/// Create a box parameter (upper-right and lower-left corners).
pub fn box(x1: Float, y1: Float, x2: Float, y2: Float) -> Param {
  Some(value.Box(x1, y1, x2, y2))
}

/// Create a path parameter (closed/open flag + list of points).
pub fn path(closed: Bool, points: List(#(Float, Float))) -> Param {
  Some(value.Path(closed, points))
}

/// Create a polygon parameter (list of vertices).
pub fn polygon(vertices: List(#(Float, Float))) -> Param {
  Some(value.Polygon(vertices))
}

/// Create an inet/cidr parameter (family, address bytes, netmask).
/// Family: 2 for IPv4, 3 for IPv6.
pub fn inet(family: Int, address: BitArray, netmask: Int) -> Param {
  Some(value.Inet(family, address, netmask))
}

/// Create a bit string parameter (bit count + data bytes).
pub fn bit_string(bit_count: Int, data: BitArray) -> Param {
  Some(value.BitString(bit_count, data))
}

/// Create a nullable parameter from an Option value.
///
/// ```gleam
/// postgleam.nullable(Some("hello"), postgleam.text)
/// // => Some(value.Text("hello"))
///
/// postgleam.nullable(None, postgleam.text)
/// // => None
/// ```
pub fn nullable(val: Option(a), to_param: fn(a) -> Param) -> Param {
  case val {
    Some(v) -> to_param(v)
    None -> None
  }
}

// =============================================================================
// Helpers
// =============================================================================

fn decode_rows(
  rows: List(List(Option(Value))),
  decoder: RowDecoder(a),
  acc: List(a),
) -> Result(List(a), Error) {
  case rows {
    [] -> Ok(list.reverse(acc))
    [row, ..rest] ->
      case decode.run(decoder, row) {
        Ok(val) -> decode_rows(rest, decoder, [val, ..acc])
        Error(e) -> Error(e)
      }
  }
}
