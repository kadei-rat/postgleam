/// Connection pool - manages multiple PostgreSQL connections.
/// Checkout/checkin pattern for concurrent access.

import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import postgleam/config.{type Config}
import postgleam/connection.{
  type ConnectionState, type ExtendedQueryResult,
  type SimpleQueryResult,
}
import postgleam/codec/defaults
import postgleam/codec/registry.{type Registry}
import postgleam/decode.{type RowDecoder}
import postgleam/error.{type Error}
import postgleam/value.{type Value}

/// Pool message type
pub type PoolMessage {
  /// Checkout a connection, execute a function, and return it
  Execute(
    fun: fn(ConnectionState, Registry, Config) ->
      #(Result(ExtendedQueryResult, Error), ConnectionState),
    reply: Subject(Result(ExtendedQueryResult, Error)),
  )
  /// Execute a simple query
  SimpleExecute(
    fun: fn(ConnectionState, Config) ->
      #(Result(List(SimpleQueryResult), Error), ConnectionState),
    reply: Subject(Result(List(SimpleQueryResult), Error)),
  )
  /// Shutdown the pool
  Shutdown(reply: Subject(Nil))
}

/// Pool state
type PoolState {
  PoolState(
    connections: List(ConnectionState),
    config: Config,
    registry: Registry,
    size: Int,
  )
}

/// Start a connection pool with the given config and size
pub fn start(
  config: Config,
  size: Int,
) -> Result(actor.Started(Subject(PoolMessage)), String) {
  case
    actor.new_with_initialiser(
      config.connect_timeout * size + 5000,
      fn(subject) {
        case connect_pool(config, size, []) {
          Ok(conns) -> {
            let reg = registry.build(defaults.matchers())
            let state =
              PoolState(
                connections: conns,
                config: config,
                registry: reg,
                size: size,
              )
            actor.initialised(state)
            |> actor.returning(subject)
            |> Ok
          }
          Error(e) -> Error(error_to_string(e))
        }
      },
    )
    |> actor.on_message(handle_message)
    |> actor.start()
  {
    Ok(started) -> Ok(started)
    Error(actor.InitTimeout) -> Error("Pool initialization timed out")
    Error(actor.InitFailed(reason)) -> Error(reason)
    Error(actor.InitExited(_)) -> Error("Pool process exited during init")
  }
}

fn connect_pool(
  config: Config,
  remaining: Int,
  acc: List(ConnectionState),
) -> Result(List(ConnectionState), Error) {
  case remaining {
    0 -> Ok(acc)
    _ ->
      case connection.connect(config) {
        Ok(conn) -> connect_pool(config, remaining - 1, [conn, ..acc])
        Error(e) -> {
          // Disconnect any already-established connections
          disconnect_all(acc)
          Error(e)
        }
      }
  }
}

fn disconnect_all(conns: List(ConnectionState)) -> Nil {
  case conns {
    [] -> Nil
    [conn, ..rest] -> {
      connection.disconnect(conn)
      disconnect_all(rest)
    }
  }
}

fn handle_message(
  state: PoolState,
  msg: PoolMessage,
) -> actor.Next(PoolState, PoolMessage) {
  case msg {
    Execute(fun, reply) -> {
      case state.connections {
        [] -> {
          process.send(reply, Error(error.ConnectionError("No connections available")))
          actor.continue(state)
        }
        [conn, ..rest] -> {
          let #(result, conn) = fun(conn, state.registry, state.config)
          process.send(reply, result)
          actor.continue(PoolState(..state, connections: append(rest, [conn])))
        }
      }
    }

    SimpleExecute(fun, reply) -> {
      case state.connections {
        [] -> {
          process.send(
            reply,
            Error(error.ConnectionError("No connections available")),
          )
          actor.continue(state)
        }
        [conn, ..rest] -> {
          let #(result, conn) = fun(conn, state.config)
          process.send(reply, result)
          actor.continue(PoolState(..state, connections: append(rest, [conn])))
        }
      }
    }

    Shutdown(reply) -> {
      disconnect_all(state.connections)
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

fn append(a: List(x), b: List(x)) -> List(x) {
  case a {
    [] -> b
    [x, ..rest] -> [x, ..append(rest, b)]
  }
}

fn error_to_string(err: Error) -> String {
  case err {
    error.PgError(fields, _, _) -> "PostgreSQL error: " <> fields.message
    error.ConnectionError(msg) -> msg
    error.AuthenticationError(msg) -> msg
    error.EncodeError(msg) -> msg
    error.DecodeError(msg) -> msg
    error.ProtocolError(msg) -> msg
    error.SocketError(msg) -> msg
    error.TimeoutError -> "Timeout"
  }
}

// =============================================================================
// Public API helpers for use with the pool
// =============================================================================

/// Execute a parameterized query through the pool
pub fn query(
  pool: Subject(PoolMessage),
  sql: String,
  params: List(Option(Value)),
  timeout: Int,
) -> Result(ExtendedQueryResult, Error) {
  process.call(pool, timeout, fn(reply) {
    Execute(
      fn(conn, reg, config) {
        case connection.extended_query(conn, sql, params, reg, config.timeout) {
          Ok(#(result, conn)) -> #(Ok(result), conn)
          Error(e) -> #(Error(e), conn)
        }
      },
      reply,
    )
  })
}

/// Execute a simple query through the pool
pub fn simple_query(
  pool: Subject(PoolMessage),
  sql: String,
  timeout: Int,
) -> Result(List(SimpleQueryResult), Error) {
  process.call(pool, timeout, fn(reply) {
    SimpleExecute(
      fn(conn, config) {
        case connection.simple_query(conn, sql, config.timeout) {
          Ok(#(results, conn)) -> #(Ok(results), conn)
          Error(e) -> #(Error(e), conn)
        }
      },
      reply,
    )
  })
}

/// Execute a parameterized query through the pool and decode rows.
pub fn query_with(
  pool: Subject(PoolMessage),
  sql: String,
  params: List(Option(Value)),
  decoder: RowDecoder(a),
  timeout: Int,
) -> Result(PoolResponse(a), Error) {
  case query(pool, sql, params, timeout) {
    Ok(result) ->
      case decode_rows(result.rows, decoder, []) {
        Ok(decoded) ->
          Ok(PoolResponse(
            rows: decoded,
            count: list.length(decoded),
            tag: result.tag,
          ))
        Error(e) -> Error(e)
      }
    Error(e) -> Error(e)
  }
}

/// Result from a decoded pool query.
pub type PoolResponse(a) {
  PoolResponse(rows: List(a), count: Int, tag: String)
}

/// Shut down the pool, disconnecting all connections
pub fn shutdown(pool: Subject(PoolMessage), timeout: Int) -> Nil {
  process.call(pool, timeout, fn(reply) { Shutdown(reply) })
}

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
