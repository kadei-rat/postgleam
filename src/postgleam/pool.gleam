/// Connection pool - manages multiple PostgreSQL connections.
/// Features: round-robin checkout, health checks, automatic reconnection,
/// wait queue, and PgBouncer compatibility.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import postgleam/codec/defaults
import postgleam/codec/registry.{type Registry}
import postgleam/config.{type Config}
import postgleam/connection.{
  type ConnectionState, type ExtendedQueryResult, type SimpleQueryResult,
}
import postgleam/decode.{type RowDecoder}
import postgleam/error.{type Error}
import postgleam/value.{type Value}

/// Monotonic time in milliseconds (for queue timeout tracking).
@external(erlang, "postgleam_ffi", "monotonic_time_ms")
fn monotonic_time_ms() -> Int

/// Pool message type
pub type PoolMessage {
  /// Execute a parameterized query
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
  /// Internal: periodic health check
  HealthCheck
  /// Internal: reconnect a slot
  Reconnect(index: Int)
}

/// Connection slot state
type ConnectionSlot {
  /// Active and available for queries
  Active(conn: ConnectionState)
  /// Currently checked out (executing a query)
  CheckedOut(conn: ConnectionState)
  /// Dead, attempting to reconnect
  Reconnecting(attempts: Int)
}

/// Queued request waiting for a connection
type QueuedExecute {
  QueuedExecute(
    fun: fn(ConnectionState, Registry, Config) ->
      #(Result(ExtendedQueryResult, Error), ConnectionState),
    reply: Subject(Result(ExtendedQueryResult, Error)),
    enqueued_at: Int,
  )
}

type QueuedSimple {
  QueuedSimple(
    fun: fn(ConnectionState, Config) ->
      #(Result(List(SimpleQueryResult), Error), ConnectionState),
    reply: Subject(Result(List(SimpleQueryResult), Error)),
    enqueued_at: Int,
  )
}

type QueuedRequest {
  QueuedExec(req: QueuedExecute)
  QueuedSimp(req: QueuedSimple)
}

/// Pool state
type PoolState {
  PoolState(
    slots: List(ConnectionSlot),
    config: Config,
    registry: Registry,
    size: Int,
    self: Subject(PoolMessage),
    queue: List(QueuedRequest),
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
            let reg = defaults.build_registry()
            let slots = list.map(conns, fn(c) { Active(c) })
            let state =
              PoolState(
                slots: slots,
                config: config,
                registry: reg,
                size: size,
                self: subject,
                queue: [],
              )
            // Schedule first health check
            let _ =
              process.send_after(subject, config.idle_interval, HealthCheck)
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
          disconnect_all_conns(acc)
          Error(e)
        }
      }
  }
}

fn disconnect_all_conns(conns: List(ConnectionState)) -> Nil {
  case conns {
    [] -> Nil
    [conn, ..rest] -> {
      connection.disconnect(conn)
      disconnect_all_conns(rest)
    }
  }
}

fn handle_message(
  state: PoolState,
  msg: PoolMessage,
) -> actor.Next(PoolState, PoolMessage) {
  case msg {
    Execute(fun, reply) -> {
      case find_available(state.slots, 0) {
        Ok(#(index, conn)) -> {
          let #(result, conn) = fun(conn, state.registry, state.config)
          let is_dead = is_socket_error(result)
          process.send(reply, result)
          case is_dead {
            True -> {
              let slots = set_slot(state.slots, index, Reconnecting(0))
              let _ =
                process.send_after(state.self, 0, Reconnect(index))
              actor.continue(PoolState(..state, slots: slots))
            }
            False -> {
              let slots = set_slot(state.slots, index, Active(conn))
              actor.continue(PoolState(..state, slots: slots))
            }
          }
        }
        Error(_) -> {
          // No available connections — enqueue
          let now = monotonic_time_ms()
          let req =
            QueuedExec(QueuedExecute(fun: fun, reply: reply, enqueued_at: now))
          actor.continue(PoolState(..state, queue: append(state.queue, [req])))
        }
      }
    }

    SimpleExecute(fun, reply) -> {
      case find_available(state.slots, 0) {
        Ok(#(index, conn)) -> {
          let #(result, conn) = fun(conn, state.config)
          let is_dead = is_simple_socket_error(result)
          process.send(reply, result)
          case is_dead {
            True -> {
              let slots = set_slot(state.slots, index, Reconnecting(0))
              let _ =
                process.send_after(state.self, 0, Reconnect(index))
              actor.continue(PoolState(..state, slots: slots))
            }
            False -> {
              let slots = set_slot(state.slots, index, Active(conn))
              actor.continue(PoolState(..state, slots: slots))
            }
          }
        }
        Error(_) -> {
          let now = monotonic_time_ms()
          let req =
            QueuedSimp(QueuedSimple(
              fun: fun,
              reply: reply,
              enqueued_at: now,
            ))
          actor.continue(PoolState(..state, queue: append(state.queue, [req])))
        }
      }
    }

    HealthCheck -> {
      // Ping idle connections, mark dead ones for reconnection
      let state = health_check_slots(state, state.slots, 0, [])
      // Expire timed-out queue entries
      let state = expire_queue(state)
      // Schedule next health check
      let _ =
        process.send_after(
          state.self,
          state.config.idle_interval,
          HealthCheck,
        )
      actor.continue(state)
    }

    Reconnect(index) -> {
      case get_slot(state.slots, index) {
        Ok(Reconnecting(attempts)) -> {
          case connection.connect(state.config) {
            Ok(conn) -> {
              // Reconnected — try to drain queue
              let state =
                PoolState(
                  ..state,
                  slots: set_slot(state.slots, index, Active(conn)),
                )
              let state = drain_queue(state)
              actor.continue(state)
            }
            Error(_) -> {
              // Exponential backoff: 1s, 2s, 4s, 8s, ... max 30s
              let delay =
                int.min(30_000, pow2(attempts) * 1000)
              let _ =
                process.send_after(state.self, delay, Reconnect(index))
              let slots =
                set_slot(state.slots, index, Reconnecting(attempts + 1))
              actor.continue(PoolState(..state, slots: slots))
            }
          }
        }
        _ -> actor.continue(state)
      }
    }

    Shutdown(reply) -> {
      // Reject all queued requests
      reject_queue(state.queue)
      // Disconnect all active connections
      disconnect_all_slots(state.slots)
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

// =============================================================================
// Slot management
// =============================================================================

fn find_available(
  slots: List(ConnectionSlot),
  index: Int,
) -> Result(#(Int, ConnectionState), Nil) {
  case slots {
    [] -> Error(Nil)
    [Active(conn), ..] -> Ok(#(index, conn))
    [_, ..rest] -> find_available(rest, index + 1)
  }
}

fn get_slot(slots: List(ConnectionSlot), index: Int) -> Result(ConnectionSlot, Nil) {
  case slots, index {
    [], _ -> Error(Nil)
    [slot, ..], 0 -> Ok(slot)
    [_, ..rest], n -> get_slot(rest, n - 1)
  }
}

fn set_slot(
  slots: List(ConnectionSlot),
  index: Int,
  value: ConnectionSlot,
) -> List(ConnectionSlot) {
  case slots, index {
    [], _ -> []
    [_, ..rest], 0 -> [value, ..rest]
    [slot, ..rest], n -> [slot, ..set_slot(rest, n - 1, value)]
  }
}

// =============================================================================
// Health checks
// =============================================================================

fn health_check_slots(
  state: PoolState,
  slots: List(ConnectionSlot),
  index: Int,
  acc: List(ConnectionSlot),
) -> PoolState {
  case slots {
    [] -> PoolState(..state, slots: list.reverse(acc))
    [Active(conn), ..rest] -> {
      // Ping with SELECT 1
      case connection.simple_query(conn, "SELECT 1", state.config.timeout) {
        Ok(#(_, conn)) ->
          health_check_slots(state, rest, index + 1, [Active(conn), ..acc])
        Error(_) -> {
          // Dead — schedule reconnect
          let _ =
            process.send_after(state.self, 0, Reconnect(index))
          health_check_slots(
            state,
            rest,
            index + 1,
            [Reconnecting(0), ..acc],
          )
        }
      }
    }
    [slot, ..rest] ->
      health_check_slots(state, rest, index + 1, [slot, ..acc])
  }
}

// =============================================================================
// Queue management
// =============================================================================

fn expire_queue(state: PoolState) -> PoolState {
  let now = monotonic_time_ms()
  let timeout = state.config.queue_timeout
  let #(alive, expired) =
    list.partition(state.queue, fn(req) {
      let enqueued = case req {
        QueuedExec(r) -> r.enqueued_at
        QueuedSimp(r) -> r.enqueued_at
      }
      now - enqueued < timeout
    })
  // Reject expired requests
  list.each(expired, fn(req) {
    case req {
      QueuedExec(r) -> process.send(r.reply, Error(error.TimeoutError))
      QueuedSimp(r) -> process.send(r.reply, Error(error.TimeoutError))
    }
  })
  PoolState(..state, queue: alive)
}

fn drain_queue(state: PoolState) -> PoolState {
  case state.queue {
    [] -> state
    [first, ..rest] -> {
      let now = monotonic_time_ms()
      case first {
        QueuedExec(req) -> {
          case now - req.enqueued_at >= state.config.queue_timeout {
            True -> {
              process.send(req.reply, Error(error.TimeoutError))
              drain_queue(PoolState(..state, queue: rest))
            }
            False -> {
              case find_available(state.slots, 0) {
                Ok(#(index, conn)) -> {
                  let #(result, conn) =
                    req.fun(conn, state.registry, state.config)
                  let is_dead = is_socket_error(result)
                  process.send(req.reply, result)
                  case is_dead {
                    True -> {
                      let slots =
                        set_slot(state.slots, index, Reconnecting(0))
                      let _ =
                        process.send_after(state.self, 0, Reconnect(index))
                      drain_queue(PoolState(..state, slots: slots, queue: rest))
                    }
                    False -> {
                      let slots = set_slot(state.slots, index, Active(conn))
                      drain_queue(PoolState(..state, slots: slots, queue: rest))
                    }
                  }
                }
                Error(_) -> state
              }
            }
          }
        }
        QueuedSimp(req) -> {
          case now - req.enqueued_at >= state.config.queue_timeout {
            True -> {
              process.send(req.reply, Error(error.TimeoutError))
              drain_queue(PoolState(..state, queue: rest))
            }
            False -> {
              case find_available(state.slots, 0) {
                Ok(#(index, conn)) -> {
                  let #(result, conn) = req.fun(conn, state.config)
                  let is_dead = is_simple_socket_error(result)
                  process.send(req.reply, result)
                  case is_dead {
                    True -> {
                      let slots =
                        set_slot(state.slots, index, Reconnecting(0))
                      let _ =
                        process.send_after(state.self, 0, Reconnect(index))
                      drain_queue(PoolState(..state, slots: slots, queue: rest))
                    }
                    False -> {
                      let slots = set_slot(state.slots, index, Active(conn))
                      drain_queue(PoolState(..state, slots: slots, queue: rest))
                    }
                  }
                }
                Error(_) -> state
              }
            }
          }
        }
      }
    }
  }
}

fn reject_queue(queue: List(QueuedRequest)) -> Nil {
  case queue {
    [] -> Nil
    [QueuedExec(r), ..rest] -> {
      process.send(r.reply, Error(error.ConnectionError("Pool shutting down")))
      reject_queue(rest)
    }
    [QueuedSimp(r), ..rest] -> {
      process.send(r.reply, Error(error.ConnectionError("Pool shutting down")))
      reject_queue(rest)
    }
  }
}

fn disconnect_all_slots(slots: List(ConnectionSlot)) -> Nil {
  case slots {
    [] -> Nil
    [Active(conn), ..rest] -> {
      connection.disconnect(conn)
      disconnect_all_slots(rest)
    }
    [CheckedOut(conn), ..rest] -> {
      connection.disconnect(conn)
      disconnect_all_slots(rest)
    }
    [Reconnecting(_), ..rest] -> disconnect_all_slots(rest)
  }
}

// =============================================================================
// Error detection
// =============================================================================

fn is_socket_error(result: Result(ExtendedQueryResult, Error)) -> Bool {
  case result {
    Error(error.SocketError(_)) -> True
    _ -> False
  }
}

fn is_simple_socket_error(
  result: Result(List(SimpleQueryResult), Error),
) -> Bool {
  case result {
    Error(error.SocketError(_)) -> True
    _ -> False
  }
}

// =============================================================================
// Helpers
// =============================================================================

fn pow2(n: Int) -> Int {
  case n {
    0 -> 1
    _ -> 2 * pow2(n - 1)
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

/// Execute a parameterized query through the pool.
/// Uses PgBouncer-safe unnamed statements when pgbouncer mode is enabled.
pub fn query(
  pool: Subject(PoolMessage),
  sql: String,
  params: List(Option(Value)),
  timeout: Int,
) -> Result(ExtendedQueryResult, Error) {
  process.call(pool, timeout, fn(reply) {
    Execute(
      fn(conn, reg, config) {
        let result = case config.pgbouncer {
          True ->
            connection.extended_query_unnamed(
              conn, sql, params, reg, config.timeout,
            )
          False ->
            connection.extended_query(conn, sql, params, reg, config.timeout)
        }
        case result {
          Ok(#(r, c)) -> #(Ok(r), c)
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
