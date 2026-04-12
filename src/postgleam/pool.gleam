/// Connection pool - manages multiple PostgreSQL connections.
/// Each connection is an independent actor/process, enabling true parallel
/// query execution across N connections.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, Some}
import gleam/otp/actor
import postgleam/codec/defaults
import postgleam/codec/registry.{type Registry}
import postgleam/codec/text
import postgleam/config.{type Config}
import postgleam/connection.{type ExtendedQueryResult, type SimpleQueryResult}
import postgleam/decode.{type RowDecoder}
import postgleam/error.{type Error}
import postgleam/internal/connection_actor
import postgleam/value.{type Value}

/// Monotonic time in milliseconds (for queue timeout tracking).
@external(erlang, "postgleam_ffi", "monotonic_time_ms")
fn monotonic_time_ms() -> Int

/// Pool message type
pub type PoolMessage {
  /// Request a connection actor from the pool
  Checkout(
    reply: Subject(Result(#(Int, Subject(connection_actor.Message)), Error)),
  )
  /// Return a connection actor to the pool
  Checkin(index: Int, is_dead: Bool)
  /// Shutdown the pool
  Shutdown(reply: Subject(Nil))
  /// Internal: periodic health check
  HealthCheck
  /// Internal: health check detected a dead connection
  HealthCheckFailed(index: Int)
  /// Internal: reconnect a slot
  Reconnect(index: Int)
}

/// Connection slot state
type ConnectionSlot {
  /// Idle, available for checkout
  Active(subject: Subject(connection_actor.Message))
  /// Checked out to a caller
  CheckedOut(subject: Subject(connection_actor.Message), since: Int)
  /// Dead, attempting to reconnect
  Reconnecting(attempts: Int)
}

/// Queued checkout request waiting for a connection
type QueuedCheckout {
  QueuedCheckout(
    reply: Subject(Result(#(Int, Subject(connection_actor.Message)), Error)),
    enqueued_at: Int,
  )
}

/// Pool state
type PoolState {
  PoolState(
    slots: List(ConnectionSlot),
    config: Config,
    registry: Registry,
    size: Int,
    self: Subject(PoolMessage),
    queue: List(QueuedCheckout),
  )
}

/// Start a connection pool with the given config and size
pub fn start(
  config: Config,
  size: Int,
) -> Result(actor.Started(Subject(PoolMessage)), String) {
  case
    actor.new_with_initialiser(
      config.connect_timeout * { size + 1 } + 5000,
      fn(subject) {
        let reg = defaults.build_registry()
        // Discover enum types using a temporary raw connection
        let reg = case connection.connect(config) {
          Ok(temp_conn) -> {
            let #(reg, temp_conn) =
              discover_enum_types(temp_conn, reg, config)
            connection.disconnect(temp_conn)
            reg
          }
          Error(_) -> reg
        }
        // Start N connection actors with the enriched registry
        case start_actors(config, reg, size, []) {
          Ok(actors) -> {
            let slots = list.map(actors, fn(a) { Active(a) })
            let state =
              PoolState(
                slots: slots,
                config: config,
                registry: reg,
                size: size,
                self: subject,
                queue: [],
              )
            let _ =
              process.send_after(subject, config.idle_interval, HealthCheck)
            actor.initialised(state)
            |> actor.returning(subject)
            |> Ok
          }
          Error(e) -> Error(e)
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

fn start_actors(
  config: Config,
  registry: Registry,
  remaining: Int,
  acc: List(Subject(connection_actor.Message)),
) -> Result(List(Subject(connection_actor.Message)), String) {
  case remaining {
    0 -> Ok(acc)
    _ ->
      case connection_actor.start_with_registry(config, registry) {
        Ok(started) ->
          start_actors(config, registry, remaining - 1, [started.data, ..acc])
        Error(_) -> {
          list.each(acc, fn(subj) {
            process.send(
              subj,
              connection_actor.Disconnect(process.new_subject()),
            )
          })
          Error("Failed to start connection actor")
        }
      }
  }
}

/// Query pg_type for custom enum types and register their OIDs with
/// the text codec. Enums are text-representable on the wire — their
/// binary format is the enum label as UTF-8, identical to the text codec.
fn discover_enum_types(
  conn: connection.ConnectionState,
  reg: Registry,
  config: Config,
) -> #(Registry, connection.ConnectionState) {
  case
    connection.simple_query(
      conn,
      "SELECT oid::text FROM pg_type WHERE typtype = 'e'",
      config.timeout,
    )
  {
    Ok(#(results, conn)) -> {
      let reg = case results {
        [result, ..] ->
          list.fold(result.rows, reg, fn(reg, row) {
            case row {
              [Some(oid_str), ..] ->
                case int.parse(oid_str) {
                  Ok(oid) -> registry.register(reg, oid, text.matcher())
                  Error(_) -> reg
                }
              _ -> reg
            }
          })
        _ -> reg
      }
      #(reg, conn)
    }
    Error(_) -> #(reg, conn)
  }
}

// =============================================================================
// Message handler
// =============================================================================

fn handle_message(
  state: PoolState,
  msg: PoolMessage,
) -> actor.Next(PoolState, PoolMessage) {
  case msg {
    Checkout(reply) -> {
      case find_available(state.slots, 0) {
        Ok(#(index, subject)) -> {
          let now = monotonic_time_ms()
          let slots =
            set_slot(state.slots, index, CheckedOut(subject, now))
          process.send(reply, Ok(#(index, subject)))
          actor.continue(PoolState(..state, slots: slots))
        }
        Error(_) -> {
          let now = monotonic_time_ms()
          let queue =
            append(state.queue, [QueuedCheckout(reply: reply, enqueued_at: now)])
          actor.continue(PoolState(..state, queue: queue))
        }
      }
    }

    Checkin(index, is_dead) -> {
      case get_slot(state.slots, index) {
        Ok(CheckedOut(subject, _)) -> {
          case is_dead {
            True -> {
              process.send(
                subject,
                connection_actor.Disconnect(process.new_subject()),
              )
              let slots =
                set_slot(state.slots, index, Reconnecting(0))
              let _ = process.send_after(state.self, 0, Reconnect(index))
              actor.continue(PoolState(..state, slots: slots))
            }
            False -> {
              let slots =
                set_slot(state.slots, index, Active(subject))
              let state = PoolState(..state, slots: slots)
              let state = drain_queue(state)
              actor.continue(state)
            }
          }
        }
        _ -> actor.continue(state)
      }
    }

    HealthCheck -> {
      let now = monotonic_time_ms()
      // Ping idle connections, reclaim stale checkouts
      let state = health_check_slots(state, state.slots, 0, [], now)
      // Expire timed-out queue entries
      let state = expire_queue(state)
      let _ =
        process.send_after(state.self, state.config.idle_interval, HealthCheck)
      actor.continue(state)
    }

    HealthCheckFailed(index) -> {
      case get_slot(state.slots, index) {
        Ok(Active(subject)) -> {
          process.send(
            subject,
            connection_actor.Disconnect(process.new_subject()),
          )
          let slots = set_slot(state.slots, index, Reconnecting(0))
          let _ = process.send_after(state.self, 0, Reconnect(index))
          actor.continue(PoolState(..state, slots: slots))
        }
        _ -> actor.continue(state)
      }
    }

    Reconnect(index) -> {
      case get_slot(state.slots, index) {
        Ok(Reconnecting(attempts)) -> {
          case
            connection_actor.start_with_registry(state.config, state.registry)
          {
            Ok(started) -> {
              let slots =
                set_slot(state.slots, index, Active(started.data))
              let state = PoolState(..state, slots: slots)
              let state = drain_queue(state)
              actor.continue(state)
            }
            Error(_) -> {
              let delay = int.min(30_000, pow2(attempts) * 1000)
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
      reject_queue(state.queue)
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
) -> Result(#(Int, Subject(connection_actor.Message)), Nil) {
  case slots {
    [] -> Error(Nil)
    [Active(subject), ..] -> Ok(#(index, subject))
    [_, ..rest] -> find_available(rest, index + 1)
  }
}

fn get_slot(
  slots: List(ConnectionSlot),
  index: Int,
) -> Result(ConnectionSlot, Nil) {
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
  now: Int,
) -> PoolState {
  case slots {
    [] -> PoolState(..state, slots: list.reverse(acc))
    [Active(subject), ..rest] -> {
      // Spawn a process to ping this idle connection
      let pool = state.self
      let i = index
      process.spawn(fn() {
        let subj = process.new_subject()
        process.send(subject, connection_actor.SimpleQuery("SELECT 1", subj))
        case process.receive(subj, 5000) {
          Ok(Ok(_)) -> Nil
          _ -> process.send(pool, HealthCheckFailed(i))
        }
      })
      health_check_slots(state, rest, index + 1, [Active(subject), ..acc], now)
    }
    [CheckedOut(subject, since), ..rest] -> {
      // Reclaim slots checked out for too long (caller probably crashed)
      case now - since > state.config.timeout * 2 {
        True ->
          health_check_slots(
            state,
            rest,
            index + 1,
            [Active(subject), ..acc],
            now,
          )
        False ->
          health_check_slots(
            state,
            rest,
            index + 1,
            [CheckedOut(subject, since), ..acc],
            now,
          )
      }
    }
    [slot, ..rest] ->
      health_check_slots(state, rest, index + 1, [slot, ..acc], now)
  }
}

// =============================================================================
// Queue management
// =============================================================================

fn expire_queue(state: PoolState) -> PoolState {
  let now = monotonic_time_ms()
  let timeout = state.config.queue_timeout
  let #(alive, expired) =
    list.partition(state.queue, fn(req) { now - req.enqueued_at < timeout })
  list.each(expired, fn(req) {
    process.send(req.reply, Error(error.TimeoutError))
  })
  PoolState(..state, queue: alive)
}

fn drain_queue(state: PoolState) -> PoolState {
  case state.queue {
    [] -> state
    [first, ..rest] -> {
      let now = monotonic_time_ms()
      case now - first.enqueued_at >= state.config.queue_timeout {
        True -> {
          process.send(first.reply, Error(error.TimeoutError))
          drain_queue(PoolState(..state, queue: rest))
        }
        False -> {
          case find_available(state.slots, 0) {
            Ok(#(index, subject)) -> {
              let slots =
                set_slot(state.slots, index, CheckedOut(subject, now))
              process.send(first.reply, Ok(#(index, subject)))
              drain_queue(PoolState(..state, slots: slots, queue: rest))
            }
            Error(_) -> state
          }
        }
      }
    }
  }
}

fn reject_queue(queue: List(QueuedCheckout)) -> Nil {
  case queue {
    [] -> Nil
    [req, ..rest] -> {
      process.send(
        req.reply,
        Error(error.ConnectionError("Pool shutting down")),
      )
      reject_queue(rest)
    }
  }
}

fn disconnect_all_slots(slots: List(ConnectionSlot)) -> Nil {
  case slots {
    [] -> Nil
    [Active(subject), ..rest] -> {
      process.send(subject, connection_actor.Disconnect(process.new_subject()))
      disconnect_all_slots(rest)
    }
    [CheckedOut(subject, _), ..rest] -> {
      process.send(subject, connection_actor.Disconnect(process.new_subject()))
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

// =============================================================================
// Public API
// =============================================================================

/// Checkout a connection actor, run a function, and check it back in.
/// Ensures the connection is always returned to the pool, even on timeout.
fn with_connection(
  pool: Subject(PoolMessage),
  timeout: Int,
  run: fn(Subject(connection_actor.Message)) -> #(Result(a, Error), Bool),
) -> Result(a, Error) {
  let checkout_subj = process.new_subject()
  process.send(pool, Checkout(checkout_subj))
  case process.receive(checkout_subj, timeout) {
    Ok(Ok(#(index, actor))) -> {
      let #(result, is_dead) = run(actor)
      process.send(pool, Checkin(index, is_dead))
      result
    }
    Ok(Error(e)) -> Error(e)
    Error(Nil) -> Error(error.TimeoutError)
  }
}

/// Execute a parameterized query through the pool.
pub fn query(
  pool: Subject(PoolMessage),
  sql: String,
  params: List(Option(Value)),
  timeout: Int,
) -> Result(ExtendedQueryResult, Error) {
  with_connection(pool, timeout, fn(actor) {
    let subj = process.new_subject()
    process.send(actor, connection_actor.Query(sql, params, subj))
    case process.receive(subj, timeout) {
      Ok(r) -> #(r, is_socket_error(r))
      Error(Nil) -> #(Error(error.TimeoutError), True)
    }
  })
}

/// Execute a simple query through the pool
pub fn simple_query(
  pool: Subject(PoolMessage),
  sql: String,
  timeout: Int,
) -> Result(List(SimpleQueryResult), Error) {
  with_connection(pool, timeout, fn(actor) {
    let subj = process.new_subject()
    process.send(actor, connection_actor.SimpleQuery(sql, subj))
    case process.receive(subj, timeout) {
      Ok(r) -> #(r, is_simple_socket_error(r))
      Error(Nil) -> #(Error(error.TimeoutError), True)
    }
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
