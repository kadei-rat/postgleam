/// OTP actor wrapping a PostgreSQL connection.
/// Handles sequential query execution through typed messages.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import postgleam/codec/defaults
import postgleam/codec/registry.{type Registry}
import postgleam/config.{type Config}
import postgleam/connection.{
  type ConnectionState, type ExtendedQueryResult, type PreparedStatement,
}
import postgleam/copy
import postgleam/error.{type Error}
import postgleam/value.{type Value}

/// Messages the connection actor handles
pub type Message {
  /// Execute a query using the extended protocol
  Query(
    sql: String,
    params: List(Option(Value)),
    reply: Subject(Result(ExtendedQueryResult, Error)),
  )
  /// Execute a simple (text) query. `timeout` is the per-call socket
  /// timeout (ms) — lets callers (e.g. the pool's health check) bound
  /// how long the actor blocks in `gen_tcp:recv` on a dead socket
  /// instead of waiting for `config.timeout`.
  SimpleQuery(
    sql: String,
    timeout: Int,
    reply: Subject(
      Result(List(connection.SimpleQueryResult), Error),
    ),
  )
  /// Prepare a named statement
  Prepare(
    name: String,
    sql: String,
    reply: Subject(Result(PreparedStatement, Error)),
  )
  /// Execute a prepared statement
  ExecutePrepared(
    prepared: PreparedStatement,
    params: List(Option(Value)),
    reply: Subject(Result(ExtendedQueryResult, Error)),
  )
  /// Execute a query with multiple param sets in a pipeline
  BatchQuery(
    sql: String,
    param_sets: List(List(Option(Value))),
    reply: Subject(Result(List(ExtendedQueryResult), Error)),
  )
  /// Close a prepared statement
  CloseStatement(
    name: String,
    reply: Subject(Result(Nil, Error)),
  )
  /// COPY data into a table (bulk insert)
  CopyIn(
    sql: String,
    data: List(BitArray),
    reply: Subject(Result(String, Error)),
  )
  /// COPY data out of a table
  CopyOut(
    sql: String,
    reply: Subject(Result(List(BitArray), Error)),
  )
  /// Disconnect
  Disconnect(reply: Subject(Nil))
}

/// Maximum number of cached prepared statements before eviction.
const max_cache_size = 256

/// Actor state
pub type ActorState {
  ActorState(
    conn: ConnectionState,
    config: Config,
    registry: Registry,
    cache: Dict(String, PreparedStatement),
    cache_order: List(String),
    next_id: Int,
  )
}

/// Start the connection actor
pub fn start(
  config: Config,
) -> Result(actor.Started(Subject(Message)), actor.StartError) {
  start_with_registry(config, defaults.build_registry())
}

/// Start the connection actor with a pre-built registry.
/// Used by the pool to share an enriched registry (with enum OIDs etc.)
/// across all connection actors.
pub fn start_with_registry(
  config: Config,
  registry: Registry,
) -> Result(actor.Started(Subject(Message)), actor.StartError) {
  actor.new_with_initialiser(config.connect_timeout + 1000, fn(subject) {
    case connection.connect(config) {
      Ok(conn) -> {
        let state =
          ActorState(
            conn: conn,
            config: config,
            registry: registry,
            cache: dict.new(),
            cache_order: [],
            next_id: 0,
          )
        actor.initialised(state)
        |> actor.returning(subject)
        |> Ok
      }
      Error(e) -> Error(error_to_string(e))
    }
  })
  |> actor.on_message(handle_message)
  |> actor.start()
}

fn handle_message(
  state: ActorState,
  msg: Message,
) -> actor.Next(ActorState, Message) {
  case msg {
    Query(sql, params, reply) -> {
      case state.config.pgbouncer {
        // PgBouncer mode: always use unnamed statements, no caching
        True -> {
          case
            connection.extended_query_unnamed(
              state.conn,
              sql,
              params,
              state.registry,
              state.config.timeout,
            )
          {
            Ok(#(result, conn)) -> {
              process.send(reply, Ok(result))
              actor.continue(ActorState(..state, conn: conn))
            }
            Error(e) -> {
              process.send(reply, Error(e))
              actor.continue(state)
            }
          }
        }
        // Normal mode: named statements + caching
        False -> {
          case dict.get(state.cache, sql) {
            // Cache hit: skip Parse+Describe, just Bind+Execute
            Ok(cached) -> {
              case
                connection.execute_prepared(
                  state.conn,
                  cached,
                  params,
                  state.registry,
                  state.config.timeout,
                )
              {
                Ok(#(result, conn)) -> {
                  process.send(reply, Ok(result))
                  actor.continue(ActorState(..state, conn: conn))
                }
                Error(e) -> {
                  process.send(reply, Error(e))
                  actor.continue(state)
                }
              }
            }
            // Cache miss: Prepare, cache, then Execute
            Error(_) -> {
              let state = evict_if_needed(state)
              let stmt_name = "_pg_" <> int.to_string(state.next_id)
              case
                connection.prepare(
                  state.conn,
                  stmt_name,
                  sql,
                  [],
                  state.config.timeout,
                )
              {
                Ok(#(prepared, conn)) -> {
                  let new_cache = dict.insert(state.cache, sql, prepared)
                  let new_order =
                    list.append(state.cache_order, [sql])
                  case
                    connection.execute_prepared(
                      conn,
                      prepared,
                      params,
                      state.registry,
                      state.config.timeout,
                    )
                  {
                    Ok(#(result, conn2)) -> {
                      process.send(reply, Ok(result))
                      actor.continue(
                        ActorState(
                          ..state,
                          conn: conn2,
                          cache: new_cache,
                          cache_order: new_order,
                          next_id: state.next_id + 1,
                        ),
                      )
                    }
                    Error(e) -> {
                      process.send(reply, Error(e))
                      actor.continue(
                        ActorState(
                          ..state,
                          conn: conn,
                          cache: new_cache,
                          cache_order: new_order,
                          next_id: state.next_id + 1,
                        ),
                      )
                    }
                  }
                }
                Error(e) -> {
                  process.send(reply, Error(e))
                  actor.continue(state)
                }
              }
            }
          }
        }
      }
    }

    SimpleQuery(sql, timeout, reply) -> {
      case connection.simple_query(state.conn, sql, timeout) {
        Ok(#(results, conn)) -> {
          process.send(reply, Ok(results))
          actor.continue(ActorState(..state, conn: conn))
        }
        Error(e) -> {
          process.send(reply, Error(e))
          actor.continue(state)
        }
      }
    }

    Prepare(name, sql, reply) -> {
      case
        connection.prepare(state.conn, name, sql, [], state.config.timeout)
      {
        Ok(#(prepared, conn)) -> {
          process.send(reply, Ok(prepared))
          actor.continue(ActorState(..state, conn: conn))
        }
        Error(e) -> {
          process.send(reply, Error(e))
          actor.continue(state)
        }
      }
    }

    ExecutePrepared(prepared, params, reply) -> {
      case
        connection.execute_prepared(
          state.conn,
          prepared,
          params,
          state.registry,
          state.config.timeout,
        )
      {
        Ok(#(result, conn)) -> {
          process.send(reply, Ok(result))
          actor.continue(ActorState(..state, conn: conn))
        }
        Error(e) -> {
          process.send(reply, Error(e))
          actor.continue(state)
        }
      }
    }

    BatchQuery(sql, param_sets, reply) -> {
      case state.config.pgbouncer {
        // PgBouncer mode: unnamed pipeline
        True -> {
          case
            connection.extended_query_pipeline_unnamed(
              state.conn,
              sql,
              param_sets,
              state.registry,
              state.config.timeout,
            )
          {
            Ok(#(results, conn)) -> {
              process.send(reply, Ok(results))
              actor.continue(ActorState(..state, conn: conn))
            }
            Error(e) -> {
              process.send(reply, Error(e))
              actor.continue(state)
            }
          }
        }
        // Normal mode: named statement + pipeline
        False -> {
          case dict.get(state.cache, sql) {
            Ok(cached) -> {
              case
                connection.execute_pipeline(
                  state.conn,
                  cached,
                  param_sets,
                  state.registry,
                  state.config.timeout,
                )
              {
                Ok(#(results, conn)) -> {
                  process.send(reply, Ok(results))
                  actor.continue(ActorState(..state, conn: conn))
                }
                Error(e) -> {
                  process.send(reply, Error(e))
                  actor.continue(state)
                }
              }
            }
            Error(_) -> {
              let state = evict_if_needed(state)
              let stmt_name = "_pg_" <> int.to_string(state.next_id)
              case
                connection.prepare(
                  state.conn,
                  stmt_name,
                  sql,
                  [],
                  state.config.timeout,
                )
              {
                Ok(#(prepared, conn)) -> {
                  let new_cache = dict.insert(state.cache, sql, prepared)
                  let new_order =
                    list.append(state.cache_order, [sql])
                  case
                    connection.execute_pipeline(
                      conn,
                      prepared,
                      param_sets,
                      state.registry,
                      state.config.timeout,
                    )
                  {
                    Ok(#(results, conn2)) -> {
                      process.send(reply, Ok(results))
                      actor.continue(
                        ActorState(
                          ..state,
                          conn: conn2,
                          cache: new_cache,
                          cache_order: new_order,
                          next_id: state.next_id + 1,
                        ),
                      )
                    }
                    Error(e) -> {
                      process.send(reply, Error(e))
                      actor.continue(
                        ActorState(
                          ..state,
                          conn: conn,
                          cache: new_cache,
                          cache_order: new_order,
                          next_id: state.next_id + 1,
                        ),
                      )
                    }
                  }
                }
                Error(e) -> {
                  process.send(reply, Error(e))
                  actor.continue(state)
                }
              }
            }
          }
        }
      }
    }

    CloseStatement(name, reply) -> {
      case
        connection.close_statement(state.conn, name, state.config.timeout)
      {
        Ok(conn) -> {
          process.send(reply, Ok(Nil))
          actor.continue(ActorState(..state, conn: conn))
        }
        Error(e) -> {
          process.send(reply, Error(e))
          actor.continue(state)
        }
      }
    }

    CopyIn(sql, data, reply) -> {
      case copy.copy_in(state.conn, sql, data, state.config.timeout) {
        Ok(#(tag, conn)) -> {
          process.send(reply, Ok(tag))
          actor.continue(ActorState(..state, conn: conn))
        }
        Error(e) -> {
          process.send(reply, Error(e))
          actor.continue(state)
        }
      }
    }

    CopyOut(sql, reply) -> {
      case copy.copy_out(state.conn, sql, state.config.timeout) {
        Ok(#(data, conn)) -> {
          process.send(reply, Ok(data))
          actor.continue(ActorState(..state, conn: conn))
        }
        Error(e) -> {
          process.send(reply, Error(e))
          actor.continue(state)
        }
      }
    }

    Disconnect(reply) -> {
      connection.disconnect(state.conn)
      process.send(reply, Nil)
      actor.stop()
    }
  }
}

/// Evict the oldest cached statement if cache is at capacity.
fn evict_if_needed(state: ActorState) -> ActorState {
  case list.length(state.cache_order) >= max_cache_size {
    True -> {
      case state.cache_order {
        [oldest, ..rest] -> {
          // Close the statement on the server (best-effort)
          case dict.get(state.cache, oldest) {
            Ok(prepared) -> {
              let _ =
                connection.close_statement(
                  state.conn,
                  prepared.name,
                  state.config.timeout,
                )
              Nil
            }
            Error(_) -> Nil
          }
          ActorState(
            ..state,
            cache: dict.delete(state.cache, oldest),
            cache_order: rest,
          )
        }
        [] -> state
      }
    }
    False -> state
  }
}

fn error_to_string(err: Error) -> String {
  case err {
    error.PgError(fields, _, _) ->
      "PostgreSQL error: " <> fields.message
    error.ConnectionError(msg) -> "Connection error: " <> msg
    error.AuthenticationError(msg) -> "Authentication error: " <> msg
    error.EncodeError(msg) -> "Encode error: " <> msg
    error.DecodeError(msg) -> "Decode error: " <> msg
    error.ProtocolError(msg) -> "Protocol error: " <> msg
    error.SocketError(msg) -> "Socket error: " <> msg
    error.TimeoutError -> "Timeout"
  }
}
