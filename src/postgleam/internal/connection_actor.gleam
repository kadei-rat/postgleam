/// OTP actor wrapping a PostgreSQL connection.
/// Handles sequential query execution through typed messages.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
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
  /// Execute a simple (text) query
  SimpleQuery(
    sql: String,
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

/// Actor state
pub type ActorState {
  ActorState(
    conn: ConnectionState,
    config: Config,
    registry: Registry,
    cache: Dict(String, PreparedStatement),
    next_id: Int,
  )
}

/// Start the connection actor
pub fn start(
  config: Config,
) -> Result(actor.Started(Subject(Message)), actor.StartError) {
  actor.new_with_initialiser(config.connect_timeout + 1000, fn(subject) {
    case connection.connect(config) {
      Ok(conn) -> {
        let reg = defaults.build_registry()
        let state =
          ActorState(
            conn: conn,
            config: config,
            registry: reg,
            cache: dict.new(),
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

    SimpleQuery(sql, reply) -> {
      case connection.simple_query(state.conn, sql, state.config.timeout) {
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
      // Ensure the statement is cached
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
