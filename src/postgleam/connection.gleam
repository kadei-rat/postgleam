/// Core PostgreSQL connection - TCP connect, authentication, protocol state machine

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import mug
import postgleam/auth/md5
import postgleam/auth/scram
import postgleam/codec/registry.{type Registry}
import postgleam/config.{type Config, SslDisabled, SslUnverified, SslVerified}
import postgleam/error.{type Error}
import postgleam/internal/transport.{type Transport}
import postgleam/message.{
  type BackendMessage, type RowField, type TransactionStatus,
  AuthCleartext, AuthMd5, AuthOk, AuthSasl, AuthSaslContinue, AuthSaslFinal,
  AuthenticationMsg, BackendKeyData, BindComplete, CloseComplete,
  CommandComplete, DataRow, Decoded, DecodeFailed, ErrorResponse,
  Incomplete, NoData, NoticeResponse, ParameterDescription, ParameterStatus,
  ParseComplete, PortalSuspended, ReadyForQuery, RowDescription,
}
import postgleam/value.{type Value}

/// Connection state
pub type ConnectionState {
  ConnectionState(
    transport: Transport,
    connection_id: Option(Int),
    connection_key: Option(Int),
    parameters: Dict(String, String),
    transaction_status: TransactionStatus,
    buffer: BitArray,
  )
}

/// Establish a connection to PostgreSQL, complete authentication, and return ready state
pub fn connect(config: Config) -> Result(ConnectionState, Error) {
  // TCP connect
  use tcp_socket <- result.try(
    mug.new(config.host, port: config.port)
    |> mug.timeout(milliseconds: config.connect_timeout)
    |> mug.connect()
    |> result.map_error(fn(e) {
      error.SocketError("TCP connect failed: " <> connect_error_to_string(e))
    }),
  )

  // SSL negotiation if requested (matching Postgrex's do_handshake flow)
  use conn_transport <- result.try(case config.ssl {
    SslDisabled -> Ok(transport.Tcp(tcp_socket))
    SslVerified ->
      transport.upgrade_to_ssl(tcp_socket, config.host, config.connect_timeout, True)
    SslUnverified ->
      transport.upgrade_to_ssl(tcp_socket, config.host, config.connect_timeout, False)
  })

  let state =
    ConnectionState(
      transport: conn_transport,
      connection_id: None,
      connection_key: None,
      parameters: dict.new(),
      transaction_status: message.Idle,
      buffer: <<>>,
    )

  // Send startup message
  let base_params = [
    #("user", config.username),
    #("database", config.database),
  ]
  let startup =
    message.StartupMessage(params: list.append(base_params, config.extra_parameters))
  use state <- result.try(send_message(state, startup))

  // Process authentication and startup
  authenticate_loop(state, config)
}

/// Send a frontend message over the transport
pub fn send_message(
  state: ConnectionState,
  msg: message.FrontendMessage,
) -> Result(ConnectionState, Error) {
  let bytes = message.encode_frontend(msg)
  case transport.send(state.transport, bytes) {
    Ok(_) -> Ok(state)
    Error(e) -> Error(e)
  }
}

/// Send raw bytes over the transport
pub fn send_bytes(
  state: ConnectionState,
  bytes: BitArray,
) -> Result(ConnectionState, Error) {
  case transport.send(state.transport, bytes) {
    Ok(_) -> Ok(state)
    Error(e) -> Error(e)
  }
}

/// Receive and decode the next backend message
pub fn receive_message(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(BackendMessage, ConnectionState), Error) {
  receive_message_loop(state, timeout)
}

fn receive_message_loop(
  state: ConnectionState,
  timeout: Int,
) -> Result(#(BackendMessage, ConnectionState), Error) {
  // Try to decode from existing buffer first
  case message.decode_backend(state.buffer) {
    Decoded(msg, rest) ->
      Ok(#(msg, ConnectionState(..state, buffer: rest)))
    DecodeFailed(reason) ->
      Error(error.ProtocolError("Decode failed: " <> reason))
    Incomplete -> {
      // Need more data
      case transport.receive(state.transport, timeout) {
        Ok(data) -> {
          let new_buffer = <<state.buffer:bits, data:bits>>
          receive_message_loop(
            ConnectionState(..state, buffer: new_buffer),
            timeout,
          )
        }
        Error(e) -> Error(e)
      }
    }
  }
}

/// Execute a simple query (text protocol) and return all results
pub fn simple_query(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(SimpleQueryResult), ConnectionState), Error) {
  use state <- result.try(send_message(state, message.SimpleQuery(sql)))
  simple_query_loop(state, sql, timeout, [], None, [])
}

/// Result from a simple query
pub type SimpleQueryResult {
  SimpleQueryResult(
    tag: String,
    columns: List(String),
    rows: List(List(Option(String))),
  )
}

fn simple_query_loop(
  state: ConnectionState,
  sql: String,
  timeout: Int,
  results: List(SimpleQueryResult),
  current_columns: Option(List(String)),
  current_rows: List(List(Option(String))),
) -> Result(#(List(SimpleQueryResult), ConnectionState), Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    ReadyForQuery(status) -> {
      let state = ConnectionState(..state, transaction_status: status)
      Ok(#(list_reverse(results), state))
    }
    RowDescription(fields) -> {
      let columns = extract_column_names(fields)
      simple_query_loop(state, sql, timeout, results, Some(columns), [])
    }
    DataRow(values) -> {
      let row = decode_text_row(values)
      simple_query_loop(state, sql, timeout, results, current_columns, [row, ..current_rows])
    }
    CommandComplete(tag) -> {
      let cols = case current_columns {
        Some(c) -> c
        None -> []
      }
      let result =
        SimpleQueryResult(
          tag: tag,
          columns: cols,
          rows: list_reverse(current_rows),
        )
      simple_query_loop(state, sql, timeout, [result, ..results], None, [])
    }
    message.EmptyQueryResponse -> {
      simple_query_loop(state, sql, timeout, results, current_columns, current_rows)
    }
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      // Drain to ReadyForQuery so the connection is clean for the next query
      let _ = drain_to_ready(state, timeout)
      Error(error.PgError(fields: pg_fields, connection_id: state.connection_id, query: Some(sql)))
    }
    NoticeResponse(_) -> {
      simple_query_loop(state, sql, timeout, results, current_columns, current_rows)
    }
    message.NotificationResponse(_, _, _) -> {
      simple_query_loop(state, sql, timeout, results, current_columns, current_rows)
    }
    _ -> {
      Error(error.ProtocolError("Unexpected message during simple query"))
    }
  }
}

// =============================================================================
// Extended Query Protocol
// =============================================================================

/// A prepared statement returned by `prepare`
pub type PreparedStatement {
  PreparedStatement(
    name: String,
    statement: String,
    param_oids: List(Int),
    result_fields: List(RowField),
  )
}

/// Result from an extended query execution
pub type ExtendedQueryResult {
  ExtendedQueryResult(
    tag: String,
    columns: List(RowField),
    rows: List(List(Option(Value))),
  )
}

/// Prepare a statement: Parse + Describe + Sync
/// Returns the prepared statement with parameter and result type info.
pub fn prepare(
  state: ConnectionState,
  name: String,
  sql: String,
  type_oids: List(Int),
  timeout: Int,
) -> Result(#(PreparedStatement, ConnectionState), Error) {
  // Send Parse + Describe(Statement) + Sync
  use state <- result.try(send_message(
    state,
    message.Parse(name, sql, type_oids),
  ))
  use state <- result.try(send_message(
    state,
    message.Describe(message.DescribeStatement, name),
  ))
  use state <- result.try(send_message(state, message.Sync))

  // Receive ParseComplete
  use state <- result.try(recv_parse_complete(state, sql, timeout))
  // Receive ParameterDescription
  use #(param_oids_result, state) <- result.try(
    recv_parameter_description(state, sql, timeout),
  )
  // Receive RowDescription or NoData
  use #(result_fields, state) <- result.try(
    recv_row_description_or_nodata(state, sql, timeout),
  )
  // Receive ReadyForQuery
  use state <- result.try(recv_ready_for_query(state, sql, timeout))

  let prepared =
    PreparedStatement(
      name: name,
      statement: sql,
      param_oids: param_oids_result,
      result_fields: result_fields,
    )
  Ok(#(prepared, state))
}

/// Execute a prepared statement with binary parameters: Bind + Execute + Sync
/// Parameters are encoded using the codec registry.
pub fn execute_prepared(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  // Encode parameters
  use encoded_params <- result.try(
    encode_params_binary(params, prepared.param_oids, registry),
  )

  // Build format lists
  let param_formats =
    list.map(prepared.param_oids, fn(_) { message.BinaryFormat })
  let result_formats =
    list.map(prepared.result_fields, fn(_) { message.BinaryFormat })

  // Send Bind + Execute + Sync
  use state <- result.try(send_message(
    state,
    message.Bind("", prepared.name, param_formats, encoded_params, result_formats),
  ))
  use state <- result.try(send_message(
    state,
    message.Execute("", 0),
  ))
  use state <- result.try(send_message(state, message.Sync))

  // Receive BindComplete
  use state <- result.try(
    recv_bind_complete(state, prepared.statement, timeout),
  )
  // Resolve decoders once for all rows
  let decoders = resolve_row_decoders(prepared.result_fields, registry)
  // Receive DataRows + CommandComplete
  use #(tag, rows, state) <- result.try(
    recv_execute_rows(state, prepared, decoders, timeout, []),
  )
  // Receive ReadyForQuery
  use state <- result.try(
    recv_ready_for_query(state, prepared.statement, timeout),
  )

  Ok(#(
    ExtendedQueryResult(
      tag: tag,
      columns: prepared.result_fields,
      rows: list_reverse(rows),
    ),
    state,
  ))
}

/// Execute a prepared statement with multiple param sets in a pipeline.
/// Sends all Bind+Execute messages, then a single Sync — avoiding N-1
/// round-trips compared to calling execute_prepared N times.
pub fn execute_pipeline(
  state: ConnectionState,
  prepared: PreparedStatement,
  param_sets: List(List(Option(Value))),
  registry: Registry,
  timeout: Int,
) -> Result(#(List(ExtendedQueryResult), ConnectionState), Error) {
  let param_formats =
    list.map(prepared.param_oids, fn(_) { message.BinaryFormat })
  let result_formats =
    list.map(prepared.result_fields, fn(_) { message.BinaryFormat })

  // Send all Bind+Execute pairs without Sync
  use state <- result.try(send_pipeline_messages(
    state,
    prepared,
    param_sets,
    param_formats,
    result_formats,
    registry,
  ))
  // Single Sync at the end
  use state <- result.try(send_message(state, message.Sync))

  // Read all results
  let decoders = resolve_row_decoders(prepared.result_fields, registry)
  let count = list.length(param_sets)
  use #(results, state) <- result.try(recv_pipeline_results(
    state,
    prepared,
    decoders,
    timeout,
    count,
    [],
  ))
  // Read final ReadyForQuery
  use state <- result.try(
    recv_ready_for_query(state, prepared.statement, timeout),
  )
  Ok(#(results, state))
}

fn send_pipeline_messages(
  state: ConnectionState,
  prepared: PreparedStatement,
  param_sets: List(List(Option(Value))),
  param_formats: List(message.Format),
  result_formats: List(message.Format),
  registry: Registry,
) -> Result(ConnectionState, Error) {
  case param_sets {
    [] -> Ok(state)
    [params, ..rest] -> {
      use encoded_params <- result.try(
        encode_params_binary(params, prepared.param_oids, registry),
      )
      use state <- result.try(send_message(
        state,
        message.Bind(
          "",
          prepared.name,
          param_formats,
          encoded_params,
          result_formats,
        ),
      ))
      use state <- result.try(send_message(state, message.Execute("", 0)))
      send_pipeline_messages(
        state,
        prepared,
        rest,
        param_formats,
        result_formats,
        registry,
      )
    }
  }
}

fn recv_pipeline_results(
  state: ConnectionState,
  prepared: PreparedStatement,
  decoders: List(fn(BitArray) -> Result(Value, String)),
  timeout: Int,
  remaining: Int,
  acc: List(ExtendedQueryResult),
) -> Result(#(List(ExtendedQueryResult), ConnectionState), Error) {
  case remaining {
    0 -> Ok(#(list_reverse(acc), state))
    _ -> {
      // Each result: BindComplete + DataRow* + CommandComplete
      use state <- result.try(
        recv_bind_complete(state, prepared.statement, timeout),
      )
      use #(tag, rows, state) <- result.try(
        recv_execute_rows(state, prepared, decoders, timeout, []),
      )
      let result =
        ExtendedQueryResult(
          tag: tag,
          columns: prepared.result_fields,
          rows: list_reverse(rows),
        )
      recv_pipeline_results(
        state,
        prepared,
        decoders,
        timeout,
        remaining - 1,
        [result, ..acc],
      )
    }
  }
}

/// Close a prepared statement: Close + Sync
pub fn close_statement(
  state: ConnectionState,
  name: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use state <- result.try(send_message(
    state,
    message.Close(message.DescribeStatement, name),
  ))
  use state <- result.try(send_message(state, message.Sync))

  // Receive CloseComplete
  use state <- result.try(recv_close_complete(state, timeout))
  // Receive ReadyForQuery
  recv_ready_for_query(state, "", timeout)
}

/// All-in-one extended query: Parse + Describe + Bind + Execute + Close + Sync
/// Uses unnamed statement and portal for simplicity.
pub fn extended_query(
  state: ConnectionState,
  sql: String,
  params: List(Option(Value)),
  registry: Registry,
  timeout: Int,
) -> Result(#(ExtendedQueryResult, ConnectionState), Error) {
  // Step 1: Parse + Describe + Sync to get type info
  use state <- result.try(send_message(
    state,
    message.Parse("", sql, []),
  ))
  use state <- result.try(send_message(
    state,
    message.Describe(message.DescribeStatement, ""),
  ))
  use state <- result.try(send_message(state, message.Sync))

  use state <- result.try(recv_parse_complete(state, sql, timeout))
  use #(param_oids, state) <- result.try(
    recv_parameter_description(state, sql, timeout),
  )
  use #(result_fields, state) <- result.try(
    recv_row_description_or_nodata(state, sql, timeout),
  )
  use state <- result.try(recv_ready_for_query(state, sql, timeout))

  // Step 2: Encode params and Bind + Execute + Close + Sync
  use encoded_params <- result.try(
    encode_params_binary(params, param_oids, registry),
  )

  let param_formats = list.map(param_oids, fn(_) { message.BinaryFormat })
  let result_formats =
    list.map(result_fields, fn(_) { message.BinaryFormat })

  let prepared =
    PreparedStatement(
      name: "",
      statement: sql,
      param_oids: param_oids,
      result_fields: result_fields,
    )

  use state <- result.try(send_message(
    state,
    message.Bind("", "", param_formats, encoded_params, result_formats),
  ))
  use state <- result.try(send_message(state, message.Execute("", 0)))
  use state <- result.try(send_message(
    state,
    message.Close(message.DescribeStatement, ""),
  ))
  use state <- result.try(send_message(state, message.Sync))

  use state <- result.try(
    recv_bind_complete(state, sql, timeout),
  )
  let decoders = resolve_row_decoders(prepared.result_fields, registry)
  use #(tag, rows, state) <- result.try(
    recv_execute_rows(state, prepared, decoders, timeout, []),
  )
  use state <- result.try(recv_close_complete(state, timeout))
  use state <- result.try(recv_ready_for_query(state, sql, timeout))

  Ok(#(
    ExtendedQueryResult(
      tag: tag,
      columns: result_fields,
      rows: list_reverse(rows),
    ),
    state,
  ))
}

/// Result of a streamed execute - either more rows available or done
pub type StreamChunk {
  /// More rows available, call execute_portal again
  StreamMore(rows: List(List(Option(Value))))
  /// All rows received, includes the command tag
  StreamDone(tag: String, rows: List(List(Option(Value))))
}

/// Bind parameters and execute first chunk: Bind + Execute(max_rows) + Flush
/// Uses Flush instead of Sync to keep the unnamed portal alive.
/// Returns the first chunk of rows.
pub fn bind_and_execute_portal(
  state: ConnectionState,
  prepared: PreparedStatement,
  params: List(Option(Value)),
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Error) {
  use encoded_params <- result.try(
    encode_params_binary(params, prepared.param_oids, registry),
  )
  let param_formats =
    list.map(prepared.param_oids, fn(_) { message.BinaryFormat })
  let result_formats =
    list.map(prepared.result_fields, fn(_) { message.BinaryFormat })

  use state <- result.try(send_message(
    state,
    message.Bind("", prepared.name, param_formats, encoded_params, result_formats),
  ))
  use state <- result.try(send_message(
    state,
    message.Execute("", max_rows),
  ))
  use state <- result.try(send_message(state, message.Flush))
  use state <- result.try(
    recv_bind_complete(state, prepared.statement, timeout),
  )
  let decoders = resolve_row_decoders(prepared.result_fields, registry)
  recv_stream_rows(state, prepared, decoders, timeout, [])
}

/// Execute the next chunk from a portal: Execute(max_rows) + Flush
/// Returns StreamMore if portal is suspended (more rows), StreamDone if complete.
pub fn execute_portal(
  state: ConnectionState,
  prepared: PreparedStatement,
  registry: Registry,
  max_rows: Int,
  timeout: Int,
) -> Result(#(StreamChunk, ConnectionState), Error) {
  use state <- result.try(send_message(
    state,
    message.Execute("", max_rows),
  ))
  use state <- result.try(send_message(state, message.Flush))
  let decoders = resolve_row_decoders(prepared.result_fields, registry)
  recv_stream_rows(state, prepared, decoders, timeout, [])
}

/// Finalize portal streaming: Sync to get ReadyForQuery
pub fn sync_portal(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use state <- result.try(send_message(state, message.Sync))
  recv_ready_for_query(state, "", timeout)
}

// --- Extended query response helpers ---

fn recv_parse_complete(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    ParseComplete -> Ok(state)
    ErrorResponse(fields) -> {
      // Need to drain until ReadyForQuery
      let pg_fields = error.parse_error_fields(fields)
      let _ = drain_to_ready(state, timeout)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: Some(sql),
      ))
    }
    NoticeResponse(_) -> recv_parse_complete(state, sql, timeout)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_parse_complete(state, sql, timeout)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected ParseComplete, got unexpected message",
      ))
  }
}

fn recv_parameter_description(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(Int), ConnectionState), Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    ParameterDescription(type_oids) -> Ok(#(type_oids, state))
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      let _ = drain_to_ready(state, timeout)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: Some(sql),
      ))
    }
    NoticeResponse(_) -> recv_parameter_description(state, sql, timeout)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_parameter_description(state, sql, timeout)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected ParameterDescription, got unexpected message",
      ))
  }
}

fn recv_row_description_or_nodata(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(#(List(RowField), ConnectionState), Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    RowDescription(fields) -> Ok(#(fields, state))
    NoData -> Ok(#([], state))
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      let _ = drain_to_ready(state, timeout)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: Some(sql),
      ))
    }
    NoticeResponse(_) ->
      recv_row_description_or_nodata(state, sql, timeout)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_row_description_or_nodata(state, sql, timeout)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected RowDescription or NoData, got unexpected message",
      ))
  }
}

fn recv_bind_complete(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    BindComplete -> Ok(state)
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      let _ = drain_to_ready(state, timeout)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: Some(sql),
      ))
    }
    NoticeResponse(_) -> recv_bind_complete(state, sql, timeout)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_bind_complete(state, sql, timeout)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected BindComplete, got unexpected message",
      ))
  }
}

fn recv_execute_rows(
  state: ConnectionState,
  prepared: PreparedStatement,
  decoders: List(fn(BitArray) -> Result(Value, String)),
  timeout: Int,
  rows: List(List(Option(Value))),
) -> Result(#(String, List(List(Option(Value))), ConnectionState), Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    DataRow(values) -> {
      let row = decode_binary_row(values, decoders)
      recv_execute_rows(state, prepared, decoders, timeout, [row, ..rows])
    }
    CommandComplete(tag) -> Ok(#(tag, rows, state))
    message.EmptyQueryResponse -> Ok(#("", rows, state))
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      let _ = drain_to_ready(state, timeout)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: Some(prepared.statement),
      ))
    }
    NoticeResponse(_) ->
      recv_execute_rows(state, prepared, decoders, timeout, rows)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_execute_rows(state, prepared, decoders, timeout, rows)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected DataRow or CommandComplete, got unexpected message",
      ))
  }
}

fn recv_stream_rows(
  state: ConnectionState,
  prepared: PreparedStatement,
  decoders: List(fn(BitArray) -> Result(Value, String)),
  timeout: Int,
  rows: List(List(Option(Value))),
) -> Result(#(StreamChunk, ConnectionState), Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    DataRow(values) -> {
      let row = decode_binary_row(values, decoders)
      recv_stream_rows(state, prepared, decoders, timeout, [row, ..rows])
    }
    CommandComplete(tag) -> Ok(#(StreamDone(tag, list_reverse(rows)), state))
    PortalSuspended -> Ok(#(StreamMore(list_reverse(rows)), state))
    message.EmptyQueryResponse ->
      Ok(#(StreamDone("", list_reverse(rows)), state))
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      let _ = drain_to_ready(state, timeout)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: Some(prepared.statement),
      ))
    }
    NoticeResponse(_) ->
      recv_stream_rows(state, prepared, decoders, timeout, rows)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_stream_rows(state, prepared, decoders, timeout, rows)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected DataRow, CommandComplete, or PortalSuspended",
      ))
  }
}

fn recv_close_complete(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    CloseComplete -> Ok(state)
    NoticeResponse(_) -> recv_close_complete(state, timeout)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_close_complete(state, timeout)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected CloseComplete, got unexpected message",
      ))
  }
}

fn recv_ready_for_query(
  state: ConnectionState,
  sql: String,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    ReadyForQuery(status) ->
      Ok(ConnectionState(..state, transaction_status: status))
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: Some(sql),
      ))
    }
    NoticeResponse(_) -> recv_ready_for_query(state, sql, timeout)
    ParameterStatus(name: name, value: val) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, val),
        )
      recv_ready_for_query(state, sql, timeout)
    }
    _ ->
      Error(error.ProtocolError(
        "Expected ReadyForQuery, got unexpected message",
      ))
  }
}

/// Drain messages until ReadyForQuery (for error recovery)
fn drain_to_ready(
  state: ConnectionState,
  timeout: Int,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, timeout))
  case msg {
    ReadyForQuery(status) ->
      Ok(ConnectionState(..state, transaction_status: status))
    _ -> drain_to_ready(state, timeout)
  }
}

// --- Binary encode/decode helpers ---

/// Encode parameters to binary using the codec registry
fn encode_params_binary(
  params: List(Option(Value)),
  param_oids: List(Int),
  reg: Registry,
) -> Result(List(Option(BitArray)), Error) {
  encode_params_loop(params, param_oids, reg, [])
}

fn encode_params_loop(
  params: List(Option(Value)),
  oids: List(Int),
  reg: Registry,
  acc: List(Option(BitArray)),
) -> Result(List(Option(BitArray)), Error) {
  case params, oids {
    [], [] -> Ok(list_reverse(acc))
    [None, ..rest_params], [_, ..rest_oids] ->
      encode_params_loop(rest_params, rest_oids, reg, [None, ..acc])
    [Some(val), ..rest_params], [oid, ..rest_oids] -> {
      case registry.lookup(reg, oid) {
        Ok(codec) ->
          case codec.encode(val) {
            Ok(bytes) ->
              encode_params_loop(
                rest_params,
                rest_oids,
                reg,
                [Some(bytes), ..acc],
              )
            Error(e) -> Error(error.EncodeError(e))
          }
        Error(e) -> Error(error.EncodeError(e))
      }
    }
    _, _ ->
      Error(error.EncodeError(
        "Parameter count mismatch: params and OIDs have different lengths",
      ))
  }
}

/// Resolve codecs for result fields once, to avoid per-row registry lookups
fn resolve_row_decoders(
  fields: List(RowField),
  reg: Registry,
) -> List(fn(BitArray) -> Result(Value, String)) {
  list.map(fields, fn(field) {
    case registry.lookup(reg, field.type_oid) {
      Ok(codec) -> codec.decode
      Error(_) -> fn(bytes) {
        case bit_array.to_string(bytes) {
          Ok(s) -> Ok(value.Text(s))
          Error(_) -> Ok(value.Bytea(bytes))
        }
      }
    }
  })
}

/// Decode a binary DataRow using pre-resolved decoders
fn decode_binary_row(
  values: BitArray,
  decoders: List(fn(BitArray) -> Result(Value, String)),
) -> List(Option(Value)) {
  let raw_values = message.extract_row_values(values)
  decode_values_with_decoders(raw_values, decoders)
}

fn decode_values_with_decoders(
  raw_values: List(Option(BitArray)),
  decoders: List(fn(BitArray) -> Result(Value, String)),
) -> List(Option(Value)) {
  case raw_values, decoders {
    [], [] -> []
    [None, ..rest_vals], [_, ..rest_decoders] ->
      [None, ..decode_values_with_decoders(rest_vals, rest_decoders)]
    [Some(bytes), ..rest_vals], [decoder, ..rest_decoders] -> {
      let decoded = case decoder(bytes) {
        Ok(val) -> Some(val)
        Error(_) -> Some(value.Text("<decode error>"))
      }
      [decoded, ..decode_values_with_decoders(rest_vals, rest_decoders)]
    }
    _, _ -> []
  }
}

// --- Authentication ---

fn authenticate_loop(
  state: ConnectionState,
  config: Config,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  case msg {
    AuthenticationMsg(AuthOk) -> startup_loop(state, config)
    AuthenticationMsg(AuthCleartext) -> {
      use state <- result.try(
        send_message(state, message.PasswordMessage(config.password)),
      )
      authenticate_loop(state, config)
    }
    AuthenticationMsg(AuthMd5(salt: salt)) -> {
      let hash = md5.hash_password(config.password, config.username, salt)
      use state <- result.try(send_message(state, message.PasswordMessage(hash)))
      authenticate_loop(state, config)
    }
    AuthenticationMsg(AuthSasl(mechanisms: _mechanisms)) -> {
      let #(mechanism, client_first_data) = scram.client_first()
      let assert Ok(client_first_bare) =
        scram.extract_client_first_bare(client_first_data)
      use state <- result.try(
        send_message(
          state,
          message.SASLInitialResponse(mechanism, client_first_data),
        ),
      )
      scram_continue_loop(state, config, client_first_bare)
    }
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.AuthenticationError(
        "Authentication failed: " <> pg_fields.message,
      ))
    }
    _ -> Error(error.ProtocolError("Unexpected message during authentication"))
  }
}

fn scram_continue_loop(
  state: ConnectionState,
  config: Config,
  client_first_bare: String,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  case msg {
    AuthenticationMsg(AuthSaslContinue(data: server_first)) -> {
      case scram.client_final(server_first, client_first_bare, config.password) {
        Ok(#(client_final, scram_state)) -> {
          use state <- result.try(
            send_message(state, message.SASLResponse(client_final)),
          )
          scram_final_loop(state, config, scram_state)
        }
        Error(e) -> Error(error.AuthenticationError("SCRAM error: " <> e))
      }
    }
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.AuthenticationError(pg_fields.message))
    }
    _ ->
      Error(error.ProtocolError(
        "Expected SASL continue, got unexpected message",
      ))
  }
}

fn scram_final_loop(
  state: ConnectionState,
  config: Config,
  scram_state: scram.ScramState,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  case msg {
    AuthenticationMsg(AuthSaslFinal(data: server_final)) -> {
      case scram.verify_server(server_final, scram_state) {
        Ok(_) -> authenticate_loop(state, config)
        Error(e) -> Error(error.AuthenticationError(e))
      }
    }
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.AuthenticationError(pg_fields.message))
    }
    _ ->
      Error(error.ProtocolError(
        "Expected SASL final, got unexpected message",
      ))
  }
}

/// Process startup messages after authentication until ReadyForQuery
fn startup_loop(
  state: ConnectionState,
  config: Config,
) -> Result(ConnectionState, Error) {
  use #(msg, state) <- result.try(receive_message(state, config.timeout))
  case msg {
    ReadyForQuery(status) ->
      Ok(ConnectionState(..state, transaction_status: status))
    ParameterStatus(name: name, value: value) -> {
      let state =
        ConnectionState(
          ..state,
          parameters: dict.insert(state.parameters, name, value),
        )
      startup_loop(state, config)
    }
    BackendKeyData(pid: pid, key: key) -> {
      let state =
        ConnectionState(
          ..state,
          connection_id: Some(pid),
          connection_key: Some(key),
        )
      startup_loop(state, config)
    }
    NoticeResponse(_) -> startup_loop(state, config)
    ErrorResponse(fields) -> {
      let pg_fields = error.parse_error_fields(fields)
      Error(error.PgError(
        fields: pg_fields,
        connection_id: state.connection_id,
        query: None,
      ))
    }
    _ -> Error(error.ProtocolError("Unexpected message during startup"))
  }
}

/// Close the connection
pub fn disconnect(state: ConnectionState) -> Nil {
  let _ = send_message(state, message.Terminate)
  transport.close(state.transport)
}

// --- Helpers ---

fn extract_column_names(fields: List(message.RowField)) -> List(String) {
  case fields {
    [] -> []
    [f, ..rest] -> [f.name, ..extract_column_names(rest)]
  }
}

fn decode_text_row(values: BitArray) -> List(Option(String)) {
  message.extract_row_values(values)
  |> list_map(fn(v) {
    case v {
      Some(bytes) -> {
        case bit_array.to_string(bytes) {
          Ok(s) -> Some(s)
          Error(_) -> Some("<binary>")
        }
      }
      None -> None
    }
  })
}

fn connect_error_to_string(err: mug.ConnectError) -> String {
  case err {
    mug.ConnectFailedIpv4(_) -> "connection failed (IPv4)"
    mug.ConnectFailedIpv6(_) -> "connection failed (IPv6)"
    mug.ConnectFailedBoth(_, _) -> "connection failed (IPv4 and IPv6)"
  }
}

fn list_reverse(l: List(a)) -> List(a) {
  list_reverse_loop(l, [])
}

fn list_reverse_loop(l: List(a), acc: List(a)) -> List(a) {
  case l {
    [] -> acc
    [x, ..rest] -> list_reverse_loop(rest, [x, ..acc])
  }
}

fn list_map(l: List(a), f: fn(a) -> b) -> List(b) {
  case l {
    [] -> []
    [x, ..rest] -> [f(x), ..list_map(rest, f)]
  }
}
