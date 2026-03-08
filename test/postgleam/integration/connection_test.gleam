import gleam/dict
import gleam/option.{Some}
import gleeunit/should
import postgleam/config
import postgleam/connection

pub fn connect_default_user_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  // Should have a connection_id (backend PID)
  should.be_true(is_some(state.connection_id))
  // Should have server parameters
  should.be_true(dict.size(state.parameters) > 0)
  // Should have server_version parameter
  let assert Ok(_) = dict.get(state.parameters, "server_version")
  connection.disconnect(state)
}

pub fn connect_and_simple_query_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT 1 AS num", cfg.timeout)
  let assert [result] = results
  should.equal(result.tag, "SELECT 1")
  should.equal(result.columns, ["num"])
  should.equal(result.rows, [[Some("1")]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_null_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT NULL AS val", cfg.timeout)
  let assert [result] = results
  should.equal(result.rows, [[option.None]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_multiple_columns_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT 1 AS a, 'hello' AS b, true AS c", cfg.timeout)
  let assert [result] = results
  should.equal(result.columns, ["a", "b", "c"])
  should.equal(result.rows, [[Some("1"), Some("hello"), Some("t")]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_multiple_rows_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT generate_series(1,3) AS n", cfg.timeout)
  let assert [result] = results
  should.equal(result.rows, [[Some("1")], [Some("2")], [Some("3")]])
  connection.disconnect(state)
}

pub fn connect_and_simple_query_error_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)
  let assert Error(_) =
    connection.simple_query(state, "SELECT * FROM nonexistent_table_xyz", cfg.timeout)
}

pub fn connect_wrong_password_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
    |> config.password("wrong_password")
    |> config.username("postgleam_scram_pw")
  let assert Error(_) = connection.connect(cfg)
}

pub fn connect_scram_auth_test() {
  let cfg =
    config.Config(
      host: "localhost",
      port: 5432,
      database: "postgleam_test",
      username: "postgleam_scram_pw",
      password: "postgleam_scram_pw",
      timeout: 15_000,
      connect_timeout: 5000,
      extra_parameters: [],
      ssl: config.SslDisabled,
      pgbouncer: False,
      idle_interval: 1000,
      queue_timeout: 5000,
    )
  let assert Ok(state) = connection.connect(cfg)
  should.be_true(is_some(state.connection_id))
  connection.disconnect(state)
}

pub fn connect_multiple_queries_sequential_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(_, state)) =
    connection.simple_query(state, "SELECT 1", cfg.timeout)
  let assert Ok(#(_, state)) =
    connection.simple_query(state, "SELECT 2", cfg.timeout)
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT 3 AS val", cfg.timeout)

  let assert [result] = results
  should.equal(result.rows, [[Some("3")]])
  connection.disconnect(state)
}

/// When server doesn't support SSL, requesting it should give a clear error
pub fn connect_ssl_not_available_test() {
  let cfg =
    config.default()
    |> config.database("postgleam_test")
    |> config.ssl(config.SslUnverified)
  let assert Error(_) = connection.connect(cfg)
}

fn is_some(opt: option.Option(a)) -> Bool {
  case opt {
    Some(_) -> True
    option.None -> False
  }
}
