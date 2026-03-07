/// Comprehensive smoke test that exercises every major postgleam feature
/// against a live PostgreSQL instance. Each test is self-contained.

import gleam/option.{None, Some}
import gleeunit/should
import postgleam
import postgleam/codec/defaults
import postgleam/codec/registry as codec_registry
import postgleam/config
import postgleam/connection
import postgleam/copy
import postgleam/notifications
import postgleam/pool
import postgleam/replication
import postgleam/stream
import postgleam/value

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

fn build_registry() {
  codec_registry.build(defaults.matchers())
}

// =========================================================================
// 1. Public API (postgleam module) — connect, query, transaction, disconnect
// =========================================================================

pub fn smoke_public_api_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  // Parameterized query
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::int4 + $2::int4 AS sum", [
      Some(value.Integer(17)),
      Some(value.Integer(25)),
    ])
  should.equal(result.rows, [[Some(value.Integer(42))]])

  // Simple query
  let assert Ok(results) = postgleam.simple_query(conn, "SELECT 'hello' AS msg")
  let assert [r] = results
  should.equal(r.rows, [[Some("hello")]])

  postgleam.disconnect(conn)
}

pub fn smoke_transaction_commit_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  // Use TEMP TABLE to avoid replication/replica identity issues
  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TEMP TABLE _smoke_tx (id int)",
    )

  // Transaction that commits
  let assert Ok(42) =
    postgleam.transaction(conn, fn(conn) {
      let assert Ok(_) =
        postgleam.query(conn, "INSERT INTO _smoke_tx VALUES ($1::int4)", [
          Some(value.Integer(1)),
        ])
      Ok(42)
    })

  // Verify row persisted
  let assert Ok(result) =
    postgleam.query(conn, "SELECT count(*)::int4 FROM _smoke_tx", [])
  should.equal(result.rows, [[Some(value.Integer(1))]])

  postgleam.disconnect(conn)
}

pub fn smoke_transaction_rollback_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) =
    postgleam.simple_query(
      conn,
      "CREATE TEMP TABLE _smoke_tx2 (id int)",
    )

  // Transaction that rolls back
  let assert Error(_) =
    postgleam.transaction(conn, fn(conn) {
      let assert Ok(_) =
        postgleam.query(conn, "INSERT INTO _smoke_tx2 VALUES ($1::int4)", [
          Some(value.Integer(99)),
        ])
      Error(postgleam.query_error("intentional rollback"))
    })

  // Verify row NOT persisted
  let assert Ok(result) =
    postgleam.query(conn, "SELECT count(*)::int4 FROM _smoke_tx2", [])
  should.equal(result.rows, [[Some(value.Integer(0))]])

  postgleam.disconnect(conn)
}

// =========================================================================
// 2. Low-level connection — direct connect, simple query, extended query
// =========================================================================

pub fn smoke_low_level_connection_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  // Simple query
  let assert Ok(#(results, state)) =
    connection.simple_query(state, "SELECT version()", cfg.timeout)
  let assert [r] = results
  should.equal(r.tag, "SELECT 1")

  // Extended query with typed params and results
  let assert Ok(#(result, state)) =
    connection.extended_query(
      state,
      "SELECT $1::bool, $2::text, $3::float8",
      [Some(value.Boolean(True)), Some(value.Text("gleam")), Some(value.Float(3.14))],
      reg,
      cfg.timeout,
    )
  let assert [[Some(value.Boolean(True)), Some(value.Text("gleam")), Some(value.Float(f))]] =
    result.rows
  // Float comparison
  should.be_true(f >. 3.13 && f <. 3.15)

  connection.disconnect(state)
}

// =========================================================================
// 3. Type round-trips — every codec family through PostgreSQL
// =========================================================================

pub fn smoke_type_roundtrip_int_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(r, state)) =
    connection.extended_query(
      state,
      "SELECT $1::int2, $2::int4, $3::int8",
      [Some(value.Integer(123)), Some(value.Integer(456_789)), Some(value.Integer(9_876_543_210))],
      reg,
      cfg.timeout,
    )
  should.equal(r.rows, [
    [Some(value.Integer(123)), Some(value.Integer(456_789)), Some(value.Integer(9_876_543_210))],
  ])

  connection.disconnect(state)
}

pub fn smoke_type_roundtrip_text_bytea_uuid_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(r, state)) =
    connection.extended_query(
      state,
      "SELECT $1::text, $2::bytea, $3::uuid",
      [
        Some(value.Text("hello world")),
        Some(value.Bytea(<<0xDE, 0xAD, 0xBE, 0xEF>>)),
        Some(value.Uuid(<<
          0xA0, 0xEE, 0xBC, 0x99, 0x9C, 0x0B, 0x4E, 0xF8, 0xBB, 0x6D, 0x6B,
          0xB9, 0xBD, 0x38, 0x0A, 0x11,
        >>)),
      ],
      reg,
      cfg.timeout,
    )
  let assert [[Some(value.Text("hello world")), Some(value.Bytea(b)), Some(value.Uuid(_))]] =
    r.rows
  should.equal(b, <<0xDE, 0xAD, 0xBE, 0xEF>>)

  connection.disconnect(state)
}

pub fn smoke_type_roundtrip_date_time_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  // date: 2024-01-15 = days since 2000-01-01 = 8780
  // timestamp: some microseconds value
  let assert Ok(#(r, state)) =
    connection.extended_query(
      state,
      "SELECT $1::date, $2::timestamp, $3::interval",
      [
        Some(value.Date(8780)),
        Some(value.Timestamp(756_864_000_000_000)),
        Some(value.Interval(microseconds: 3_600_000_000, days: 1, months: 2)),
      ],
      reg,
      cfg.timeout,
    )
  let assert [[Some(value.Date(8780)), Some(value.Timestamp(756_864_000_000_000)), Some(value.Interval(microseconds: 3_600_000_000, days: 1, months: 2))]] =
    r.rows

  connection.disconnect(state)
}

pub fn smoke_type_roundtrip_json_numeric_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(r, state)) =
    connection.extended_query(
      state,
      "SELECT $1::json, $2::jsonb, $3::numeric",
      [
        Some(value.Json("{\"key\":\"value\"}")),
        Some(value.Jsonb("{\"num\":42}")),
        Some(value.Numeric("123456.789")),
      ],
      reg,
      cfg.timeout,
    )
  let assert [[Some(value.Json(_)), Some(value.Jsonb(_)), Some(value.Numeric(n))]] = r.rows
  should.equal(n, "123456.789")

  connection.disconnect(state)
}

pub fn smoke_type_null_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(r, state)) =
    connection.extended_query(
      state,
      "SELECT $1::int4, $2::text",
      [None, None],
      reg,
      cfg.timeout,
    )
  should.equal(r.rows, [[None, None]])

  connection.disconnect(state)
}

// =========================================================================
// 4. Prepared statements
// =========================================================================

pub fn smoke_prepared_statement_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(state, "my_stmt", "SELECT $1::int4 * 2 AS doubled", [], cfg.timeout)

  // Execute multiple times
  let assert Ok(#(r1, state)) =
    connection.execute_prepared(state, prepared, [Some(value.Integer(5))], reg, cfg.timeout)
  should.equal(r1.rows, [[Some(value.Integer(10))]])

  let assert Ok(#(r2, state)) =
    connection.execute_prepared(state, prepared, [Some(value.Integer(21))], reg, cfg.timeout)
  should.equal(r2.rows, [[Some(value.Integer(42))]])

  let assert Ok(state) = connection.close_statement(state, "my_stmt", cfg.timeout)
  connection.disconnect(state)
}

// =========================================================================
// 5. Connection pool
// =========================================================================

pub fn smoke_pool_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 3)
  let p = started.data

  // Multiple queries through pool
  let assert Ok(r1) = pool.query(p, "SELECT 1::int4", [], cfg.timeout)
  should.equal(r1.rows, [[Some(value.Integer(1))]])

  let assert Ok(r2) = pool.query(p, "SELECT 2::int4", [], cfg.timeout)
  should.equal(r2.rows, [[Some(value.Integer(2))]])

  // Error recovery
  let assert Error(_) = pool.query(p, "SELECT * FROM nonexistent_xyz", [], cfg.timeout)
  let assert Ok(r3) = pool.query(p, "SELECT 3::int4", [], cfg.timeout)
  should.equal(r3.rows, [[Some(value.Integer(3))]])

  pool.shutdown(p, cfg.timeout)
}

// =========================================================================
// 6. COPY protocol
// =========================================================================

pub fn smoke_copy_test() {
  let cfg = test_config()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(_, state)) =
    connection.simple_query(
      state,
      "CREATE TEMP TABLE _smoke_copy (id int, name text)",
      cfg.timeout,
    )

  // COPY IN
  let data = [
    <<"10\tAlice\n":utf8>>,
    <<"20\tBob\n":utf8>>,
    <<"30\tCharlie\n":utf8>>,
  ]
  let assert Ok(#(tag, state)) =
    copy.copy_in(state, "COPY _smoke_copy FROM STDIN", data, cfg.timeout)
  should.equal(tag, "COPY 3")

  // COPY OUT
  let assert Ok(#(rows, state)) =
    copy.copy_out(state, "COPY _smoke_copy TO STDOUT", cfg.timeout)
  should.equal(list_length(rows), 3)

  connection.disconnect(state)
}

// =========================================================================
// 7. LISTEN/NOTIFY
// =========================================================================

pub fn smoke_notify_test() {
  let cfg = test_config()
  let assert Ok(listener) = connection.connect(cfg)
  let assert Ok(sender) = connection.connect(cfg)

  let assert Ok(listener) =
    notifications.listen(listener, "smoke_chan", cfg.timeout)

  let assert Ok(sender) =
    notifications.notify(sender, "smoke_chan", "smoke_payload", cfg.timeout)

  let assert Ok(#(_notifs, listener)) =
    notifications.receive_notifications(listener, cfg.timeout)

  let assert Ok(listener) =
    notifications.unlisten(listener, "smoke_chan", cfg.timeout)

  connection.disconnect(listener)
  connection.disconnect(sender)
}

// =========================================================================
// 8. Streaming (portal suspension)
// =========================================================================

pub fn smoke_stream_test() {
  let cfg = test_config()
  let reg = build_registry()
  let assert Ok(state) = connection.connect(cfg)

  let assert Ok(#(prepared, state)) =
    connection.prepare(
      state,
      "",
      "SELECT * FROM generate_series(1, 10) AS s(num)",
      [],
      cfg.timeout,
    )

  // Stream 10 rows in chunks of 3
  let assert Ok(#(result, state)) =
    stream.stream_query(state, prepared, [], reg, 3, cfg.timeout)

  should.equal(list_length(result.rows), 10)

  // Verify first and last values
  let assert [Some(value.Integer(1)), ..] = case result.rows {
    [first, ..] -> first
    _ -> []
  }

  connection.disconnect(state)
}

// =========================================================================
// 9. Replication (LSN + connect)
// =========================================================================

pub fn smoke_lsn_roundtrip_test() {
  let assert Ok(s) = replication.encode_lsn(0)
  should.equal(s, "0/0")

  let assert Ok(v) = replication.decode_lsn("FFFFFFFF/FFFFFFFF")
  should.equal(v, 18_446_744_073_709_551_615)

  let assert Ok(s2) = replication.encode_lsn(v)
  should.equal(s2, "FFFFFFFF/FFFFFFFF")
}

pub fn smoke_replication_connect_and_slot_test() {
  let cfg = test_config()
  let assert Ok(state) = replication.connect(config: cfg)

  let assert Ok(#(_, state)) =
    replication.query(
      state,
      "CREATE_REPLICATION_SLOT smoke_repl_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
      cfg.timeout,
    )

  replication.disconnect(state)
}

// =========================================================================
// 10. Error handling
// =========================================================================

pub fn smoke_error_handling_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  // Syntax error
  let assert Error(_) = postgleam.simple_query(conn, "SELCT bad syntax")

  // Connection should still work after error
  let assert Ok(results) = postgleam.simple_query(conn, "SELECT 1")
  let assert [r] = results
  should.equal(r.tag, "SELECT 1")

  // Wrong table
  let assert Error(_) = postgleam.query(conn, "SELECT * FROM nonexistent_abc", [])

  // Still works
  let assert Ok(r2) = postgleam.query(conn, "SELECT 42::int4", [])
  should.equal(r2.rows, [[Some(value.Integer(42))]])

  postgleam.disconnect(conn)
}

pub fn smoke_auth_failure_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("wrong_password")
  let assert Error(_) = postgleam.connect(cfg)
}

// =========================================================================
// Helpers
// =========================================================================

fn list_length(l: List(a)) -> Int {
  case l {
    [] -> 0
    [_, ..rest] -> 1 + list_length(rest)
  }
}
