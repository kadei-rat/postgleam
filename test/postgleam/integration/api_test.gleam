import gleam/option.{None, Some}
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/decode
import postgleam/error
import postgleam/value

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

pub fn connect_and_disconnect_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)
  postgleam.disconnect(conn)
}

pub fn query_select_int_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(conn, "SELECT 42::int4 AS num", [])

  should.equal(result.tag, "SELECT 1")
  should.equal(result.rows, [[Some(value.Integer(42))]])
  postgleam.disconnect(conn)
}

pub fn query_with_param_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT $1::int4 + $2::int4 AS sum",
      [Some(value.Integer(10)), Some(value.Integer(32))],
    )

  should.equal(result.rows, [[Some(value.Integer(42))]])
  postgleam.disconnect(conn)
}

pub fn query_with_null_param_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::int4 AS val", [None])

  should.equal(result.rows, [[None]])
  postgleam.disconnect(conn)
}

pub fn query_multiple_types_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT 1::int4 AS i, 'hello'::text AS t, true::bool AS b, 3.14::float8 AS f",
      [],
    )

  let assert [[Some(value.Integer(1)), Some(value.Text("hello")), Some(value.Boolean(True)), Some(value.Float(f))]] =
    result.rows
  should.be_true(f >. 3.13 && f <. 3.15)
  postgleam.disconnect(conn)
}

pub fn query_error_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Error(_) =
    postgleam.query(conn, "SELECT * FROM nonexistent_xyz", [])

  // Connection should still be usable after error
  let assert Ok(result) =
    postgleam.query(conn, "SELECT 1::int4 AS num", [])

  should.equal(result.rows, [[Some(value.Integer(1))]])
  postgleam.disconnect(conn)
}

pub fn simple_query_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(results) =
    postgleam.simple_query(conn, "SELECT 1 AS num")

  let assert [result] = results
  should.equal(result.tag, "SELECT 1")
  should.equal(result.columns, ["num"])
  should.equal(result.rows, [[Some("1")]])
  postgleam.disconnect(conn)
}

pub fn prepare_execute_close_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(prepared) =
    postgleam.prepare(conn, "my_stmt", "SELECT $1::int4 AS val")

  should.equal(prepared.param_oids, [23])

  let assert Ok(result) =
    postgleam.execute(conn, prepared, [Some(value.Integer(42))])

  should.equal(result.rows, [[Some(value.Integer(42))]])

  // Execute again
  let assert Ok(result2) =
    postgleam.execute(conn, prepared, [Some(value.Integer(99))])

  should.equal(result2.rows, [[Some(value.Integer(99))]])

  // Close
  let assert Ok(Nil) = postgleam.close(conn, "my_stmt")

  postgleam.disconnect(conn)
}

pub fn sequential_queries_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) = postgleam.query(conn, "SELECT 1::int4", [])
  let assert Ok(_) = postgleam.query(conn, "SELECT 2::int4", [])
  let assert Ok(result) = postgleam.query(conn, "SELECT 3::int4 AS val", [])

  should.equal(result.rows, [[Some(value.Integer(3))]])
  postgleam.disconnect(conn)
}

pub fn transaction_commit_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  // Create temp table
  let assert Ok(_) =
    postgleam.simple_query(conn, "CREATE TEMP TABLE _tx_test (id int4)")

  // Transaction that succeeds
  let assert Ok(42) =
    postgleam.transaction(conn, fn(conn) {
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO _tx_test VALUES ($1::int4)",
          [Some(value.Integer(1))],
        )
      Ok(42)
    })

  // Verify the insert persisted
  let assert Ok(result) =
    postgleam.query(conn, "SELECT id FROM _tx_test", [])

  should.equal(result.rows, [[Some(value.Integer(1))]])
  postgleam.disconnect(conn)
}

pub fn transaction_rollback_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  // Create temp table
  let assert Ok(_) =
    postgleam.simple_query(conn, "CREATE TEMP TABLE _tx_rb_test (id int4)")

  // Transaction that fails
  let assert Error(_) =
    postgleam.transaction(conn, fn(conn) {
      let assert Ok(_) =
        postgleam.query(
          conn,
          "INSERT INTO _tx_rb_test VALUES ($1::int4)",
          [Some(value.Integer(99))],
        )
      Error(postgleam.query_error("deliberate rollback"))
    })

  // Verify the insert was rolled back
  let assert Ok(result) =
    postgleam.query(conn, "SELECT id FROM _tx_rb_test", [])

  should.equal(result.rows, [])
  postgleam.disconnect(conn)
}

pub fn connect_wrong_password_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("wrong_password")

  let assert Error(_) = postgleam.connect(cfg)
}

pub fn bytea_roundtrip_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let data = <<0, 1, 2, 255, 128, 64>>
  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::bytea AS data", [Some(value.Bytea(data))])

  should.equal(result.rows, [[Some(value.Bytea(data))]])
  postgleam.disconnect(conn)
}

// =============================================================================
// DX: Parameter constructors
// =============================================================================

pub fn param_constructors_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT $1::int4 + $2::int4 AS sum, $3::text AS name, $4::bool AS flag",
      [postgleam.int(10), postgleam.int(32), postgleam.text("hello"), postgleam.bool(True)],
    )

  should.equal(result.rows, [
    [Some(value.Integer(42)), Some(value.Text("hello")), Some(value.Boolean(True))],
  ])
  postgleam.disconnect(conn)
}

pub fn param_null_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(conn, "SELECT $1::int4 AS val", [postgleam.null()])

  should.equal(result.rows, [[None]])
  postgleam.disconnect(conn)
}

pub fn param_nullable_some_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT $1::text AS val",
      [postgleam.nullable(Some("hello"), postgleam.text)],
    )

  should.equal(result.rows, [[Some(value.Text("hello"))]])
  postgleam.disconnect(conn)
}

pub fn param_nullable_none_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT $1::text AS val",
      [postgleam.nullable(None, postgleam.text)],
    )

  should.equal(result.rows, [[None]])
  postgleam.disconnect(conn)
}

// =============================================================================
// DX: query_with (decoder-integrated query)
// =============================================================================

pub fn query_with_single_column_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  let assert Ok(response) =
    postgleam.query_with(conn, "SELECT 42::int4 AS num", [], decoder)

  should.equal(response.rows, [42])
  should.equal(response.count, 1)
  should.equal(response.tag, "SELECT 1")
  postgleam.disconnect(conn)
}

pub fn query_with_multiple_columns_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let decoder = {
    use id <- decode.element(0, decode.int)
    use name <- decode.element(1, decode.text)
    use active <- decode.element(2, decode.bool)
    decode.success(#(id, name, active))
  }

  let assert Ok(response) =
    postgleam.query_with(
      conn,
      "SELECT 1::int4, 'alice'::text, true::bool",
      [],
      decoder,
    )

  should.equal(response.rows, [#(1, "alice", True)])
  postgleam.disconnect(conn)
}

pub fn query_with_multiple_rows_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  let assert Ok(response) =
    postgleam.query_with(
      conn,
      "SELECT generate_series(1, 3)::int4",
      [],
      decoder,
    )

  should.equal(response.rows, [1, 2, 3])
  should.equal(response.count, 3)
  postgleam.disconnect(conn)
}

pub fn query_with_optional_column_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let decoder = {
    use val <- decode.element(0, decode.optional(decode.text))
    decode.success(val)
  }

  let assert Ok(response) =
    postgleam.query_with(conn, "SELECT NULL::text", [], decoder)

  should.equal(response.rows, [None])
  postgleam.disconnect(conn)
}

pub fn query_with_params_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let decoder = {
    use sum <- decode.element(0, decode.int)
    decode.success(sum)
  }

  let assert Ok(response) =
    postgleam.query_with(
      conn,
      "SELECT $1::int4 + $2::int4",
      [postgleam.int(17), postgleam.int(25)],
      decoder,
    )

  should.equal(response.rows, [42])
  postgleam.disconnect(conn)
}

// =============================================================================
// DX: query_one
// =============================================================================

pub fn query_one_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  let assert Ok(val) =
    postgleam.query_one(conn, "SELECT 42::int4", [], decoder)

  should.equal(val, 42)
  postgleam.disconnect(conn)
}

pub fn query_one_no_rows_error_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) =
    postgleam.simple_query(conn, "CREATE TEMP TABLE _empty_test (id int)")

  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  let assert Error(_) =
    postgleam.query_one(conn, "SELECT id FROM _empty_test", [], decoder)

  postgleam.disconnect(conn)
}

// =============================================================================
// DX: connect returns Error type
// =============================================================================

pub fn connect_error_type_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("wrong_password")

  // Should return Error(Error) not Error(String)
  let assert Error(err) = postgleam.connect(cfg)
  // Can pattern match on the Error type
  let _msg = case err {
    error.ConnectionError(m) -> m
    _ -> "other error"
  }
}

// =============================================================================
// DX: transaction with connection parameter
// =============================================================================

pub fn transaction_with_conn_param_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(_) =
    postgleam.simple_query(conn, "CREATE TEMP TABLE _tx_dx_test (id int)")

  let decoder = {
    use n <- decode.element(0, decode.int)
    decode.success(n)
  }

  let assert Ok(count) =
    postgleam.transaction(conn, fn(c) {
      let assert Ok(_) =
        postgleam.query(c, "INSERT INTO _tx_dx_test VALUES ($1::int4)", [
          postgleam.int(1),
        ])
      let assert Ok(_) =
        postgleam.query(c, "INSERT INTO _tx_dx_test VALUES ($1::int4)", [
          postgleam.int(2),
        ])
      postgleam.query_one(
        c,
        "SELECT count(*)::int4 FROM _tx_dx_test",
        [],
        decoder,
      )
    })

  should.equal(count, 2)
  postgleam.disconnect(conn)
}

// =============================================================================
// Existing tests
// =============================================================================

pub fn float_special_values_test() {
  let cfg = test_config()
  let assert Ok(conn) = postgleam.connect(cfg)

  let assert Ok(result) =
    postgleam.query(
      conn,
      "SELECT 'NaN'::float8 AS nan, 'Infinity'::float8 AS inf, '-Infinity'::float8 AS ninf",
      [],
    )

  should.equal(result.rows, [
    [Some(value.NaN), Some(value.PosInfinity), Some(value.NegInfinity)],
  ])
  postgleam.disconnect(conn)
}
