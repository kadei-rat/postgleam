import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleeunit/should
import postgleam/config
import postgleam/pool
import postgleam/value

@external(erlang, "postgleam_ffi", "monotonic_time_ms")
fn monotonic_time_ms() -> Int

fn test_config() {
  config.default()
  |> config.database("postgleam_test")
}

pub fn pool_start_and_shutdown_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  pool.shutdown(started.data, cfg.timeout)
}

pub fn pool_query_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  let p = started.data

  let assert Ok(result) =
    pool.query(p, "SELECT 42::int4 AS num", [], cfg.timeout)
  should.equal(result.rows, [[Some(value.Integer(42))]])

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_simple_query_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  let p = started.data

  let assert Ok(results) =
    pool.simple_query(p, "SELECT 1 AS num", cfg.timeout)
  let assert [result] = results
  should.equal(result.tag, "SELECT 1")

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_sequential_queries_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 2)
  let p = started.data

  let assert Ok(_) = pool.query(p, "SELECT 1::int4", [], cfg.timeout)
  let assert Ok(_) = pool.query(p, "SELECT 2::int4", [], cfg.timeout)
  let assert Ok(result) = pool.query(p, "SELECT 3::int4 AS val", [], cfg.timeout)
  should.equal(result.rows, [[Some(value.Integer(3))]])

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_with_params_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 3)
  let p = started.data

  let assert Ok(result) =
    pool.query(
      p,
      "SELECT $1::int4 + $2::int4 AS sum",
      [Some(value.Integer(10)), Some(value.Integer(32))],
      cfg.timeout,
    )
  should.equal(result.rows, [[Some(value.Integer(42))]])

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_error_recovery_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 1)
  let p = started.data

  // This should error
  let assert Error(_) =
    pool.query(p, "SELECT * FROM nonexistent_xyz", [], cfg.timeout)

  // The pool should still work after the error
  let assert Ok(result) =
    pool.query(p, "SELECT 1::int4 AS val", [], cfg.timeout)
  should.equal(result.rows, [[Some(value.Integer(1))]])

  pool.shutdown(p, cfg.timeout)
}

/// Queries on a pool with N connections should run concurrently.
/// We fire 3 pg_sleep(0.5) queries in parallel on a pool of size 3.
/// If concurrent: ~500ms total. If serialized: ~1500ms.
pub fn pool_concurrent_queries_test() {
  let cfg = test_config()
  let assert Ok(started) = pool.start(cfg, 3)
  let p = started.data

  let t0 = monotonic_time_ms()

  // Spawn 3 processes that each run pg_sleep(0.5) through the pool
  let subjects =
    list.map([1, 2, 3], fn(_) {
      let subj = process.new_subject()
      process.spawn(fn() {
        let result =
          pool.simple_query(p, "SELECT pg_sleep(0.5)", cfg.timeout)
        process.send(subj, result)
      })
      subj
    })

  // Wait for all 3 to complete
  list.each(subjects, fn(subj) {
    let assert Ok(_) = process.receive(subj, 5000)
  })

  let elapsed = monotonic_time_ms() - t0

  // With true parallelism this should take ~500ms.
  // Allow up to 1000ms to account for CI jitter, but reject the ~1500ms
  // that serialized execution would produce.
  should.be_true(elapsed < 1000)

  pool.shutdown(p, cfg.timeout)
}

pub fn pool_wrong_password_test() {
  let cfg =
    test_config()
    |> config.username("postgleam_scram_pw")
    |> config.password("wrong_password")

  let assert Error(_) = pool.start(cfg, 1)
}
