# postgleam

[![Package Version](https://img.shields.io/hexpm/v/postgleam)](https://hex.pm/packages/postgleam)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/postgleam/)

A native Gleam PostgreSQL driver implementing the wire protocol from scratch. No NIFs, no C dependencies, no wrappers around existing Erlang/Elixir drivers — just Gleam + BitArrays + a small Erlang FFI for crypto and SSL.

```sh
gleam add postgleam@1
```

## Quick start

```gleam
import postgleam
import postgleam/config
import postgleam/decode

pub fn main() {
  let assert Ok(conn) =
    config.default()
    |> config.database("mydb")
    |> postgleam.connect()

  // Parameterized queries with typed params — SQL injection safe
  let assert Ok(response) =
    postgleam.query_with(
      conn,
      "SELECT id, name, email FROM users WHERE active = $1",
      [postgleam.bool(True)],
      {
        use id <- decode.element(0, decode.int)
        use name <- decode.element(1, decode.text)
        use email <- decode.element(2, decode.optional(decode.text))
        decode.success(#(id, name, email))
      },
    )

  response.rows
  // -> [#(1, "alice", Some("alice@example.com")), #(2, "bob", None)]

  postgleam.disconnect(conn)
}
```

## Features

- **Full PostgreSQL wire protocol v3** — binary format for all queries
- **25+ type codecs** — bool, int2/4/8, float4/8, text, bytea, uuid, date, time, timetz, timestamp, timestamptz, interval, json, jsonb, numeric, point, inet/cidr, macaddr, arrays, and more
- **SSL/TLS** — verified (system CA + SNI + hostname check) and unverified modes, works with Neon and other cloud providers
- **SCRAM-SHA-256, MD5, and cleartext authentication**
- **Connection pooling** — supervised pool with round-robin checkout
- **Transactions** — `postgleam.transaction(conn, fn(conn) { ... })` with auto-commit/rollback
- **LISTEN/NOTIFY** — pub/sub notifications
- **COPY IN/OUT** — bulk data transfer
- **Portal streaming** — fetch large result sets in chunks
- **WAL replication** — logical replication with LSN tracking
- **Row decoders** — composable, type-safe result decoding via `use` syntax
- **Parameter constructors** — `postgleam.int(42)`, `postgleam.text("hello")`, `postgleam.null()`

## Usage

### Connection

```gleam
import postgleam
import postgleam/config

// Builder pattern
let assert Ok(conn) =
  config.default()  // localhost:5432, postgres/postgres
  |> config.host("db.example.com")
  |> config.port(5432)
  |> config.database("myapp")
  |> config.username("myuser")
  |> config.password("secret")
  |> config.ssl(config.SslVerified)
  |> postgleam.connect()

// Don't forget to disconnect
postgleam.disconnect(conn)
```

### Queries with decoders

```gleam
import postgleam
import postgleam/decode

// Define a decoder for your row shape
let user_decoder = {
  use id <- decode.element(0, decode.int)
  use name <- decode.element(1, decode.text)
  use email <- decode.element(2, decode.optional(decode.text))
  decode.success(User(id:, name:, email:))
}

// query_with returns decoded rows
let assert Ok(response) =
  postgleam.query_with(conn, "SELECT id, name, email FROM users", [], user_decoder)
response.rows  // -> [User(1, "alice", Some("alice@example.com")), ...]

// query_one returns a single decoded row (errors if no rows)
let assert Ok(user) =
  postgleam.query_one(
    conn,
    "SELECT id, name, email FROM users WHERE id = $1",
    [postgleam.int(1)],
    user_decoder,
  )
```

### Parameters

```gleam
// Typed constructors — no wrapping needed
postgleam.query(conn, "SELECT $1::int4, $2::text, $3::bool", [
  postgleam.int(42),
  postgleam.text("hello"),
  postgleam.bool(True),
])

// NULL
postgleam.query(conn, "SELECT $1::int4", [postgleam.null()])

// Nullable from Option values
let maybe_email: Option(String) = None
postgleam.query(conn, "INSERT INTO users (email) VALUES ($1::text)", [
  postgleam.nullable(maybe_email, postgleam.text),
])
```

Available constructors: `int`, `float`, `text`, `bool`, `null`, `bytea`, `uuid`, `json`, `jsonb`, `numeric`, `date`, `timestamp`, `timestamptz`, `nullable`.

### Transactions

```gleam
let assert Ok(user_id) =
  postgleam.transaction(conn, fn(conn) {
    let assert Ok(_) =
      postgleam.query(conn, "INSERT INTO users (name) VALUES ($1::text)", [
        postgleam.text("alice"),
      ])
    postgleam.query_one(
      conn,
      "SELECT currval('users_id_seq')::int4",
      [],
      { use id <- decode.element(0, decode.int); decode.success(id) },
    )
  })
// Commits on Ok, rolls back on Error
```

### Connection pool

```gleam
import postgleam/pool

let assert Ok(started) = pool.start(cfg, pool_size: 5)
let p = started.data

let assert Ok(result) =
  pool.query(p, "SELECT 1::int4", [], timeout: 5000)

pool.shutdown(p, timeout: 5000)
```

### Simple queries (text protocol)

```gleam
// For DDL, multi-statement queries, or when you don't need binary decoding
let assert Ok(results) =
  postgleam.simple_query(conn, "CREATE TABLE foo (id serial); INSERT INTO foo DEFAULT VALUES")
```

### SSL/TLS

```gleam
// Verified — full certificate validation (recommended for production)
config.default()
|> config.ssl(config.SslVerified)

// Unverified — skip certificate verification (for Neon, self-signed certs)
config.default()
|> config.ssl(config.SslUnverified)

// Disabled — plain TCP (default, for local development)
config.default()
|> config.ssl(config.SslDisabled)
```

### LISTEN/NOTIFY

```gleam
import postgleam/notifications

let assert Ok(state) = notifications.listen(state, "my_channel", timeout)
// ... from another connection:
let assert Ok(_) = notifications.notify(other, "my_channel", "payload", timeout)
// Receive:
let assert Ok(#(notifs, state)) = notifications.receive_notifications(state, timeout)
```

### COPY

```gleam
import postgleam/copy

// Bulk insert
let data = [<<"1\tAlice\n":utf8>>, <<"2\tBob\n":utf8>>]
let assert Ok(#("COPY 2", state)) =
  copy.copy_in(state, "COPY users FROM STDIN", data, timeout)

// Bulk export
let assert Ok(#(rows, state)) =
  copy.copy_out(state, "COPY users TO STDOUT", timeout)
```

## Architecture

Postgleam is a complete port of [Postgrex](https://github.com/elixir-ecto/postgrex) to native Gleam, adapted to Gleam's type system and conventions:

| Layer | Module | Description |
|-------|--------|-------------|
| **Public API** | `postgleam` | `connect`, `query`, `query_with`, `query_one`, `transaction`, etc. |
| **Decoders** | `postgleam/decode` | Composable row decoders for type-safe result extraction |
| **Config** | `postgleam/config` | Connection configuration with builder pattern |
| **Pool** | `postgleam/pool` | Supervised connection pool |
| **Actor** | `postgleam/internal/connection_actor` | OTP actor wrapping the connection |
| **Protocol** | `postgleam/connection` | Wire protocol state machine (low-level) |
| **Messages** | `postgleam/message` | Encode/decode all 33+ PostgreSQL message types |
| **Codecs** | `postgleam/codec/*` | Binary encode/decode for each PostgreSQL type |
| **Auth** | `postgleam/auth/*` | SCRAM-SHA-256, MD5, cleartext |
| **Transport** | `postgleam/internal/transport` | TCP/SSL abstraction |

## Development

```sh
# Start PostgreSQL
docker compose up -d

# Setup test database
./scripts/setup_test_db.sh

# Run tests (379 tests)
gleam test
```

## Target

Gleam on the BEAM (Erlang). JavaScript target is not supported — this library uses TCP sockets and OTP actors.
