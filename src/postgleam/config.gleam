/// Connection configuration for PostgreSQL
pub type Config {
  Config(
    host: String,
    port: Int,
    database: String,
    username: String,
    password: String,
    timeout: Int,
    connect_timeout: Int,
    extra_parameters: List(#(String, String)),
    ssl: SslMode,
    pgbouncer: Bool,
    idle_interval: Int,
    queue_timeout: Int,
    aggressive_reconnect: Bool,
  )
}

/// SSL connection mode
pub type SslMode {
  /// No SSL (plain TCP)
  SslDisabled
  /// SSL required, verify server certificate (uses system CA store + SNI)
  SslVerified
  /// SSL required, skip certificate verification (for Neon, self-signed certs)
  SslUnverified
}

/// Create a default config for localhost
pub fn default() -> Config {
  Config(
    host: "localhost",
    port: 5432,
    database: "postgres",
    username: "postgres",
    password: "postgres",
    timeout: 15_000,
    connect_timeout: 5000,
    extra_parameters: [],
    ssl: SslDisabled,
    pgbouncer: False,
    idle_interval: 1000,
    queue_timeout: 5000,
    aggressive_reconnect: False,
  )
}

pub fn host(config: Config, host: String) -> Config {
  Config(..config, host: host)
}

pub fn port(config: Config, port: Int) -> Config {
  Config(..config, port: port)
}

pub fn database(config: Config, database: String) -> Config {
  Config(..config, database: database)
}

pub fn username(config: Config, username: String) -> Config {
  Config(..config, username: username)
}

pub fn password(config: Config, password: String) -> Config {
  Config(..config, password: password)
}

pub fn timeout(config: Config, timeout: Int) -> Config {
  Config(..config, timeout: timeout)
}

pub fn ssl(config: Config, mode: SslMode) -> Config {
  Config(..config, ssl: mode)
}

/// Set connect timeout in milliseconds.
pub fn connect_timeout(config: Config, ms: Int) -> Config {
  Config(..config, connect_timeout: ms)
}

/// Set extra startup parameters sent to PostgreSQL.
pub fn extra_parameters(
  config: Config,
  params: List(#(String, String)),
) -> Config {
  Config(..config, extra_parameters: params)
}

/// Enable PgBouncer compatibility mode.
/// Uses unnamed prepared statements with Flush to avoid mid-query
/// backend reassignment in PgBouncer transaction pooling mode.
pub fn pgbouncer(config: Config, enabled: Bool) -> Config {
  Config(..config, pgbouncer: enabled)
}

/// Set the health check interval in milliseconds (pool only).
/// Idle connections are pinged at this interval. Default: 1000ms.
pub fn idle_interval(config: Config, ms: Int) -> Config {
  Config(..config, idle_interval: ms)
}

/// Set the maximum time in milliseconds to wait for a pool connection.
/// If no connection becomes available within this time, returns TimeoutError.
/// Default: 5000ms.
pub fn queue_timeout(config: Config, ms: Int) -> Config {
  Config(..config, queue_timeout: ms)
}

/// Enable aggressive reconnection for suspend/resume environments
/// When enabled, if 2+ health checks fail simultaneously the pool assumes all
/// connections are dead (e.g. after VM resume with stale TCP sockets), recycles
/// every connection, and signals in-flight queries to retry on fresh ones.
/// Default: False.
pub fn aggressive_reconnect(config: Config, enabled: Bool) -> Config {
  Config(..config, aggressive_reconnect: enabled)
}
