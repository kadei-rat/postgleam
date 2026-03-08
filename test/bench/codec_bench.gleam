/// Codec encode/decode benchmarks — the inner loop of every query.

import bench/runner.{type BenchmarkResult}
import gleam/list
import gleam/option.{Some}
import gleam/string
import gleamy/bench
import postgleam/codec
import postgleam/codec/array
import postgleam/codec/bool as bool_codec
import postgleam/codec/circle as circle_codec
import postgleam/codec/date as date_codec
import postgleam/codec/float8 as float8_codec
import postgleam/codec/inet as inet_codec
import postgleam/codec/int4 as int4_codec
import postgleam/codec/int8 as int8_codec
import postgleam/codec/interval as interval_codec
import postgleam/codec/json as json_codec
import postgleam/codec/jsonb as jsonb_codec
import postgleam/codec/numeric as numeric_codec
import postgleam/codec/point as point_codec
import postgleam/codec/text as text_codec
import postgleam/codec/timestamp as timestamp_codec
import postgleam/codec/uuid as uuid_codec
import postgleam/value

const duration = 2000

const warmup = 100

pub fn run() -> List(BenchmarkResult) {
  let results = [
    bench_int4_encode(),
    bench_int4_decode(),
    bench_int8_encode(),
    bench_int8_decode(),
    bench_bool_encode(),
    bench_bool_decode(),
    bench_float8_encode(),
    bench_float8_decode(),
    bench_text_encode_small(),
    bench_text_encode_1kb(),
    bench_text_decode_small(),
    bench_text_decode_1kb(),
    bench_uuid_encode(),
    bench_uuid_decode(),
    bench_json_encode(),
    bench_json_decode(),
    bench_jsonb_encode(),
    bench_jsonb_decode(),
    bench_numeric_encode(),
    bench_numeric_decode(),
    bench_date_encode(),
    bench_date_decode(),
    bench_timestamp_encode(),
    bench_timestamp_decode(),
    bench_point_encode(),
    bench_point_decode(),
    bench_circle_encode(),
    bench_circle_decode(),
    bench_inet_encode(),
    bench_inet_decode(),
    bench_interval_encode(),
    bench_interval_decode(),
    bench_array_int4_encode(),
    bench_array_int4_decode(),
    bench_array_text_encode(),
    bench_array_text_decode(),
  ]
  list.flat_map(results, fn(r) { runner.extract_results(r) })
}

fn bench_int4_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("42", value.Integer(42))],
    [bench.Function("int4.encode", fn(v) { int4_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_int4_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("4_bytes", <<0, 0, 0, 42>>)],
    [bench.Function("int4.decode", fn(d) { int4_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_int8_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("large", value.Integer(9_999_999_999))],
    [bench.Function("int8.encode", fn(v) { int8_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_int8_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("8_bytes", <<0, 0, 0, 2, 84, 11, 227, 255>>)],
    [bench.Function("int8.decode", fn(d) { int8_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_bool_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("true", value.Boolean(True))],
    [bench.Function("bool.encode", fn(v) { bool_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_bool_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("1_byte", <<1>>)],
    [bench.Function("bool.decode", fn(d) { bool_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_float8_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("pi", value.Float(3.14159265358979))],
    [bench.Function("float8.encode", fn(v) { float8_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_float8_decode() -> bench.BenchResults {
  // IEEE 754 encoding of 3.14159265358979
  bench.run(
    [bench.Input("8_bytes", <<64, 9, 33, 251, 84, 68, 45, 24>>)],
    [bench.Function("float8.decode", fn(d) { float8_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_text_encode_small() -> bench.BenchResults {
  bench.run(
    [bench.Input("5_bytes", value.Text("hello"))],
    [bench.Function("text.encode", fn(v) { text_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_text_encode_1kb() -> bench.BenchResults {
  let big_text = value.Text(string.repeat("x", 1000))
  bench.run(
    [bench.Input("1kb", big_text)],
    [bench.Function("text.encode", fn(v) { text_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_text_decode_small() -> bench.BenchResults {
  bench.run(
    [bench.Input("5_bytes", <<"hello":utf8>>)],
    [bench.Function("text.decode", fn(d) { text_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_text_decode_1kb() -> bench.BenchResults {
  let big_bytes = <<string.repeat("x", 1000):utf8>>
  bench.run(
    [bench.Input("1kb", big_bytes)],
    [bench.Function("text.decode", fn(d) { text_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_uuid_encode() -> bench.BenchResults {
  let uuid_bytes = <<0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7,
    0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00>>
  bench.run(
    [bench.Input("16_bytes", value.Uuid(uuid_bytes))],
    [bench.Function("uuid.encode", fn(v) { uuid_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_uuid_decode() -> bench.BenchResults {
  let uuid_bytes = <<0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7,
    0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00>>
  bench.run(
    [bench.Input("16_bytes", uuid_bytes)],
    [bench.Function("uuid.decode", fn(d) { uuid_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_json_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("small_obj", value.Json("{\"key\":\"value\",\"num\":42}"))],
    [bench.Function("json.encode", fn(v) { json_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_json_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("small_obj", <<"{\"key\":\"value\",\"num\":42}":utf8>>)],
    [bench.Function("json.decode", fn(d) { json_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_jsonb_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("small_obj", value.Jsonb("{\"key\":\"value\"}"))],
    [bench.Function("jsonb.encode", fn(v) { jsonb_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_jsonb_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("small_obj", <<1, "{\"key\":\"value\"}":utf8>>)],
    [bench.Function("jsonb.decode", fn(d) { jsonb_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_numeric_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("decimal", value.Numeric("12345.6789"))],
    [bench.Function("numeric.encode", fn(v) { numeric_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_numeric_decode() -> bench.BenchResults {
  // Pre-encode a numeric value to get the binary
  let assert Ok(encoded) = numeric_codec.encode(value.Numeric("12345.6789"))
  bench.run(
    [bench.Input("decimal", encoded)],
    [bench.Function("numeric.decode", fn(d) { numeric_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_date_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("2025-01-01", value.Date(9131))],
    [bench.Function("date.encode", fn(v) { date_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_date_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("4_bytes", <<0, 0, 35, 171>>)],
    [bench.Function("date.decode", fn(d) { date_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_timestamp_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("2025", value.Timestamp(788_918_400_000_000))],
    [bench.Function("timestamp.encode", fn(v) { timestamp_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_timestamp_decode() -> bench.BenchResults {
  let assert Ok(encoded) =
    timestamp_codec.encode(value.Timestamp(788_918_400_000_000))
  bench.run(
    [bench.Input("8_bytes", encoded)],
    [bench.Function("timestamp.decode", fn(d) { timestamp_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_point_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("point", value.Point(1.5, -2.5))],
    [bench.Function("point.encode", fn(v) { point_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_point_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("16_bytes", <<1.5:float-64-big, -2.5:float-64-big>>)],
    [bench.Function("point.decode", fn(d) { point_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_circle_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("circle", value.Circle(1.0, 2.0, 5.0))],
    [bench.Function("circle.encode", fn(v) { circle_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_circle_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("24_bytes", <<1.0:float-64-big, 2.0:float-64-big, 5.0:float-64-big>>)],
    [bench.Function("circle.decode", fn(d) { circle_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_inet_encode() -> bench.BenchResults {
  bench.run(
    [bench.Input("ipv4", value.Inet(2, <<192, 168, 1, 1>>, 32))],
    [bench.Function("inet.encode", fn(v) { inet_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_inet_decode() -> bench.BenchResults {
  bench.run(
    [bench.Input("8_bytes", <<2, 32, 0, 4, 192, 168, 1, 1>>)],
    [bench.Function("inet.decode", fn(d) { inet_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_interval_encode() -> bench.BenchResults {
  // 1 year 2 months 3 days 4 hours
  bench.run(
    [bench.Input("interval", value.Interval(14_400_000_000, 3, 14))],
    [bench.Function("interval.encode", fn(v) { interval_codec.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_interval_decode() -> bench.BenchResults {
  let assert Ok(encoded) =
    interval_codec.encode(value.Interval(14_400_000_000, 3, 14))
  bench.run(
    [bench.Input("16_bytes", encoded)],
    [bench.Function("interval.decode", fn(d) { interval_codec.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn build_int4_array_codec() -> codec.Codec {
  let elem =
    codec.Codec(
      type_name: "int4",
      oid: 23,
      format: codec.Binary,
      encode: int4_codec.encode,
      decode: int4_codec.decode,
    )
  array.build_array_codec(elem, 1007)
}

fn build_text_array_codec() -> codec.Codec {
  let elem =
    codec.Codec(
      type_name: "text",
      oid: 25,
      format: codec.Binary,
      encode: text_codec.encode,
      decode: text_codec.decode,
    )
  array.build_array_codec(elem, 1009)
}

fn bench_array_int4_encode() -> bench.BenchResults {
  let c = build_int4_array_codec()
  let elements =
    list.range(1, 100)
    |> list.map(fn(n) { Some(value.Integer(n)) })
  bench.run(
    [bench.Input("100_elements", value.Array(elements))],
    [bench.Function("array_int4.encode", fn(v) { c.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_array_int4_decode() -> bench.BenchResults {
  let c = build_int4_array_codec()
  let elements =
    list.range(1, 100)
    |> list.map(fn(n) { Some(value.Integer(n)) })
  let assert Ok(encoded) = c.encode(value.Array(elements))
  bench.run(
    [bench.Input("100_elements", encoded)],
    [bench.Function("array_int4.decode", fn(d) { c.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_array_text_encode() -> bench.BenchResults {
  let c = build_text_array_codec()
  let elements =
    list.range(1, 10)
    |> list.map(fn(_) { Some(value.Text("hello world")) })
  bench.run(
    [bench.Input("10_elements", value.Array(elements))],
    [bench.Function("array_text.encode", fn(v) { c.encode(v) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}

fn bench_array_text_decode() -> bench.BenchResults {
  let c = build_text_array_codec()
  let elements =
    list.range(1, 10)
    |> list.map(fn(_) { Some(value.Text("hello world")) })
  let assert Ok(encoded) = c.encode(value.Array(elements))
  bench.run(
    [bench.Input("10_elements", encoded)],
    [bench.Function("array_text.decode", fn(d) { c.decode(d) })],
    [bench.Duration(duration), bench.Warmup(warmup)],
  )
}
