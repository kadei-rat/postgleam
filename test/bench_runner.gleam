/// Benchmark runner entry point.
/// Invoke via: gleam build && erl -pa build/dev/erlang/*/ebin -noshell -run bench_runner main -- <args>

import bench/codec_bench
import bench/compare
import bench/decode_bench
import bench/integration_bench
import bench/message_bench
import bench/realworld_bench
import bench/registry_bench
import bench/runner
import bench/uuid_bench
import gleam/io
import gleam/list
import gleam/string
import postgleam/config

pub fn main() {
  let args = get_args()

  case args {
    ["pure"] -> run_pure()
    ["integration"] -> run_integration()
    ["realworld"] -> run_realworld()
    ["compare", baseline, current] -> compare.run(baseline, current)
    ["all"] -> {
      run_pure()
      run_integration()
      run_realworld()
    }
    _ -> {
      io.println("Usage:")
      io.println("  make bench-pure          Run pure Gleam benchmarks")
      io.println("  make bench-integration   Run database benchmarks")
      io.println("  make bench-realworld     Run real-world benchmarks")
      io.println("  make bench               Run all benchmarks")
      io.println(
        "  make bench-compare BASELINE=... CURRENT=...  Compare results",
      )
    }
  }

  halt(0)
}

fn run_pure() -> Nil {
  io.println("Running pure Gleam benchmarks...")
  io.println("")

  let codec_results = codec_bench.run()
  let uuid_results = uuid_bench.run()
  let message_results = message_bench.run()
  let decode_results = decode_bench.run()
  let registry_results = registry_bench.run()

  let all =
    list.flatten([
      codec_results,
      uuid_results,
      message_results,
      decode_results,
      registry_results,
    ])

  let report = runner.make_report("pure", all)
  runner.print_table(report)
  auto_compare("bench/results/pure.json", report)

  case runner.write_report(report, "bench/results/pure.json") {
    Ok(_) -> io.println("Results written to bench/results/pure.json")
    Error(e) -> io.println("Failed to write results: " <> e)
  }
}

fn run_integration() -> Nil {
  io.println("Running integration benchmarks...")
  io.println("")

  let results = integration_bench.run()
  let report = runner.make_report("integration", results)
  runner.print_table(report)
  auto_compare("bench/results/integration.json", report)

  case runner.write_report(report, "bench/results/integration.json") {
    Ok(_) -> io.println("Results written to bench/results/integration.json")
    Error(e) -> io.println("Failed to write results: " <> e)
  }
}

fn run_realworld() -> Nil {
  let cfg = config.default() |> config.database("postgleam_test")
  let _ = realworld_bench.run(cfg)
  Nil
}

/// Auto-compare current results with previous saved results
fn auto_compare(
  path: String,
  current: runner.BenchReport,
) -> Nil {
  case runner.read_file(path) {
    Ok(prev_json) -> {
      // Write current to a temp file for comparison
      let temp_path = string.replace(path, ".json", "-tmp.json")
      let content = runner.report_to_json(current)
      case write_file(temp_path, content) {
        Ok(_) -> {
          compare.run(path, temp_path)
          let _ = delete_file(temp_path)
          Nil
        }
        Error(_) -> Nil
      }
      let _ = prev_json
      Nil
    }
    Error(_) -> {
      io.println("(No previous results to compare against)")
      Nil
    }
  }
}

@external(erlang, "bench_ffi", "get_args")
fn get_args() -> List(String)

@external(erlang, "bench_ffi", "write_file")
fn write_file(path: String, content: String) -> Result(Nil, String)

@external(erlang, "bench_ffi", "delete_file")
fn delete_file(path: String) -> Result(Nil, String)

@external(erlang, "init", "stop")
fn halt(code: Int) -> Nil
