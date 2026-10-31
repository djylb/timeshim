# AGENTS.md

- Before you finish, `gofmt -l .` must print nothing and `go vet ./...`,
  `go test -race ./...`, `GOOS=js GOARCH=wasm go vet ./...` and the js/wasm
  tests (`GOOS=js GOARCH=wasm go test ./...` with
  `$(go env GOROOT)/lib/wasm` on `PATH`) must pass. Give every test run a
  `-timeout`.
- Standard library only, also for tests; `deps_test.go` checks it.
- `Fake` never holds its lock while calling a timer's function, and never
  moves backwards.
- Keep the names and semantics of the `time` package; `Real` calls `time`
  and `context` directly. Start no goroutine per timer, ticker or context.
- Keep `Clock` at `Now` and `AfterFunc`; build the rest on them.
- Tests must not leak goroutines or timers.
- The README, docs and commits name no other project and quote no benchmark
  numbers.
- Commit subjects are short, English and imperative.
- Loops that retry non-blocking calls (tests included) must yield, e.g. with
  `runtime.Gosched()`: js/wasm has no preemption, so a spinning goroutine
  starves every other one, and `go test -timeout` cannot fire.
