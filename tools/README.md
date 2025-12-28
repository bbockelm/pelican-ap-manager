# Tools

Utility binaries for working with Pelican integration artifacts and debugging.

- `collect_job_epochs/`: gathers job epoch ads (ClassAd JSON) from a remote schedd for tests; sanitizes the data if needed.
- `collect_transfers/`: gathers transfer ads from a schedd, recording them as JSON, and potentially sanitizing the data for tests/fixtures.
- `regenerate_golden/`: regenerates the golden `epoch_history` file in `artifacts/integration_history/` from sanitized test data. This file is used as the expected output reference for integration tests.
- `fake_transfer_plugin/`: small mock transfer plugin used in integration tests to emit predictable ads.
- `internal/`: shared libraries used by the tool binaries (not meant for direct invocation).

Build/run examples:

```bash
# Build a specific tool
cd tools
go build ./collect_job_epochs

# Run a tool directly
cd tools
go run ./collect_transfers --help

# Regenerate golden test fixtures
make regenerate-golden
```
