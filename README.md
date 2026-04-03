# lite_proto
A SQLite extension to support Protobufs natively.

## Testing and Coverage
This project uses `gcov` and `lcov` for code coverage analysis.
The current code coverage for `lite_proto.c` is **95.3%** (line coverage), achieved by covering all reachable branches and excluding hard-to-test OOM handlers.

You can run the tests and generate the coverage report using Docker:
```bash
docker build -t lite_proto_test .
```
