# Design Document - SQLite Protobuf Extension (lite_proto)

This document outlines the design and phased implementation plan for the `lite_proto` SQLite extension.

## Objective
To build a schema-aware SQLite virtual table extension in C that enables native querying of Protobuf data stored in BLOBs, with high performance and minimal dependencies (using `upb`).

## Architecture
- **Language**: C (C11 or later)
- **Dependencies**:
  - SQLite3 (including loadable extension headers)
  - `upb` (Micro PB) for Protobuf parsing (included as git submodule).
- **Core Component**: A SQLite virtual table module (`sqlite3_module`).

## Phased Approach

### Phase 1: Setup and Design (Current)
- Initialize `upb` submodule.
- Setup `Makefile` to link with `upb` sources directly or build them.
- Create a minimal test to verify `upb` integration.
- Environment: Docker containing all build tools.

### Phase 2: Minimal Virtual Table
- Implement boilerplate `lite_proto.c` and `lite_proto.h`.
- Register a dummy module.
- Verify extension can be loaded in SQLite: `SELECT load_extension('./lite_proto.so');`

### Phase 3: Schema Parsing with upb
- Load and parse `FileDescriptorSet` binary files using `upb`.
- Extract message field names, numbers, and types.
- Provide a way to associate a virtual table with a specific message type.

### Phase 4: Full Virtual Table Implementation
- Implement `xCreate` (parsing schema argument).
- Implement `xBestIndex` and `xFilter` to support scanning and basic filtering.
- Implement `xColumn` to extract Protobuf fields using `upb` wire decoder.

### Phase 5: Showcase & Benchmarking
- E-commerce use case: Orders stored as Protobuf BLOBs.
- Benchmark: Compare query times and storage size between:
  - Protobuf BLOB + `lite_proto`
  - JSON text + native SQLite JSON functions.
- Use Docker Compose to run benchmarks.

## Verification Strategy
- Each phase must have a corresponding test file (e.g., `test_phase1.c`, `test_phase2.sql`).
- Continuous verification inside the Docker container.
