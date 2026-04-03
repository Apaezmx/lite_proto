# Benchmark Results: Protobuf vs JSON in SQLite

## Environment
- OS: Ubuntu 22.04 (Docker)
- SQLite: 3.37.2 (default in Ubuntu 22.04)
- Data: 10,000 synthetic e-commerce orders

## Schema
`Order` message with 10 scalar fields (strings, ints, doubles).

## Results

### Query Performance (Time in seconds)

| Query Type | JSON (`json_extract`) | Protobuf (Virtual Table) | Winner |
| :--- | :--- | :--- | :--- |
| Full Scan (Project 1 Field) | 0.0134 | 0.0161 | JSON |
| Full Scan (Project 5 Fields) | 0.0266 | 0.0332 | JSON |
| Full Scan (Filter on Amount) | 0.0101 | 0.0133 | JSON |

### Storage Size

| Data Type | Size |
| :--- | :--- |
| Raw Protobuf Data | 1.26 MB |
| Raw JSON Data | 2.87 MB |
| **Protobuf Advantage** | **2.28x smaller** |

## Analysis

1. **Storage**: Protobuf is significantly smaller (2.28x) than JSON. This is expected as Protobuf is a binary format with a compact encoding scheme.
2. **Speed**: Native SQLite JSON extraction is currently faster than our Protobuf virtual table implementation.
   - **Reason 1: Virtual Table Overhead**: Every row access in the virtual table involves multiple C function calls (`xNext`, `xEof`, `xColumn`). Native JSON functions run directly in the SQLite core with less overhead.
   - **Reason 2: Full Parse**: Our current implementation calls `upb_Decode` on the full blob for every row, even if only one field is requested.

## Next Steps for Optimization
- **Scalar Function**: Implement a `proto_extract(blob, 'field')` scalar function to avoid Virtual Table overhead and compare directly with `json_extract`.
- **Lazy Parsing**: Investigate if `upb` supports lazy parsing or wire-format seeking to avoid full message decoding when only a few fields are needed.
