# lite_proto
A SQLite extension to support Protobufs natively.

## Usage

### 1. Loading the Extension
In your SQLite session or application, load the extension:
```sql
.load ./lite_proto.so sqlite3_liteproto_init
```

### 2. Creating a Virtual Table
To query Protobuf messages stored as BLOBs in a base table, create a virtual table:
```sql
CREATE VIRTUAL TABLE orders USING lite_proto('path/to/schema.pb', 'MessageName', 'base_table_name', 'blob_column_name');
```
Example:
```sql
CREATE VIRTUAL TABLE v_orders USING lite_proto('ecommerce.pb', 'Order', 'orders_base', 'pb_blob');
```
Now you can query `v_orders` as a normal table, and its columns will match the fields of the `Order` message!

### 3. Extracting Fields directly
You can also use the `proto_extract` scalar function to extract fields from a blob without creating a virtual table:
```sql
SELECT id, proto_extract(pb_blob, 'schema.pb', 'MessageName', 'field_path') FROM base_table;
```
Example:
```sql
SELECT id, proto_extract(pb_blob, 'ecommerce.pb', 'Order', 'total_amount') FROM orders_base;
```

### 4. Inserting Data
If you specified a base table and column during creation, you can insert rows via the virtual table:
```sql
INSERT INTO v_orders (id, customer_id, total_amount, status) VALUES ('ORD004', 'CUST03', 150.0, 1);
```
This will encode the fields into a Protobuf blob and insert it into the base table.

## Testing and Coverage
This project uses `gcov` and `lcov` for code coverage analysis.
The current code coverage for `lite_proto.c` is **95.3%** (line coverage), achieved by covering all reachable branches and excluding hard-to-test OOM handlers.

You can run the tests and generate the coverage report using Docker:
```bash
docker build -t lite_proto_test .
```
