import sqlite3
import person_pb2
import all_types_pb2

# Connect to in-memory DB
conn = sqlite3.connect(':memory:')
conn.enable_load_extension(True)

# Load extension
# SQLite expects the entry point name if it's not the default.
# Our entry point is 'sqlite3_liteproto_init'.
# In python, load_extension takes (path, entry_point).
conn.load_extension('./lite_proto.so')

# Create virtual table
conn.execute("CREATE VIRTUAL TABLE v_person USING lite_proto('person.pb', 'Person')")

# Read binary proto
with open('person.bin', 'rb') as f:
    blob = f.read()

# Query using the blob
cursor = conn.execute("SELECT name, id, email FROM v_person WHERE _blob = ?", (blob,))
row = cursor.fetchone()

print(f"Result: {row}")
assert row == ('Alice', 123, 'alice@example.com'), f"Expected ('Alice', 123, 'alice@example.com'), got {row}"
print("Virtual table test PASSED!")

# Test proto_extract with nested field
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'address.city')", (blob,))
row = cursor.fetchone()
print(f"proto_extract Result: {row}")
assert row == ('Wonderland',), f"Expected ('Wonderland',), got {row}"
print("proto_extract nested field test PASSED!")

# Test proto_extract with repeated field
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'phone_numbers')", (blob,))
row = cursor.fetchone()
print(f"proto_extract repeated field Result: {row}")
assert row == ('123-456,789-012',), f"Expected ('123-456,789-012',), got {row}"
print("proto_extract repeated field test PASSED!")

# Test write support (INSERT)
print("Testing write support (INSERT)...")
# Create base table
conn.execute("CREATE TABLE base_person_table (id INTEGER PRIMARY KEY, pb_blob BLOB)")

# Create virtual table with write support
conn.execute("CREATE VIRTUAL TABLE v_person_rw USING lite_proto('person.pb', 'Person', 'base_person_table', 'pb_blob')")

# Insert data into virtual table
conn.execute("INSERT INTO v_person_rw (name, id, email) VALUES (?, ?, ?)", ("Bob", 456, "bob@example.com"))

# Verify data in base table
cursor = conn.execute("SELECT id, pb_blob FROM base_person_table")
row = cursor.fetchone()
print(f"Base table row: {row}")
assert row is not None, "No data found in base table after insert"

# Verify we can read it back via virtual table
cursor = conn.execute("SELECT name, id, email FROM v_person_rw")
row = cursor.fetchone()
print(f"Virtual table read back: {row}")
assert row == ("Bob", 456, "bob@example.com"), f"Expected ('Bob', 456, 'bob@example.com'), got {row}"
print("Write support test PASSED!")

# Additional tests for coverage
print("Testing additional cases for coverage...")

# 1. Test proto_extract with int32 field
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'id')", (blob,))
row = cursor.fetchone()
print(f"proto_extract id Result: {row}")
assert row == (123,), f"Expected (123,), got {row}"
print("proto_extract int32 test PASSED!")

# 2. Test proto_extract with non-existent field
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'non_existent')", (blob,))
row = cursor.fetchone()
print(f"proto_extract non_existent Result: {row}")
assert row == (None,), f"Expected (None,), got {row}"
print("proto_extract non-existent field test PASSED!")

# 3. Test proto_extract with invalid path (traversing non-message)
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'id.something')", (blob,))
row = cursor.fetchone()
print(f"proto_extract invalid path Result: {row}")
assert row == (None,), f"Expected (None,), got {row}"
print("proto_extract invalid path test PASSED!")

print("All additional coverage tests PASSED!")

# Tests for new fields (bool and float)
print("Testing new fields for coverage...")

# 4. Test proto_extract with bool field
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'active')", (blob,))
row = cursor.fetchone()
print(f"proto_extract active Result: {row}")
assert row == (1,), f"Expected (1,), got {row}" # SQLite returns 1 for True
print("proto_extract bool test PASSED!")

# 5. Test proto_extract with float field
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'score')", (blob,))
row = cursor.fetchone()
print(f"proto_extract score Result: {row}")
# Use approximate comparison for floats
assert row is not None and abs(row[0] - 95.5) < 0.001, f"Expected close to (95.5,), got {row}"
print("proto_extract float test PASSED!")

# Tests for edge cases (NULL sub-message and empty array)
print("Testing edge cases for coverage...")

# 6. Test proto_extract with null sub-message
p = person_pb2.Person()
p.name = "No Address"
p.id = 789
blob_no_addr = p.SerializeToString()

cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'address.city')", (blob_no_addr,))
row = cursor.fetchone()
print(f"proto_extract null sub-message Result: {row}")
assert row == (None,), f"Expected (None,), got {row}"
print("proto_extract null sub-message test PASSED!")

# 7. Test proto_extract with empty repeated field
cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'phone_numbers')", (blob_no_addr,))
row = cursor.fetchone()
print(f"proto_extract empty repeated field Result: {row}")
# It might be None or empty string depending on upb behavior for unset repeated fields
assert row == (None,) or row == ("",), f"Expected (None,) or ('',), got {row}"
print("proto_extract empty repeated field test PASSED!")

# Tests for error handling
print("Testing error handling for coverage...")

# 8. Test proto_extract with invalid blob
try:
    cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person', 'name')", (b'invalid blob',))
    row = cursor.fetchone()
    print(f"proto_extract invalid blob Result: {row}")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"proto_extract invalid blob expectedly failed: {e}")
    assert "Failed to decode protobuf message" in str(e)
print("proto_extract invalid blob test PASSED!")

# 9. Test proto_extract with invalid schema file
try:
    cursor = conn.execute("SELECT proto_extract(?, 'non_existent.pb', 'Person', 'name')", (blob,))
    row = cursor.fetchone()
    print(f"proto_extract invalid schema Result: {row}")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"proto_extract invalid schema expectedly failed: {e}")
    assert "Failed to open schema file" in str(e) or "Failed to load schema" in str(e)
print("proto_extract invalid schema test PASSED!")

# Tests for AllTypes for maximum coverage
print("Testing AllTypes for maximum coverage...")

at = all_types_pb2.AllTypes()
at.b = True
at.i32 = -123
at.u32 = 123
at.i64 = -456
at.u64 = 456
at.f = 1.23
at.d = 4.56
at.s = "hello"
at.by = b"bytes"

at.rb.extend([True, False])
at.ri32.extend([-1, -2])
at.ru32.extend([1, 2])
at.ri64.extend([-10, -20])
at.ru64.extend([10, 20])
at.rf.extend([1.1, 2.2])
at.rd.extend([3.3, 4.4])
at.rs.extend(["a", "b"])

at.c = all_types_pb2.WEB

blob_all = at.SerializeToString()

# Test scalar types
tests = [
    ('b', (1,)),
    ('i32', (-123,)),
    ('u32', (123,)),
    ('i64', (-456,)),
    ('u64', (456,)),
    ('f', (1.23,)),
    ('d', (4.56,)),
    ('s', ("hello",)),
    ('by', (b"bytes",)),
    ('c', (1,)),
]

for field, expected in tests:
    cursor = conn.execute(f"SELECT proto_extract(?, 'all_types.pb', 'AllTypes', '{field}')", (blob_all,))
    row = cursor.fetchone()
    print(f"proto_extract {field} Result: {row}")
    if isinstance(expected[0], float):
        assert row is not None and abs(row[0] - expected[0]) < 0.001, f"Expected close to {expected}, got {row}"
    else:
        assert row == expected, f"Expected {expected}, got {row}"
    print(f"proto_extract {field} test PASSED!")

# Test repeated types
repeated_tests = [
    ('rb', ("1,0",)),
    ('ri32', ("-1,-2",)),
    ('ru32', ("1,2",)),
    ('ri64', ("-10,-20",)),
    ('ru64', ("10,20",)),
    ('rf', ("1.100000,2.200000",)),
    ('rd', ("3.300000,4.400000",)),
    ('rs', ("a,b",)),
]

for field, expected in repeated_tests:
    cursor = conn.execute(f"SELECT proto_extract(?, 'all_types.pb', 'AllTypes', '{field}')", (blob_all,))
    row = cursor.fetchone()
    print(f"proto_extract {field} Result: {row}")
    assert row == expected, f"Expected {expected}, got {row}"
    print(f"proto_extract {field} test PASSED!")

# Test write support for AllTypes
print("Testing write support for AllTypes (INSERT)...")
conn.execute("CREATE TABLE base_all_types (id INTEGER PRIMARY KEY, pb_blob BLOB)")
conn.execute("CREATE VIRTUAL TABLE v_all_types_rw USING lite_proto('all_types.pb', 'AllTypes', 'base_all_types', 'pb_blob')")

conn.execute("INSERT INTO v_all_types_rw (b, i32, u32, i64, u64, s, f) VALUES (?, ?, ?, ?, ?, ?, ?)", (True, -123, 123, -456, 456, "hello", 1.23))

# Verify data in base table
cursor = conn.execute("SELECT id, pb_blob FROM base_all_types")
row = cursor.fetchone()
print(f"Base table row for AllTypes: {row}")
assert row is not None, "No data found in base table after insert"

# Verify we can read it back via virtual table
cursor = conn.execute("SELECT b, i32, u32, i64, u64, s FROM v_all_types_rw")
row = cursor.fetchone()
print(f"Virtual table read back for AllTypes: {row}")
assert row == (1, -123, 123, -456, 456, "hello"), f"Expected (1, -123, 123, -456, 456, 'hello'), got {row}"
print("AllTypes write support test PASSED!")

print("Testing DELETE on virtual table...")
try:
    conn.execute("DELETE FROM v_person_rw WHERE name = 'Bob'")
    print("DELETE succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"DELETE expectedly failed: {e}")
    assert "DELETE not supported yet" in str(e)
print("DELETE test PASSED!")

print("Testing UPDATE on virtual table...")
try:
    conn.execute("UPDATE v_person_rw SET email = 'new@example.com' WHERE name = 'Bob'")
    print("UPDATE succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"UPDATE expectedly failed: {e}")
    assert "UPDATE not supported yet" in str(e)
print("UPDATE test PASSED!")

# Bonus coverage tests

# 1. Usage error in CREATE VIRTUAL TABLE
print("Testing CREATE VIRTUAL TABLE usage error...")
try:
    conn.execute("CREATE VIRTUAL TABLE v_bad USING lite_proto('only_one_arg')")
    print("CREATE succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"CREATE expectedly failed: {e}")
    assert "Usage: CREATE VIRTUAL TABLE" in str(e)
print("Usage error test PASSED!")

# 2. Query without WHERE on read-only table
print("Testing query without WHERE on read-only table...")
cursor = conn.execute("SELECT * FROM v_person")
rows = cursor.fetchall()
print(f"Query without WHERE on read-only table result: {rows}")
assert len(rows) == 0, "Expected 0 rows"
print("Query without WHERE test PASSED!")

# 3. Insert into read-only table
print("Testing INSERT into read-only table...")
try:
    conn.execute("INSERT INTO v_person (name) VALUES ('Alice')")
    print("INSERT succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"INSERT into read-only table expectedly failed: {e}")
    assert "Virtual table is read-only" in str(e)
print("Insert into read-only table test PASSED!")

# 4. proto_extract with wrong number of arguments
print("Testing proto_extract with wrong number of arguments...")
try:
    cursor = conn.execute("SELECT proto_extract(?, 'person.pb', 'Person')", (b'',))
    print("proto_extract succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"proto_extract with 3 args expectedly failed: {e}")
    assert "proto_extract requires 4 arguments" in str(e)
print("proto_extract with wrong args test PASSED!")

# 5. DROP TABLE to cover xDestroy
print("Testing DROP TABLE...")
conn.execute("DROP TABLE v_person")
print("DROP TABLE test PASSED!")

# 6. Read Double and Bytes from virtual table
print("Testing reading Double and Bytes from virtual table...")
at2 = all_types_pb2.AllTypes()
at2.f = 1.23
at2.d = 4.56
at2.by = b"bytes"
blob_all2 = at2.SerializeToString()

# Insert directly into base table for AllTypes
conn.execute("INSERT INTO base_all_types (pb_blob) VALUES (?)", (blob_all2,))

# Query via virtual table
cursor = conn.execute("SELECT f, d, by FROM v_all_types_rw")
rows = cursor.fetchall()
print(f"All rows from v_all_types_rw: {rows}")
assert len(rows) >= 2, f"Expected at least 2 rows, got {len(rows)}"
row = rows[1] # Take the second row (the one we just inserted directly)
print(f"Virtual table read back for Double/Bytes: {row}")
assert abs(row[0] - 1.23) < 0.001, f"Expected 1.23, got {row[0]}"
assert abs(row[1] - 4.56) < 0.001, f"Expected 4.56, got {row[1]}"
assert row[2] == b"bytes", f"Expected b'bytes', got {row[2]}"
print("Read Double and Bytes test PASSED!")

# 7. Double quotes in CREATE VIRTUAL TABLE
print("Testing CREATE VIRTUAL TABLE with double quotes...")
conn.execute('CREATE VIRTUAL TABLE v_person_dq USING lite_proto("person.pb", "Person")')
print("CREATE VIRTUAL TABLE with double quotes PASSED!")

# 8. CREATE VIRTUAL TABLE with missing schema
print("Testing CREATE VIRTUAL TABLE with missing schema...")
try:
    conn.execute("CREATE VIRTUAL TABLE v_broken USING lite_proto('non_existent.pb', 'Person')")
    print("CREATE succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"CREATE with missing schema expectedly failed: {e}")
    assert "Failed to open schema file" in str(e)
print("CREATE with missing schema test PASSED!")

# 9. CREATE VIRTUAL TABLE with invalid message name
print("Testing CREATE VIRTUAL TABLE with invalid message name...")
try:
    conn.execute("CREATE VIRTUAL TABLE v_invalid_msg USING lite_proto('person.pb', 'NonExistentMessage')")
    print("CREATE succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"CREATE with invalid message name expectedly failed: {e}")
    assert "Failed to find message" in str(e)
print("CREATE with invalid message name test PASSED!")

# 10. CREATE VIRTUAL TABLE with corrupt schema file
print("Testing CREATE VIRTUAL TABLE with corrupt schema file...")
with open('corrupt.pb', 'wb') as f:
    f.write(b'this is not a valid proto descriptor set')
try:
    conn.execute("CREATE VIRTUAL TABLE v_corrupt USING lite_proto('corrupt.pb', 'Person')")
    print("CREATE succeeded unexpectedly")
    assert False, "Expected OperationalError"
except sqlite3.OperationalError as e:
    print(f"CREATE with corrupt schema file expectedly failed: {e}")
    assert "Failed to parse FileDescriptorSet" in str(e)
print("CREATE with corrupt schema file test PASSED!")

# 11. Query empty virtual table
print("Testing query on empty virtual table...")
conn.execute("CREATE TABLE base_empty (id INTEGER PRIMARY KEY, pb_blob BLOB)")
conn.execute("CREATE VIRTUAL TABLE v_empty USING lite_proto('person.pb', 'Person', 'base_empty', 'pb_blob')")
cursor = conn.execute("SELECT * FROM v_empty")
rows = cursor.fetchall()
print(f"Empty table query result: {rows}")
assert len(rows) == 0, f"Expected 0 rows, got {len(rows)}"
print("Query empty virtual table test PASSED!")
