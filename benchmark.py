import sqlite3
import time
import random
import string
import json
import os
import ecommerce_pb2

# Configuration
NUM_ORDERS = 10000
DB_FILE = 'benchmark.db'

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_data():
    print(f"Generating {NUM_ORDERS} orders...")
    orders_pb = []
    orders_json = []
    
    for i in range(NUM_ORDERS):
        # Protobuf
        order = ecommerce_pb2.Order()
        order.id = f"ORD-{i:06d}"
        order.customer_id = f"CUST-{random.randint(1, 1000):04d}"
        order.total_amount = round(random.uniform(10.0, 1000.0), 2)
        order.status = random.randint(0, 2)
        order.shipping_address = random_string(30)
        order.billing_address = random_string(30)
        order.carrier = random.choice(['UPS', 'FedEx', 'DHL', 'USPS'])
        order.tracking_number = random_string(15)
        order.created_at = int(time.time())
        order.updated_at = int(time.time())
        
        orders_pb.append(order.SerializeToString())
        
        # JSON
        orders_json.append(json.dumps({
            'id': order.id,
            'customer_id': order.customer_id,
            'total_amount': order.total_amount,
            'status': order.status,
            'shipping_address': order.shipping_address,
            'billing_address': order.billing_address,
            'carrier': order.carrier,
            'tracking_number': order.tracking_number,
            'created_at': order.created_at,
            'updated_at': order.updated_at
        }))
        
    return orders_pb, orders_json

def setup_db(orders_pb, orders_json):
    if os.path.exists(DB_FILE):
         os.remove(DB_FILE)
         
    conn = sqlite3.connect(DB_FILE)
    
    # Enable extension loading
    conn.enable_load_extension(True)
    # Python 3.10 on Ubuntu 22.04 doesn't support entry point argument
    conn.load_extension('./lite_proto.so')
    
    # Create tables
    conn.execute("CREATE TABLE pb_orders (id TEXT, b BLOB, total_amount REAL GENERATED ALWAYS AS (proto_extract(b, 'ecommerce.pb', 'Order', 'total_amount')) STORED);")
    conn.execute("CREATE TABLE json_orders (id TEXT, j TEXT, total_amount REAL GENERATED ALWAYS AS (json_extract(j, '$.total_amount')) STORED);")
    
    # Insert data
    print("Inserting data...")
    t0 = time.time()
    
    cursor = conn.cursor()
    
    # Protobuf insert
    data_pb = [(f"ORD-{i:06d}", b) for i, b in enumerate(orders_pb)]
    cursor.executemany("INSERT INTO pb_orders VALUES (?, ?)", data_pb)
    
    # JSON insert
    data_json = [(f"ORD-{i:06d}", j) for i, j in enumerate(orders_json)]
    cursor.executemany("INSERT INTO json_orders VALUES (?, ?)", data_json)
    
    conn.commit()
    print(f"Data inserted in {time.time() - t0:.2f} seconds.")
    
    # Create Virtual Table
    conn.execute("CREATE VIRTUAL TABLE v_order USING lite_proto('ecommerce.pb', 'Order');")
    
    # Add indexes (columns are created in CREATE TABLE)
    print("Adding indexes...")
    conn.execute("CREATE INDEX idx_pb_orders_amount ON pb_orders(total_amount);")
    conn.execute("CREATE INDEX idx_json_orders_amount ON json_orders(total_amount);")
    
    return conn

def run_benchmarks(conn):
    print("\nRunning benchmarks...")
    
    # 1. Full Scan - Extract 1 field
    print("\n--- Full Scan: Project 1 Field ---")
    
    # JSON
    t0 = time.time()
    cursor = conn.execute("SELECT json_extract(j, '$.total_amount') FROM json_orders;")
    res = cursor.fetchall()
    print(f"JSON: {time.time() - t0:.4f} seconds (Rows: {len(res)})")
    
    # Protobuf
    t0 = time.time()
    cursor = conn.execute("SELECT v.total_amount FROM pb_orders JOIN v_order v WHERE v._blob = pb_orders.b;")
    res = cursor.fetchall()
    print(f"Protobuf (VTable): {time.time() - t0:.4f} seconds (Rows: {len(res)})")
    
    # Protobuf (Scalar)
    t0 = time.time()
    cursor = conn.execute("SELECT proto_extract(b, 'ecommerce.pb', 'Order', 'total_amount') FROM pb_orders;")
    res = cursor.fetchall()
    print(f"Protobuf (Scalar): {time.time() - t0:.4f} seconds (Rows: {len(res)})")
    
    # 2. Full Scan - Extract multiple fields
    print("\n--- Full Scan: Project 5 Fields ---")
    
    # JSON
    t0 = time.time()
    cursor = conn.execute("SELECT json_extract(j, '$.id'), json_extract(j, '$.customer_id'), json_extract(j, '$.total_amount'), json_extract(j, '$.status'), json_extract(j, '$.carrier') FROM json_orders;")
    res = cursor.fetchall()
    print(f"JSON: {time.time() - t0:.4f} seconds")
    
    # Protobuf
    t0 = time.time()
    cursor = conn.execute("SELECT v.id, v.customer_id, v.total_amount, v.status, v.carrier FROM pb_orders JOIN v_order v WHERE v._blob = pb_orders.b;")
    res = cursor.fetchall()
    print(f"Protobuf (VTable): {time.time() - t0:.4f} seconds")
    
    # Protobuf (Scalar)
    t0 = time.time()
    cursor = conn.execute("SELECT proto_extract(b, 'ecommerce.pb', 'Order', 'id'), proto_extract(b, 'ecommerce.pb', 'Order', 'customer_id'), proto_extract(b, 'ecommerce.pb', 'Order', 'total_amount'), proto_extract(b, 'ecommerce.pb', 'Order', 'status'), proto_extract(b, 'ecommerce.pb', 'Order', 'carrier') FROM pb_orders;")
    res = cursor.fetchall()
    print(f"Protobuf (Scalar): {time.time() - t0:.4f} seconds")
    
    # 3. Filter on extracted field
    print("\n--- Full Scan: Filter on Amount > 500 ---")
    
    # JSON
    t0 = time.time()
    cursor = conn.execute("SELECT count(*) FROM json_orders WHERE json_extract(j, '$.total_amount') > 500.0;")
    res = cursor.fetchone()
    print(f"JSON: {time.time() - t0:.4f} seconds (Count: {res[0]})")
    
    # Protobuf
    t0 = time.time()
    cursor = conn.execute("SELECT count(*) FROM pb_orders JOIN v_order v WHERE v._blob = pb_orders.b AND v.total_amount > 500.0;")
    res = cursor.fetchone()
    print(f"Protobuf (VTable): {time.time() - t0:.4f} seconds (Count: {res[0]})")
    
    # Protobuf (Scalar)
    t0 = time.time()
    cursor = conn.execute("SELECT count(*) FROM pb_orders WHERE proto_extract(b, 'ecommerce.pb', 'Order', 'total_amount') > 500.0;")
    res = cursor.fetchone()
    print(f"Protobuf (Scalar): {time.time() - t0:.4f} seconds (Count: {res[0]})")
    
    # 4. Indexed Filter
    print("\n--- Indexed Filter: Amount > 500 ---")
    
    # JSON
    t0 = time.time()
    cursor = conn.execute("SELECT count(*) FROM json_orders WHERE total_amount > 500.0;")
    res = cursor.fetchone()
    print(f"JSON (Indexed): {time.time() - t0:.4f} seconds (Count: {res[0]})")
    
    # Protobuf
    t0 = time.time()
    cursor = conn.execute("SELECT count(*) FROM pb_orders WHERE total_amount > 500.0;")
    res = cursor.fetchone()
    print(f"Protobuf (Indexed): {time.time() - t0:.4f} seconds (Count: {res[0]})")
    
    # Storage Size
    print("\n--- Storage Size ---")
    cursor = conn.execute("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size();")
    total_db_size = cursor.fetchone()[0]
    print(f"Total DB Size: {total_db_size / 1024 / 1024:.2f} MB")
    
    # Size of tables
    cursor = conn.execute("SELECT SUM(LENGTH(b)) FROM pb_orders;")
    pb_size = cursor.fetchone()[0]
    cursor = conn.execute("SELECT SUM(LENGTH(j)) FROM json_orders;")
    json_size = cursor.fetchone()[0]
    
    print(f"Raw Protobuf Data Size: {pb_size / 1024 / 1024:.2f} MB")
    print(f"Raw JSON Data Size: {json_size / 1024 / 1024:.2f} MB")
    print(f"Protobuf is {json_size / pb_size:.2f}x smaller than JSON")

if __name__ == '__main__':
    pb, json_data = generate_data()
    conn = setup_db(pb, json_data)
    run_benchmarks(conn)
    conn.close()
