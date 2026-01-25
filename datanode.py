# -------------------------------
# DataNode.py - Part 1
# -------------------------------
import socket
import threading
import pickle
import time
import os
import sys
import uuid
import zlib
from utils import save_pickle, load_pickle, generate_chunk_id

# -------------------------------
# Configuration
# -------------------------------
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9000

if len(sys.argv) != 2:
    print("Usage: python datanode.py <port>")
    sys.exit(1)

PORT = int(sys.argv[1])
DATA_DIR = f"./data_{PORT}"
os.makedirs(DATA_DIR, exist_ok=True)

HEARTBEAT_INTERVAL = 3      # seconds
BLOCK_REPORT_INTERVAL = 30  # seconds

# -------------------------------
# In-memory state
# -------------------------------
# chunk_id -> {"filename": ..., "checksum": ..., "size": ...}
CHUNKS = {}
CHUNKS_LOCK = threading.Lock()
# -------------------------------
# Heartbeat & Block Report Threads
# -------------------------------
def send_heartbeat():
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MASTER_HOST, MASTER_PORT))
            msg = {"heartbeat": True, "node_id": f"node-{PORT}"}
            s.sendall(pickle.dumps(msg))
            resp = pickle.loads(s.recv(4096))
            s.close()
            # print(f"[DATANODE {PORT}] Heartbeat acknowledged.")
        except Exception as e:
            print(f"[DATANODE {PORT}] Heartbeat error: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def send_block_report():
    while True:
        try:
            with CHUNKS_LOCK:
                chunk_list = list(CHUNKS.keys())
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MASTER_HOST, MASTER_PORT))
            msg = {"block_report": True, "node_id": f"node-{PORT}", "chunks": chunk_list}
            s.sendall(pickle.dumps(msg))
            resp = pickle.loads(s.recv(4096))
            s.close()
            # print(f"[DATANODE {PORT}] Block report sent.")
        except Exception as e:
            print(f"[DATANODE {PORT}] Block report error: {e}")
        time.sleep(BLOCK_REPORT_INTERVAL)

def start_background_threads():
    threading.Thread(target=send_heartbeat, daemon=True).start()
    threading.Thread(target=send_block_report, daemon=True).start()
# -------------------------------
# DataNode.py - Part 2
# -------------------------------
def write_chunk(chunk_id, data):
    """
    Write a chunk to disk with checksum
    """
    file_path = os.path.join(DATA_DIR, chunk_id)
    checksum = zlib.crc32(data)
    try:
        tmp_path = file_path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(data)
        os.rename(tmp_path, file_path)  # atomic rename
        with CHUNKS_LOCK:
            CHUNKS[chunk_id] = {"checksum": checksum, "size": len(data)}
        print(f"[DATANODE {PORT}] Stored chunk {chunk_id} ({len(data)} bytes)")
    except Exception as e:
        print(f"[DATANODE {PORT}] Failed to write chunk {chunk_id}: {e}")

def handle_client_connection(conn, addr):
    """
    Handles incoming chunks from clients or pipeline replication.
    Reads the full message before unpickling to avoid truncation errors.
    Supports versioning, checksum, and optional pipeline replication.
    """
    try:
        # Read full message until connection closes
        chunks = []
        while True:
            part = conn.recv(4096)
            if not part:
                break
            chunks.append(part)
        if not chunks:
            return
        data = b"".join(chunks)

        msg = pickle.loads(data)

        # Pipeline or client upload
        if "chunk_id" in msg and "content" in msg:
            chunk_id = msg["chunk_id"]
            content = msg["content"]
            version = msg.get("version", 1)

            # Store chunk safely
            write_chunk_safe(chunk_id, content, version)

            # Forward to other DataNodes if pipeline replication info provided
            if "replicate_to" in msg:
                targets = msg["replicate_to"]  # list of (host, port)
                threading.Thread(
                    target=start_pipeline_forward,
                    args=(chunk_id, content, targets),
                    daemon=True
                ).start()

            # ACK back to sender
            conn.sendall(pickle.dumps({"status": "ok"}))

        else:
            conn.sendall(pickle.dumps({"error": "Invalid message"}))

    except Exception as e:
        print(f"[DATANODE {PORT}] Connection handler error: {e}")
    finally:
        conn.close()


def start_server():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", PORT))
    server_sock.listen(10)
    print(f"[DATANODE {PORT}] Listening for chunk uploads...")

    while True:
        conn, addr = server_sock.accept()
        threading.Thread(target=handle_client_connection, args=(conn, addr), daemon=True).start()
# -------------------------------
# DataNode.py - Part 3
# -------------------------------

def register_with_master():
    """
    Register this DataNode with the Master and send initial heartbeat
    """
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MASTER_HOST, MASTER_PORT))
            msg = {"port": PORT}  # registration
            s.sendall(pickle.dumps(msg))
            resp = pickle.loads(s.recv(4096))
            if resp.get("status") == "ok":
                NODE_ID = resp["node_id"]
                print(f"[DATANODE {PORT}] Registered with Master as {NODE_ID}")
                s.close()
                return NODE_ID
        except Exception as e:
            print(f"[DATANODE {PORT}] Registration failed: {e}, retrying...")
            time.sleep(2)

def send_heartbeat(node_id):
    """
    Periodic heartbeat to Master
    """
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MASTER_HOST, MASTER_PORT))
            msg = {"heartbeat": True, "node_id": node_id}
            s.sendall(pickle.dumps(msg))
            resp = pickle.loads(s.recv(4096))
            if resp.get("status") == "ok":
                pass  # heartbeat acknowledged
            s.close()
        except Exception as e:
            print(f"[DATANODE {PORT}] Heartbeat failed: {e}")

def send_block_report(node_id):
    """
    Periodic full block report to Master
    """
    while True:
        time.sleep(BLOCK_REPORT_INTERVAL)
        with CHUNKS_LOCK:
            chunks = list(CHUNKS.keys())
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((MASTER_HOST, MASTER_PORT))
            msg = {"block_report": True, "node_id": node_id, "chunks": chunks}
            s.sendall(pickle.dumps(msg))
            resp = pickle.loads(s.recv(4096))
            if resp.get("status") == "ok":
                print(f"[DATANODE {PORT}] Block report sent ({len(chunks)} chunks)")
            s.close()
        except Exception as e:
            print(f"[DATANODE {PORT}] Block report failed: {e}")

def start_pipeline_forward(chunk_id, content, target_nodes):
    """
    Forward the chunk to other DataNodes for replication (pipeline replication)
    """
    for host, port in target_nodes:
        if port == PORT:
            continue  # skip self
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            payload = {"chunk_id": chunk_id, "content": content}
            s.sendall(pickle.dumps(payload))
            ack = pickle.loads(s.recv(4096))
            if ack.get("status") != "ok":
                print(f"[DATANODE {PORT}] Replication to {host}:{port} failed for {chunk_id}")
            s.close()
        except Exception as e:
            print(f"[DATANODE {PORT}] Replication error to {host}:{port} for {chunk_id}: {e}")

def main():
    global NODE_ID
    NODE_ID = register_with_master()

    # Start heartbeat thread
    threading.Thread(target=send_heartbeat, args=(NODE_ID,), daemon=True).start()

    # Start block report thread
    threading.Thread(target=send_block_report, args=(NODE_ID,), daemon=True).start()

    # Start TCP server to receive chunks (client uploads or replication)
    start_server()

if __name__ == "__main__":
    # Ensure DATA_DIR exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    main()
# -------------------------------
# DataNode.py - Part 4 (Checksum & Version Integration)
# -------------------------------

def write_chunk_safe(chunk_id, data, version=1):
    """
    Stores chunk safely with checksum, versioning, and temp file atomic write.
    Updates CHUNKS dict and disk meta file.
    """
    file_path = os.path.join(DATA_DIR, chunk_id)
    meta_path = file_path + ".meta"
    tmp_path = file_path + ".tmp"

    checksum = zlib.crc32(data)

    # Write data to temp file first
    with open(tmp_path, "wb") as f:
        f.write(data)
    os.replace(tmp_path, file_path)

    # Save meta info
    meta = {"checksum": checksum, "version": version}
    save_pickle(meta_path, meta)

    # Update in-memory state
    with CHUNKS_LOCK:
        CHUNKS[chunk_id] = {"checksum": checksum, "size": len(data), "version": version}

    print(f"[DATANODE {PORT}] Stored chunk {chunk_id} (v{version}, {len(data)} bytes, crc={checksum})")


def load_chunk_safe(chunk_id):
    """
    Loads chunk data and verifies checksum. Raises Exception if corrupted.
    """
    file_path = os.path.join(DATA_DIR, chunk_id)
    meta_path = file_path + ".meta"

    if not os.path.exists(file_path) or not os.path.exists(meta_path):
        raise FileNotFoundError(f"Chunk {chunk_id} missing")

    data = open(file_path, "rb").read()
    meta = load_pickle(meta_path)
    checksum = meta["checksum"]
    version = meta["version"]

    if zlib.crc32(data) != checksum:
        raise Exception(f"Chunk {chunk_id} corrupted!")

    with CHUNKS_LOCK:
        CHUNKS[chunk_id]["version"] = version

    return data, version


def delete_stale_chunks(stale_chunk_ids):
    """
    Deletes stale chunks (from old versions) from disk and in-memory state.
    """
    for chunk_id in stale_chunk_ids:
        file_path = os.path.join(DATA_DIR, chunk_id)
        meta_path = file_path + ".meta"
        with CHUNKS_LOCK:
            CHUNKS.pop(chunk_id, None)
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(meta_path):
            os.remove(meta_path)
        print(f"[DATANODE {PORT}] Deleted stale chunk {chunk_id}")


def handle_client_connection(conn, addr):
    """
    Enhanced connection handler: receives client chunks or pipeline replication.
    Supports versioning and checksum.
    """
    try:
        data = conn.recv(4096)
        if not data:
            return
        msg = pickle.loads(data)

        # Pipeline or client upload
        if "chunk_id" in msg and "content" in msg:
            chunk_id = msg["chunk_id"]
            content = msg["content"]
            version = msg.get("version", 1)

            # Store chunk safely
            write_chunk_safe(chunk_id, content, version)

            # Forward to other DataNodes if pipeline replication info provided
            if "replicate_to" in msg:
                targets = msg["replicate_to"]  # list of (host, port)
                threading.Thread(target=start_pipeline_forward, args=(chunk_id, content, targets), daemon=True).start()

            # ACK back to sender
            conn.sendall(pickle.dumps({"status": "ok"}))

        else:
            conn.sendall(pickle.dumps({"error": "Invalid message"}))

    except Exception as e:
        print(f"[DATANODE {PORT}] Connection handler error: {e}")
    finally:
        conn.close()
# -------------------------------
# DataNode.py - Part 5 (Main Loop & Startup)
# -------------------------------

def main():
    """
    Main entry point for DataNode:
    - Registers with Master
    - Starts heartbeat and block report threads
    - Starts TCP server for chunk uploads and replication
    """
    global NODE_ID
    NODE_ID = register_with_master()

    # Start heartbeat thread
    threading.Thread(target=send_heartbeat, args=(NODE_ID,), daemon=True).start()

    # Start block report thread
    threading.Thread(target=send_block_report, args=(NODE_ID,), daemon=True).start()

    # Start TCP server to receive chunks (client uploads or replication)
    start_server()


if __name__ == "__main__":
    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    print(f"[DATANODE {PORT}] Starting up...")

    main()
