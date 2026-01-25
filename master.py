import socket
import threading
import pickle
import time
import os
import uuid
from utils import save_pickle, load_pickle, append_wal_entry, read_wal

# -------------------------------
# Configuration
# -------------------------------
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9000
WAL_PATH = "master.log"
REPLICATION_FACTOR = 3
HEARTBEAT_INTERVAL = 3        # seconds
BLOCK_REPORT_INTERVAL = 30    # seconds

# -------------------------------
# In-memory structures
# -------------------------------
# Active DataNodes: {node_id: {"host": host, "port": port, "last_seen": timestamp, "chunks": set()}}
ACTIVE_NODES = {}
NODES_LOCK = threading.Lock()

# File -> [ChunkIDs]
FILE_CHUNKS = {}
FILE_LOCK = threading.Lock()

# Chunk -> [DataNode IDs]
CHUNK_LOCATIONS = {}
CHUNKS_LOCK = threading.Lock()


# -------------------------------
# WAL Recovery
# -------------------------------

def recover_wal():
    """
    Recover WAL entries on startup to rebuild in-memory state.
    Each WAL entry is a dict: {"op": "CREATE_FILE", "filename": ..., "chunk_ids": [...]}
    """
    print("[MASTER] Recovering WAL...")
    if not os.path.exists(WAL_PATH):
        print("[MASTER] WAL file not found. Skipping recovery.")
        return

    with open(WAL_PATH, "rb") as f:
        while True:
            try:
                entry = pickle.load(f)
                op = entry.get("op")
                if op == "CREATE_FILE":
                    filename = entry["filename"]
                    chunk_ids = entry["chunk_ids"]
                    with FILE_LOCK:
                        FILE_CHUNKS[filename] = chunk_ids
                    with CHUNKS_LOCK:
                        for cid, nodes in zip(chunk_ids, entry["assigned_nodes"]):
                            CHUNK_LOCATIONS[cid] = nodes
                elif op == "UPDATE_CHUNK":
                    cid = entry["chunk_id"]
                    nodes = entry["assigned_nodes"]
                    with CHUNKS_LOCK:
                        CHUNK_LOCATIONS[cid] = nodes
            except EOFError:
                break
            except Exception as e:
                print(f"[MASTER] Skipping invalid WAL entry: {e}")
    print("[MASTER] WAL recovery complete.")



# -------------------------------
# DataNode Registration & Heartbeat
# -------------------------------

def register_datanode(node_id, host, port):
    with NODES_LOCK:
        ACTIVE_NODES[node_id] = {
            "host": host,
            "port": port,
            "last_seen": time.time(),
            "chunks": set()
        }
    print(f"[MASTER] Registered DataNode {node_id} at {host}:{port}")

def update_heartbeat(node_id):
    with NODES_LOCK:
        if node_id in ACTIVE_NODES:
            ACTIVE_NODES[node_id]["last_seen"] = time.time()

def remove_dead_nodes():
    """Remove DataNodes that missed heartbeats for > 2 intervals."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        dead_nodes = []
        with NODES_LOCK:
            for node_id, info in ACTIVE_NODES.items():
                if now - info["last_seen"] > 2 * HEARTBEAT_INTERVAL:
                    dead_nodes.append(node_id)
            for node_id in dead_nodes:
                print(f"[MASTER] DataNode {node_id} missed heartbeat, removing.")
                del ACTIVE_NODES[node_id]
                # TODO: trigger replication for lost chunks
        # Continue looping

def start_dead_node_monitor():
    t = threading.Thread(target=remove_dead_nodes, daemon=True)
    t.start()
# -------------------------------
# Client Request Handling
# -------------------------------

def assign_chunk(filename):
    """
    Assigns chunk(s) for a file to DataNodes.
    Implements replication factor.
    Returns {"chunk_ids": [...], "assigned_nodes": [[(host, port), ...], ...]}
    """
    chunk_id = f"{filename}-{uuid.uuid4()}"
    with NODES_LOCK:
        if len(ACTIVE_NODES) < REPLICATION_FACTOR:
            raise Exception("[MASTER] Not enough active DataNodes to satisfy replication")
        # Pick N distinct DataNodes
        selected_nodes = list(ACTIVE_NODES.keys())[:REPLICATION_FACTOR]
        nodes_info = [(ACTIVE_NODES[n]["host"], ACTIVE_NODES[n]["port"]) for n in selected_nodes]

    with FILE_LOCK:
        if filename not in FILE_CHUNKS:
            FILE_CHUNKS[filename] = []
        FILE_CHUNKS[filename].append(chunk_id)

    with CHUNKS_LOCK:
        nodes_info = [(ACTIVE_NODES[n]["host"], int(ACTIVE_NODES[n]["port"])) for n in selected_nodes]
        CHUNK_LOCATIONS[chunk_id] = nodes_info 

    # Persist WAL
    wal_entry = {
        "op": "CREATE_FILE",
        "filename": filename,
        "chunk_ids": [chunk_id],
        "assigned_nodes": [selected_nodes]
    }
    append_wal_entry(WAL_PATH, wal_entry)

    print(f"[MASTER] Assigned chunk {chunk_id} to DataNodes: {selected_nodes}")
    return {"chunk_ids": [chunk_id], "assigned_nodes": nodes_info}


# -------------------------------
# DataNode Block Report Handling
# -------------------------------

def handle_block_report(node_id, chunks):
    """
    Updates CHUNK_LOCATIONS based on DataNode's block report.
    """
    with CHUNKS_LOCK, NODES_LOCK:
        if node_id not in ACTIVE_NODES:
            return
        ACTIVE_NODES[node_id]["chunks"] = set(chunks)
        for chunk_id in chunks:
            if chunk_id not in CHUNK_LOCATIONS:
                CHUNK_LOCATIONS[chunk_id] = [node_id]
            elif node_id not in CHUNK_LOCATIONS[chunk_id]:
                CHUNK_LOCATIONS[chunk_id].append(node_id)
    print(f"[MASTER] Updated block report from {node_id}")


# -------------------------------
# Connection Handler
# -------------------------------

def handle_connection(conn, addr):
    """
    Determines if connection is DataNode registration/heartbeat/block report or Client request.
    """
    try:
        data = conn.recv(4096)
        if not data:
            return
        msg = pickle.loads(data)

        # DataNode registration/heartbeat/block report
        if "port" in msg:  # Registration
            node_id = f"node-{msg['port']}"
            register_datanode(node_id, addr[0], msg['port'])
            conn.sendall(pickle.dumps({"status": "ok", "node_id": node_id}))
        elif "heartbeat" in msg:  # Heartbeat
            node_id = msg["node_id"]
            update_heartbeat(node_id)
            conn.sendall(pickle.dumps({"status": "ok"}))
        elif "block_report" in msg:  # Block report
            node_id = msg["node_id"]
            chunks = msg["chunks"]
            handle_block_report(node_id, chunks)
            conn.sendall(pickle.dumps({"status": "ok"}))
        # Client request
        elif "filename" in msg:
            filename = msg["filename"]
            try:
                assignment = assign_chunk(filename)
                conn.sendall(pickle.dumps(assignment))
            except Exception as e:
                conn.sendall(pickle.dumps({"error": str(e)}))
    except Exception as e:
        print(f"[MASTER] Connection handler error: {e}")
    finally:
        conn.close()


# -------------------------------
# Server Loop
# -------------------------------

def accept_connections(server_sock):
    while True:
        conn, addr = server_sock.accept()
        t = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
        t.start()


def main():
    recover_wal()
    start_dead_node_monitor()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((MASTER_HOST, MASTER_PORT))
    server_sock.listen(10)
    print(f"[MASTER] Listening on {MASTER_HOST}:{MASTER_PORT}")

    accept_connections(server_sock)


if __name__ == "__main__":
    main()
# -------------------------------
# Pipeline Replication & Passive Recovery
# -------------------------------

def replicate_chunk(chunk_id, source_node_id, target_node_id):
    """
    Instruct source_node_id to replicate chunk_id to target_node_id.
    This simulates the pipeline replication.
    """
    if source_node_id not in ACTIVE_NODES or target_node_id not in ACTIVE_NODES:
        print(f"[MASTER] Cannot replicate {chunk_id}, nodes unavailable")
        return

    source_host, source_port = ACTIVE_NODES[source_node_id]["host"], ACTIVE_NODES[source_node_id]["port"]
    target_host, target_port = ACTIVE_NODES[target_node_id]["host"], ACTIVE_NODES[target_node_id]["port"]

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((source_host, source_port))
        request = {
            "replicate": True,
            "chunk_id": chunk_id,
            "target_host": target_host,
            "target_port": target_port
        }
        s.sendall(pickle.dumps(request))
        resp = pickle.loads(s.recv(4096))
        if resp.get("status") == "ok":
            print(f"[MASTER] Replication of {chunk_id} from {source_node_id} to {target_node_id} successful")
            with CHUNKS_LOCK:
                if chunk_id not in CHUNK_LOCATIONS:
                    CHUNK_LOCATIONS[chunk_id] = []
                if target_node_id not in CHUNK_LOCATIONS[chunk_id]:
                    CHUNK_LOCATIONS[chunk_id].append(target_node_id)
            # WAL update for replication
            wal_entry = {
                "op": "UPDATE_CHUNK",
                "chunk_id": chunk_id,
                "assigned_nodes": CHUNK_LOCATIONS[chunk_id]
            }
            append_wal_entry(WAL_PATH, wal_entry)
    except Exception as e:
        print(f"[MASTER] Replication failed for {chunk_id} from {source_node_id} to {target_node_id}: {e}")
    finally:
        s.close()


def handle_node_failures(dead_nodes):
    """
    When a DataNode dies, re-replicate its chunks to maintain replication factor.
    """
    print(f"[MASTER] Handling failures for dead nodes: {dead_nodes}")
    with CHUNKS_LOCK:
        for chunk_id, nodes in CHUNK_LOCATIONS.items():
            # Remove dead nodes from this chunk's location list
            nodes = [n for n in nodes if n not in dead_nodes]
            CHUNK_LOCATIONS[chunk_id] = nodes
            if len(nodes) < REPLICATION_FACTOR:
                # Need to replicate to additional nodes
                needed = REPLICATION_FACTOR - len(nodes)
                available_nodes = [n for n in ACTIVE_NODES if n not in nodes]
                for target_node in available_nodes[:needed]:
                    source_node = nodes[0] if nodes else target_node
                    replicate_chunk(chunk_id, source_node, target_node)
                    nodes.append(target_node)
# -------------------------------
# -------------------------------
# Pipeline Replication & Passive Recovery
# -------------------------------
# -------------------------------

def replicate_chunk(chunk_id, source_nodes=None):
    """
    Ensure that a chunk has REPLICATION_FACTOR replicas.
    If source_nodes provided, use them as the source for replication.
    """
    with CHUNKS_LOCK, NODES_LOCK:
        current_nodes = CHUNK_LOCATIONS.get(chunk_id, [])
        missing = REPLICATION_FACTOR - len(current_nodes)
        if missing <= 0:
            return  # already enough replicas

        available_nodes = [nid for nid in ACTIVE_NODES if nid not in current_nodes]
        if not available_nodes:
            print(f"[MASTER] No available DataNodes to replicate chunk {chunk_id}")
            return

        # Pick nodes to replicate to
        target_nodes = available_nodes[:missing]

        # Pick source node if not provided
        if not source_nodes:
            source_nodes = current_nodes[:1]

        # Send replication instructions to source node
        for tnode in target_nodes:
            print(f"[MASTER] Replicating chunk {chunk_id} from {source_nodes[0]} -> {tnode}")
            # In real system, would send RPC to source node to push data
            CHUNK_LOCATIONS[chunk_id].append(tnode)

        # WAL update for replication
        wal_entry = {
            "op": "UPDATE_CHUNK",
            "chunk_id": chunk_id,
            "assigned_nodes": CHUNK_LOCATIONS[chunk_id]
        }
        append_wal_entry(WAL_PATH, wal_entry)


def passive_recovery_loop():
    """
    Periodically checks for under-replicated chunks and triggers replication.
    """
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with CHUNKS_LOCK:
            for chunk_id, nodes in CHUNK_LOCATIONS.items():
                if len(nodes) < REPLICATION_FACTOR:
                    replicate_chunk(chunk_id)


def start_passive_recovery():
    t = threading.Thread(target=passive_recovery_loop, daemon=True)
    t.start()


# -------------------------------
# -------------------------------
# Stale Chunk Handling & Bit-Rot Protection
# -------------------------------
# -------------------------------

import zlib

CHUNK_VERSIONS = {}  # {chunk_id: version_number}
CHUNK_CHECKSUMS = {}  # {chunk_id: crc32}

def update_chunk_version(chunk_id):
    """Increment version for a chunk."""
    CHUNK_VERSIONS[chunk_id] = CHUNK_VERSIONS.get(chunk_id, 0) + 1

def verify_chunk_checksum(chunk_id, data_bytes):
    """Verify CRC32 checksum of chunk."""
    expected = CHUNK_CHECKSUMS.get(chunk_id)
    crc = zlib.crc32(data_bytes)
    return expected == crc

def handle_block_report_with_stale(node_id, chunks_with_versions):
    """
    Compare DataNode block report with Master chunk versions.
    Remove stale chunks from DataNode.
    """
    with CHUNKS_LOCK, NODES_LOCK:
        if node_id not in ACTIVE_NODES:
            return
        current_chunks = set()
        for chunk_id, version in chunks_with_versions.items():
            master_version = CHUNK_VERSIONS.get(chunk_id, 0)
            if version < master_version:
                print(f"[MASTER] Instructing {node_id} to delete stale chunk {chunk_id}")
                # Send deletion instruction to DataNode in real system
                continue
            current_chunks.add(chunk_id)
        ACTIVE_NODES[node_id]["chunks"] = current_chunks


# -------------------------------
# -------------------------------
# Integrate Passive Recovery on Startup
# -------------------------------
# -------------------------------

start_passive_recovery()
