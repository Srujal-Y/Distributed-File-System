# -------------------------------
# Client.py - DistriFS Client
# -------------------------------
import socket
import pickle
import os
import sys
import uuid
import math
import time
from utils import generate_chunk_id

# -------------------------------
# Configuration
# -------------------------------
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9000
CHUNK_SIZE = 64 * 1024  # 64 KB for testing; can increase to 64 MB
RETRY_LIMIT = 3          # retries for failed chunk uploads

# -------------------------------
# Helper Functions
# -------------------------------
def request_chunk_assignment(filename):
    """
    Ask Master to assign chunk(s) for a file.
    Returns: {"chunk_ids": [...], "assigned_nodes": [[(host, port), ...], ...]}
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((MASTER_HOST, MASTER_PORT))
    msg = {"filename": filename}
    s.sendall(pickle.dumps(msg))
    resp = pickle.loads(s.recv(4096))
    s.close()
    if "error" in resp:
        raise Exception(f"[CLIENT] Master error: {resp['error']}")
    return resp

def send_chunk_to_datanode(chunk_id, content, datanode_host, datanode_port):
    datanode_port = int(datanode_port)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((datanode_host, datanode_port))
        payload = {"chunk_id": chunk_id, "content": content}
        s.sendall(pickle.dumps(payload))
        ack = pickle.loads(s.recv(4096))
        s.close()
        return ack.get("status") == "ok"
    except Exception as e:
        print(f"[CLIENT] Failed to send chunk {chunk_id} to {datanode_host}:{datanode_port}: {e}")
        return False

def upload_file(filename):
    """
    Uploads a file in chunks with pipelined replication.
    """
    if not os.path.exists(filename):
        print(f"[CLIENT] File '{filename}' does not exist.")
        return

    print(f"[CLIENT] Requesting chunk assignment from Master for '{filename}'...")
    assignment = request_chunk_assignment(filename)
    chunk_ids = assignment["chunk_ids"]
    assigned_nodes_list = assignment["assigned_nodes"]

    with open(filename, "rb") as f:
        for i, chunk_id in enumerate(chunk_ids):
            data = f.read(CHUNK_SIZE)
            if not data:
                break

            # Pipelinined replication
            target_nodes = assigned_nodes_list[i]
            first_node = target_nodes[0]
            print(f"[CLIENT] Uploading chunk {chunk_id} to pipeline: {target_nodes}")

            # Send to first DataNode
            success = send_chunk_to_datanode(chunk_id, data, first_node[0], first_node[1])
            if not success:
                print(f"[CLIENT] Failed to upload chunk {chunk_id} to first node, skipping chunk.")
                continue

            # Forward to remaining nodes via pipeline (from first node)
            # For simplicity, client sends directly to all remaining nodes (can also implement client-to-node forwarding)
            for host, port in target_nodes[1:]:
                port = int(port)
                while retry_count < RETRY_LIMIT:
                    success = send_chunk_to_datanode(chunk_id, data, host, port)
                    if success:
                        break
                    retry_count += 1
                    time.sleep(1)
                if not success:
                    print(f"[CLIENT] Warning: Failed to upload chunk {chunk_id} to node {host}:{port}")

            print(f"[CLIENT] Chunk {chunk_id} upload complete.")

    print(f"[CLIENT] File '{filename}' upload finished ({len(chunk_ids)} chunks).")


# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <filename>")
        sys.exit(1)

    upload_file(sys.argv[1])
