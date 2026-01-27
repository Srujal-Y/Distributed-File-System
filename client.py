import socket
import pickle
import os
import sys
import uuid
import math
import time
from utils import generate_chunk_id

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9000
CHUNK_SIZE = 64 * 1024  
RETRY_LIMIT = 3          

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

    if not os.path.exists(filename):
        print(f"[CLIENT] File '{filename}' does not exist.")
        return

    print(f"[CLIENT] Requesting chunk assignment from Master for '{filename}'...")
    assignment = request_chunk_assignment(filename)

    chunk_ids = assignment["chunk_ids"]
    pipelines = assignment["assigned_nodes"]

    with open(filename, "rb") as f:
        file_data = f.read()

    for chunk_id, pipeline in zip(chunk_ids, pipelines):

        print(f"[CLIENT] Uploading chunk {chunk_id} to pipeline: {pipeline}")

        first_node = pipeline[0]
        host, port = first_node

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, int(port)))

            msg = {
                "chunk_id": chunk_id,
                "content": file_data,
                "replicate_to": pipeline[1:]
            }

            s.sendall(pickle.dumps(msg))
            resp = pickle.loads(s.recv(4096))
            s.close()

            if resp.get("status") != "ok":
                print(f"[CLIENT] Failed to upload chunk {chunk_id}")
            else:
                print(f"[CLIENT] Chunk {chunk_id} uploaded successfully")

        except Exception as e:
            print(f"[CLIENT] Upload error for chunk {chunk_id}: {e}")

    print(f"[CLIENT] File '{filename}' upload finished ({len(chunk_ids)} chunks).")

def download_file(filename, dest_path):
    """
    Downloads a file from the distributed file system.
    Reads all chunks, verifies CRC, and reconstructs the file locally.
    """
    print(f"[CLIENT] Requesting chunk locations from Master for '{filename}'...")
    assignment = request_chunk_assignment(filename)  
    chunk_ids = assignment["chunk_ids"]
    assigned_nodes_list = assignment["assigned_nodes"]

    with open(dest_path, "wb") as out_file:
        for i, chunk_id in enumerate(chunk_ids):
            target_nodes = assigned_nodes_list[i]
            data = None

           
            for host, port in target_nodes:
                try:
                    data, version = read_chunk_safe(chunk_id) 
                    print(f"[CLIENT] Successfully read chunk {chunk_id} (v{version}) from {host}:{port}")
                    break
                except Exception as e:
                    print(f"[CLIENT] Warning: Failed to read chunk {chunk_id} from {host}:{port}: {e}")

            if data is None:
                print(f"[CLIENT] ERROR: Could not read chunk {chunk_id} from any replica. Aborting.")
                return

            out_file.write(data)

    print(f"[CLIENT] File '{filename}' successfully reconstructed at '{dest_path}'")





if __name__ == "__main__":

    
    if len(sys.argv) == 2:
        upload_file(sys.argv[1])
        sys.exit(0)

    
    if len(sys.argv) < 3:
        print("Usage: python client.py <upload/download> <filename> [dest_path_for_download]")
        sys.exit(1)

    action = sys.argv[1].lower()
    filename = sys.argv[2]

    if action == "upload":
        upload_file(filename)

    elif action == "download":
        if len(sys.argv) != 4:
            print("Provide destination path for download.")
            sys.exit(1)
        dest_path = sys.argv[3]
        download_file(filename, dest_path)

    else:
        print("Unknown action. Use 'upload' or 'download'.")


