import os
import sys
import time
import socket
import threading
import zlib
import pickle
from typing import Any, Dict, Optional, Tuple, List

import grpc
from concurrent import futures

from utils import (
    send_msg, recv_msg, recv_exact, atomic_write, ensure_dir, storage_root, ensure_proto_generated
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ.setdefault("PYTHONNOUSERSITE", "1")

ensure_proto_generated()

def _load_local(modname: str, filename: str):
    path = os.path.join(BASE_DIR, filename)
    spec = importlib.util.spec_from_file_location(modname, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[modname] = module
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module

pb = _load_local("distrifs_pb2", "distrifs_pb2.py")
pb_grpc = _load_local("distrifs_pb2_grpc", "distrifs_pb2_grpc.py") 


MASTER_HOST = os.getenv("DISTRIFS_MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("DISTRIFS_MASTER_PORT", "9000"))

HEARTBEAT_INTERVAL = int(os.getenv("DISTRIFS_HEARTBEAT_INTERVAL", "3"))
BLOCK_REPORT_INTERVAL = int(os.getenv("DISTRIFS_BLOCK_REPORT_INTERVAL", "30"))

SOCKET_BACKLOG = int(os.getenv("DISTRIFS_SOCKET_BACKLOG", "200"))

DATA_PORT = None
CONTROL_PORT = None
NODE_ID = None
DATA_DIR = None

CHUNKS: Dict[str, Dict[str, Any]] = {}  # chunk_id -> {version, checksum, size}
CHUNKS_LOCK = threading.Lock()


def _chunk_paths(chunk_id: str) -> Tuple[str, str]:
    fp = os.path.join(DATA_DIR, chunk_id)
    mp = fp + ".meta"
    return fp, mp


def _write_meta(chunk_id: str, meta: Dict[str, Any]) -> None:
    _, mp = _chunk_paths(chunk_id)
    atomic_write(mp, pickle.dumps(meta, protocol=pickle.HIGHEST_PROTOCOL))


def _read_meta_path(meta_path: str) -> Dict[str, Any]:
    with open(meta_path, "rb") as f:
        return pickle.loads(f.read())


def _read_meta(chunk_id: str) -> Dict[str, Any]:
    _, mp = _chunk_paths(chunk_id)
    return _read_meta_path(mp)


def cleanup_tmp_files() -> None:
    # remove leftover *.tmp from partial writes
    try:
        for name in os.listdir(DATA_DIR):
            if name.endswith(".tmp"):
                try:
                    os.remove(os.path.join(DATA_DIR, name))
                except Exception:
                    pass
    except Exception:
        pass


def scan_chunks_from_disk() -> Dict[str, Dict[str, Any]]:
    """
    Required by spec: block report must scan local disks and report full list.
    Returns chunk_id -> {version, checksum, size} derived from *.meta on disk.
    """
    out: Dict[str, Dict[str, Any]] = {}
    try:
        for name in os.listdir(DATA_DIR):
            if name.endswith(".meta") or name.endswith(".tmp"):
                continue
            fp = os.path.join(DATA_DIR, name)
            mp = fp + ".meta"
            if not os.path.exists(fp) or not os.path.exists(mp):
                continue
            try:
                meta = _read_meta_path(mp)
                out[name] = {
                    "version": int(meta["version"]),
                    "checksum": int(meta["checksum"]),
                    "size": int(meta["size"]),
                }
            except Exception:
                continue
    except Exception:
        pass
    return out


def load_existing_chunks() -> None:
    ensure_dir(DATA_DIR)
    cleanup_tmp_files()
    disk = scan_chunks_from_disk()
    with CHUNKS_LOCK:
        CHUNKS.clear()
        CHUNKS.update(disk)
    print(f"[DATANODE {DATA_PORT}] Loaded {len(disk)} chunks from disk.")


def master_stub() -> pb_grpc.MasterServiceStub:
    ch = grpc.insecure_channel(f"{MASTER_HOST}:{MASTER_PORT}")
    return pb_grpc.MasterServiceStub(ch)


def register_with_master() -> str:
    while True:
        try:
            stub = master_stub()
            resp = stub.RegisterDataNode(
                pb.RegisterRequest(data_port=int(DATA_PORT), control_port=int(CONTROL_PORT)),
                timeout=6.0,
            )
            return resp.node_id
        except Exception as e:
            print(f"[DATANODE {DATA_PORT}] Register failed: {e} (retry)")
            time.sleep(1.5)


def heartbeat_loop() -> None:
    stub = master_stub()
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        try:
            stub.Heartbeat(pb.HeartbeatRequest(node_id=NODE_ID), timeout=4.0)
        except Exception as e:
            print(f"[DATANODE {DATA_PORT}] Heartbeat error: {e}")


def block_report_loop() -> None:
    stub = master_stub()
    while True:
        time.sleep(BLOCK_REPORT_INTERVAL)

        # Required: scan disk each time.
        disk = scan_chunks_from_disk()
        with CHUNKS_LOCK:
            CHUNKS.clear()
            CHUNKS.update(disk)

        cvs = [pb.ChunkVersion(chunk_id=cid, version=int(meta["version"])) for cid, meta in disk.items()]
        try:
            stub.BlockReport(pb.BlockReportRequest(node_id=NODE_ID, chunks=cvs), timeout=10.0)
        except Exception as e:
            print(f"[DATANODE {DATA_PORT}] Block report error: {e}")


def delete_chunk_local(chunk_id: str) -> None:
    fp, mp = _chunk_paths(chunk_id)
    with CHUNKS_LOCK:
        CHUNKS.pop(chunk_id, None)
    try:
        if os.path.exists(fp):
            os.remove(fp)
        if os.path.exists(mp):
            os.remove(mp)
    except Exception:
        pass
    print(f"[DATANODE {DATA_PORT}] Deleted {chunk_id}")


def read_and_verify(chunk_id: str) -> Tuple[bytes, Dict[str, Any]]:
    fp, mp = _chunk_paths(chunk_id)
    if not os.path.exists(fp) or not os.path.exists(mp):
        raise FileNotFoundError("Chunk missing")
    meta = _read_meta(chunk_id)
    with open(fp, "rb") as f:
        data = f.read()
    expected = int(meta["checksum"])
    actual = zlib.crc32(data) & 0xFFFFFFFF
    if actual != expected:
        raise RuntimeError(f"DataCorrupt: expected {expected}, got {actual}")
    return data, {"version": int(meta["version"]), "checksum": expected, "size": int(meta["size"])}


def write_stream_atomically(
    chunk_id: str,
    version: int,
    size: int,
    src: socket.socket,
    forward: Optional[socket.socket],
) -> Dict[str, Any]:
    fp, _ = _chunk_paths(chunk_id)
    tmp = fp + ".tmp"

    received = 0
    crc = 0

    with open(tmp, "wb") as f:
        while received < size:
            buf = src.recv(min(65536, size - received))
            if not buf:
                raise ConnectionError("stream closed during STORE_STREAM")
            received += len(buf)
            f.write(buf)
            crc = zlib.crc32(buf, crc)
            if forward is not None:
                forward.sendall(buf)

        f.flush()
        os.fsync(f.fileno())

    crc &= 0xFFFFFFFF
    os.replace(tmp, fp)

    meta = {"version": int(version), "checksum": int(crc), "size": int(size)}
    _write_meta(chunk_id, meta)

    with CHUNKS_LOCK:
        CHUNKS[chunk_id] = {"version": int(version), "checksum": int(crc), "size": int(size)}
    return meta


# -----------------------------
# Data plane TCP server
# -----------------------------
def handle_data_conn(conn: socket.socket, addr) -> None:
    try:
        hdr = recv_msg(conn)
        op = hdr.get("op")

        if op == "STORE_STREAM":
            chunk_id = hdr["chunk_id"]
            version = int(hdr["version"])
            size = int(hdr["size"])
            replicate_to = hdr.get("replicate_to", [])  # list of [host,port]

            # Reject stale version
            with CHUNKS_LOCK:
                cur = CHUNKS.get(chunk_id)
                if cur and int(version) < int(cur["version"]):
                    send_msg(conn, {"status": "err", "error": "stale_version_rejected"})
                    return

            forward = None
            downstream_acked: List[str] = []
            if replicate_to:
                nh, np = replicate_to[0]
                rest = replicate_to[1:]
                forward = socket.create_connection((nh, int(np)), timeout=12)
                forward.settimeout(12)
                send_msg(forward, {
                    "op": "STORE_STREAM",
                    "chunk_id": chunk_id,
                    "version": version,
                    "size": size,
                    "replicate_to": rest,
                })

            meta = write_stream_atomically(chunk_id, version, size, conn, forward)

            # pipeline ACK: downstream -> upstream -> client
            if forward is not None:
                down = recv_msg(forward)
                forward.close()
                if down.get("status") != "ok":
                    send_msg(conn, {"status": "err", "error": f"downstream_failed:{down.get('error')}"})
                    return
                downstream_acked = list(down.get("acked_nodes", []))

            acked = [NODE_ID] + downstream_acked
            send_msg(conn, {"status": "ok", "meta": meta, "acked_nodes": acked})
            return

        if op == "READ":
            chunk_id = hdr["chunk_id"]
            try:
                data, meta = read_and_verify(chunk_id)
                send_msg(conn, {"status": "ok", **meta})
                conn.sendall(data)
            except Exception as e:
                msg = str(e)
                code = "CORRUPT" if "DataCorrupt" in msg else "ERR"
                send_msg(conn, {"status": "err", "code": code, "error": msg})
            return

        send_msg(conn, {"status": "err", "error": "unknown_op"})

    except Exception as e:
        try:
            send_msg(conn, {"status": "err", "error": str(e)})
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def data_plane_server() -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", int(DATA_PORT)))
    s.listen(SOCKET_BACKLOG)
    print(f"[DATANODE {DATA_PORT}] Data-plane on 0.0.0.0:{DATA_PORT}")
    while True:
        c, addr = s.accept()
        threading.Thread(target=handle_data_conn, args=(c, addr), daemon=True).start()


# -----------------------------
# Control plane gRPC server
# -----------------------------
class Control(pb_grpc.DataNodeControlServicer):
    def ReplicateChunk(self, request: pb.ReplicateChunkRequest, context) -> pb.Empty:
        chunk_id = request.chunk_id
        version = int(request.version)
        target = request.target

        # read local and verify integrity
        data, meta = read_and_verify(chunk_id)

        # push to target's data port as terminal STORE_STREAM
        with socket.create_connection((target.host, int(target.port)), timeout=12) as s:
            s.settimeout(12)
            send_msg(s, {
                "op": "STORE_STREAM",
                "chunk_id": chunk_id,
                "version": version,
                "size": int(meta["size"]),
                "replicate_to": [],
            })
            if data:
                s.sendall(data)
            resp = recv_msg(s)
            if resp.get("status") != "ok":
                raise RuntimeError(resp.get("error", "replication_failed"))

        return pb.Empty()

    def DeleteChunk(self, request: pb.DeleteChunkRequest, context) -> pb.Empty:
        delete_chunk_local(request.chunk_id)
        return pb.Empty()


def control_plane_server() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pb_grpc.add_DataNodeControlServicer_to_server(Control(), server)
    server.add_insecure_port(f"0.0.0.0:{int(CONTROL_PORT)}")
    server.start()
    print(f"[DATANODE {DATA_PORT}] Control-plane on 0.0.0.0:{CONTROL_PORT}")
    server.wait_for_termination()


def main() -> None:
    global DATA_PORT, CONTROL_PORT, NODE_ID, DATA_DIR
    if len(sys.argv) != 3:
        print("Usage: python datanode.py <data_port> <control_port>")
        sys.exit(1)

    DATA_PORT = int(sys.argv[1])
    CONTROL_PORT = int(sys.argv[2])

    # Practical multi-process demo: each DN gets its own subdir,
    # but chunk filenames remain UUIDs: node-<port>/chunk-<uuid>
    DATA_DIR = os.path.join(storage_root(), f"node-{DATA_PORT}")
    ensure_dir(DATA_DIR)
    load_existing_chunks()

    NODE_ID = register_with_master()
    print(f"[DATANODE {DATA_PORT}] Registered as {NODE_ID}")

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=block_report_loop, daemon=True).start()

    threading.Thread(target=data_plane_server, daemon=True).start()
    control_plane_server()


if __name__ == "__main__":
    main()
