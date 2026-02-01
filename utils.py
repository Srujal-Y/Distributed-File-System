# utils.py - shared helpers for DistriFS
from __future__ import annotations

import os
import struct
import pickle
import tempfile
import threading
import socket
import hashlib
from typing import Any, Dict, Iterator, Tuple, Optional

# -------------------------
# WAL helpers (Master)
# -------------------------
WAL_LOCK = threading.Lock()

# WAL format: [OP_CODE (1 byte)][LEN (4 bytes)][PAYLOAD (LEN bytes, pickle)]
def wal_append(path: str, op_code: int, payload: Dict[str, Any]) -> None:
    blob = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    rec = struct.pack("!BI", op_code & 0xFF, len(blob)) + blob
    with WAL_LOCK:
        with open(path, "ab") as f:
            f.write(rec)
            f.flush()
            os.fsync(f.fileno())

def wal_iter(path: str) -> Iterator[Tuple[int, Dict[str, Any]]]:
    if not os.path.exists(path):
        return
    with open(path, "rb") as f:
        while True:
            hdr = f.read(5)
            if not hdr:
                break
            if len(hdr) != 5:
                break
            op, n = struct.unpack("!BI", hdr)
            payload = f.read(n)
            if len(payload) != n:
                break
            try:
                yield op, pickle.loads(payload)
            except Exception:
                break

# -------------------------
# TCP framed messages (Data plane control headers + acks)
# -------------------------
# msg := [len:4 big-endian][pickle-bytes]
def send_msg(sock: socket.socket, obj: Any) -> None:
    data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    sock.sendall(struct.pack("!I", len(data)) + data)

def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed")
        buf += chunk
    return buf

def recv_msg(sock: socket.socket) -> Any:
    raw = recv_exact(sock, 4)
    (n,) = struct.unpack("!I", raw)
    data = recv_exact(sock, n)
    return pickle.loads(data)

# -------------------------
# Filesystem helpers
# -------------------------
def atomic_write(path: str, data: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def storage_root() -> str:
    base = "/tmp" if os.name != "nt" else tempfile.gettempdir()
    return os.path.join(base, "distrifs", "data")

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

# -------------------------
# Protobuf generation
# -------------------------
_PROTO_ONCE = threading.Lock()

def _proto_path() -> str:
    # Prefer explicit env var path
    p = os.getenv("DISTRIFS_PROTO_PATH")
    if p and os.path.exists(p):
        return os.path.abspath(p)
    # Fallback to local file in cwd
    if os.path.exists("distrifs.proto"):
        return os.path.abspath("distrifs.proto")
    raise RuntimeError("Cannot find distrifs.proto. Set DISTRIFS_PROTO_PATH or place distrifs.proto in project root.")

def ensure_proto_generated(force: bool = False) -> None:
    """
    Generate distrifs_pb2.py / distrifs_pb2_grpc.py in the current working directory.
    Safe to call from all entrypoints.
    """
    with _PROTO_ONCE:
        if (not force) and os.path.exists("distrifs_pb2.py") and os.path.exists("distrifs_pb2_grpc.py"):
            return
        try:
            from grpc_tools import protoc
        except Exception as e:
            raise RuntimeError("grpcio-tools not installed. Run: pip install grpcio grpcio-tools protobuf") from e

        proto = _proto_path()
        proto_dir = os.path.dirname(proto)
        proto_file = os.path.basename(proto)

        args = [
            "protoc",
            f"-I{proto_dir}",
            "--python_out=.",
            "--grpc_python_out=.",
            proto_file,
        ]
        # protoc runs from cwd; set cwd to proto_dir by temporarily chdir
        old = os.getcwd()
        try:
            os.chdir(proto_dir)
            code = protoc.main(args)
        finally:
            os.chdir(old)
        if code != 0:
            raise RuntimeError(f"protoc failed with exit code {code}")

# -------------------------
# Master epoch helpers (fencing)
# -------------------------
def read_master_epoch(epoch_path: str = "master.epoch") -> int:
    try:
        with open(epoch_path, "r", encoding="utf-8") as f:
            return int(f.read().strip() or "0")
    except Exception:
        return 0

