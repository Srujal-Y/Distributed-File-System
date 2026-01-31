# utils.py
import os
import sys
import struct
import pickle
import tempfile
import threading
import socket
from typing import Any, Dict, Tuple

WAL_LOCK = threading.Lock()

# WAL format: [OP_CODE (1 byte)][LEN (4 bytes)][PAYLOAD (LEN bytes)]
def wal_append(path: str, op_code: int, payload: Dict[str, Any]) -> None:
    blob = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    rec = struct.pack("!BI", op_code & 0xFF, len(blob)) + blob
    with WAL_LOCK:
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(path, "ab") as f:
            f.write(rec)
            f.flush()
            os.fsync(f.fileno())

def wal_iter(path: str):
    """
    Robust WAL iterator:
    - Stops cleanly on partial tail records (common after kill -9 / power loss).
    - Skips corrupted entries instead of crashing the Master.
    """
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
                # Partial tail write -> stop replay
                break
            try:
                yield op, pickle.loads(payload)
            except Exception:
                # Corrupt entry -> skip and continue
                continue

# TCP framed messages for small control headers on data plane
# msg := [len:4 big-endian][pickle]
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

def atomic_write(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)

def storage_root() -> str:
    # Spec says /tmp/distrifs/data. On Windows, tempfile.gettempdir() is equivalent.
    base = "/tmp" if os.name != "nt" else tempfile.gettempdir()
    return os.path.join(base, "distrifs", "data")

# -----------------------------
# Protobuf generation (HARDENED)
# -----------------------------
_PROTO_ONCE = threading.Lock()

_REQUIRED_RPC_MARKERS = (
    "rpc GetFilePlan",
    "rpc AssignChunk",
    "rpc CommitChunk",
    "service MasterService",
)

def _file_contains(path: str, needles: Tuple[str, ...]) -> bool:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            s = f.read()
        return all(n in s for n in needles)
    except Exception:
        return False

def ensure_proto_generated() -> None:
    """
    Deterministic proto generation:
    - Always uses the directory where utils.py lives (NOT the current working directory).
    - Regenerates if:
        * pb2/pb2_grpc missing
        * pb2_grpc is stale or missing required RPC symbols (fixes UNIMPLEMENTED)
        * proto is newer than generated files
        * user sets DISTRIFS_FORCE_PROTO=1
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))

    proto_path = os.path.join(base_dir, "distrifs.proto")
    pb2_path = os.path.join(base_dir, "distrifs_pb2.py")
    pb2g_path = os.path.join(base_dir, "distrifs_pb2_grpc.py")

    # Make sure imports resolve to THIS directory first
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)

    force = os.getenv("DISTRIFS_FORCE_PROTO", "").strip() == "1"

    with _PROTO_ONCE:
        if not os.path.exists(proto_path):
            raise RuntimeError(f"Missing distrifs.proto at: {proto_path}")

        # If proto itself is missing required RPCs, that's a project-state error.
        # But we still fail fast with a clear message.
        if not _file_contains(proto_path, _REQUIRED_RPC_MARKERS):
            raise RuntimeError(
                "Your distrifs.proto is missing required RPC definitions "
                "(GetFilePlan / AssignChunk / CommitChunk / MasterService). "
                "Fix distrifs.proto to match the project spec, then rerun."
            )

        def mtime(p: str) -> float:
            try:
                return os.path.getmtime(p)
            except Exception:
                return 0.0

        missing = (not os.path.exists(pb2_path)) or (not os.path.exists(pb2g_path))

        # If pb2_grpc exists but doesn't include GetFilePlan, server will return UNIMPLEMENTED.
        grpc_missing_symbols = (not os.path.exists(pb2g_path)) or (not _file_contains(pb2g_path, ("GetFilePlan", "MasterServiceStub")))

        # If proto changed after generated code, regenerate.
        stale = mtime(proto_path) > max(mtime(pb2_path), mtime(pb2g_path))

        if (not force) and (not missing) and (not grpc_missing_symbols) and (not stale):
            return

        # Delete old generated files
        for p in (pb2_path, pb2g_path):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

        try:
            from grpc_tools import protoc
        except Exception as e:
            raise RuntimeError("grpcio-tools not installed. Run: pip install grpcio-tools") from e

        # Run protoc in base_dir so -I and relative path are always correct
        args = [
            "protoc",
            f"-I{base_dir}",
            f"--python_out={base_dir}",
            f"--grpc_python_out={base_dir}",
            os.path.basename(proto_path),
        ]

        cwd = os.getcwd()
        try:
            os.chdir(base_dir)
            rc = protoc.main(args)
            if rc != 0:
                raise RuntimeError(f"protoc failed with exit code {rc}")
        finally:
            os.chdir(cwd)
