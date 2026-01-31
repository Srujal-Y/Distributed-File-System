import os
import sys
import time
import math
import socket
import zlib
from typing import Dict, Tuple, Optional, List, Any

import grpc

from utils import send_msg, recv_msg, recv_exact, ensure_proto_generated

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
    # Prevent importing stale grpc stubs from user site-packages
os.environ.setdefault("PYTHONNOUSERSITE", "1")

# If grpc stubs are stale/mismatched, gRPC will return UNIMPLEMENTED (server-side).
# Force regeneration when GetFilePlan is missing.
try:
    if os.path.exists("distrifs_pb2_grpc.py"):
        with open("distrifs_pb2_grpc.py", "r", encoding="utf-8", errors="ignore") as f:
            txt = f.read()
        if "GetFilePlan" not in txt:
            for p in ("distrifs_pb2.py", "distrifs_pb2_grpc.py"):
                try:
                    os.remove(p)
                except FileNotFoundError:
                    pass
except Exception:
    pass

ensure_proto_generated()
import distrifs_pb2 as pb
import distrifs_pb2_grpc as pb_grpc


MASTER_HOST = os.getenv("DISTRIFS_MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("DISTRIFS_MASTER_PORT", "9000"))
USER = os.getenv("DISTRIFS_USER", "user")

# Spec default 64MB; override for tests/demos.
CHUNK_SIZE = int(os.getenv("DISTRIFS_CHUNK_SIZE", str(64 * 1024 * 1024)))
REPLICATION_FACTOR = int(os.getenv("DISTRIFS_REPLICATION_FACTOR", "3"))

SOCKET_TIMEOUT = int(os.getenv("DISTRIFS_SOCKET_TIMEOUT", "12"))
RETRY_LIMIT = int(os.getenv("DISTRIFS_CLIENT_RETRY", "5"))

# metadata cache (read optimization)
CACHE_TTL = int(os.getenv("DISTRIFS_CACHE_TTL", "10"))
_chunk_cache: Dict[str, Tuple[float, List[pb.Endpoint], int, int, int]] = {}  # chunk_id -> (ts, replicas, version, checksum, size)


def _md():
    return (("x-user", USER),)


def master_stub() -> pb_grpc.MasterServiceStub:
    ch = grpc.insecure_channel(f"{MASTER_HOST}:{MASTER_PORT}")
    return pb_grpc.MasterServiceStub(ch)


def _connect(ep: pb.Endpoint) -> socket.socket:
    s = socket.create_connection((ep.host, int(ep.port)), timeout=SOCKET_TIMEOUT)
    s.settimeout(SOCKET_TIMEOUT)
    return s


def mkdir(path: str, owner: str = USER, mode: int = 0o755) -> None:
    master_stub().Mkdir(pb.MkdirRequest(path=path, owner=owner, mode=int(mode)), timeout=6.0, metadata=_md())


def listdir(path: str):
    resp = master_stub().ListDir(pb.ListDirRequest(path=path), timeout=6.0, metadata=_md())
    return list(resp.dirs), list(resp.files)


def stat(path: str) -> Dict[str, Any]:
    resp = master_stub().Stat(pb.StatRequest(path=path), timeout=6.0, metadata=_md())
    return {"type": resp.type, "mode": resp.mode, "chunks": resp.chunks, "size": resp.size, "owner": resp.owner}


def delete_path(path: str) -> None:
    master_stub().DeletePath(pb.DeletePathRequest(path=path), timeout=10.0, metadata=_md())


def rename_path(src: str, dst: str) -> None:
    master_stub().RenamePath(pb.RenamePathRequest(src=src, dst=dst), timeout=10.0, metadata=_md())


def assign_chunk(path: str, idx: int) -> pb.AssignChunkResponse:
    return master_stub().AssignChunk(pb.AssignChunkRequest(path=path, index=int(idx)), timeout=10.0, metadata=_md())


def commit_chunk(path: str, idx: int, chunk_id: str, version: int, checksum: int, size: int, node_ids: List[str]) -> None:
    master_stub().CommitChunk(pb.CommitChunkRequest(
        path=path,
        index=int(idx),
        chunk_id=chunk_id,
        version=int(version),
        checksum=int(checksum),
        size=int(size),
        node_ids=node_ids,
    ), timeout=10.0, metadata=_md())


def get_file_plan(path: str) -> pb.GetFilePlanResponse:
    return master_stub().GetFilePlan(pb.GetFilePlanRequest(path=path), timeout=10.0, metadata=_md())


def _store_stream_from_file(pipeline: List[pb.Endpoint], chunk_id: str, version: int, f, size: int) -> Dict[str, Any]:
    if not pipeline:
        raise RuntimeError("No pipeline from master")
    if len(pipeline) < REPLICATION_FACTOR:
        raise RuntimeError("CP write denied: replication factor not satisfiable")

    replicate_to = [(ep.host, int(ep.port)) for ep in pipeline[1:]]
    head = pipeline[0]

    with _connect(head) as s:
        send_msg(s, {
            "op": "STORE_STREAM",
            "chunk_id": chunk_id,
            "version": int(version),
            "size": int(size),
            "replicate_to": replicate_to,
        })

        remaining = size
        while remaining > 0:
            buf = f.read(min(65536, remaining))
            if not buf:
                raise RuntimeError("Unexpected EOF while streaming upload chunk")
            s.sendall(buf)
            remaining -= len(buf)

        resp = recv_msg(s)
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("error", "store_failed"))

        meta = resp.get("meta", {})
        acked_nodes = list(resp.get("acked_nodes", []))
        if len(acked_nodes) < REPLICATION_FACTOR:
            raise RuntimeError("Write failed: pipeline did not satisfy replication factor")

        return {
            "checksum": int(meta.get("checksum", 0)),
            "size": int(meta.get("size", size)),
            "version": int(meta.get("version", version)),
            "node_ids": acked_nodes,
        }


def upload_file(local_path: str, remote_path: str) -> None:
    total = os.path.getsize(local_path)
    n_chunks = int(math.ceil(total / float(CHUNK_SIZE))) if total else 1
    print(f"[CLIENT] PUT {local_path} -> {remote_path} bytes={total} chunks={n_chunks} chunk_size={CHUNK_SIZE}")

    with open(local_path, "rb") as f:
        for idx in range(n_chunks):
            # compute this chunk size without reading entire file
            remaining_total = total - idx * CHUNK_SIZE
            csize = CHUNK_SIZE if remaining_total > CHUNK_SIZE else max(0, remaining_total)

            if total == 0:
                csize = 0

            last_err: Optional[Exception] = None
            for attempt in range(RETRY_LIMIT):
                try:
                    plan = assign_chunk(remote_path, idx)
                    # stream exactly csize bytes from current position
                    pos_before = f.tell()
                    try:
                        res = _store_stream_from_file(list(plan.pipeline), plan.chunk_id, int(plan.version), f, int(csize))
                    except Exception:
                        # rewind for retry
                        f.seek(pos_before, os.SEEK_SET)
                        raise

                    commit_chunk(remote_path, idx, plan.chunk_id, int(res["version"]), int(res["checksum"]), int(res["size"]), node_ids=list(res["node_ids"]))
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    # backoff + retry with replan
                    time.sleep(min(0.25 * (attempt + 1), 1.0))

            if last_err is not None:
                raise RuntimeError(f"Failed PUT chunk idx={idx}: {last_err}")

    print("[CLIENT] PUT complete.")


def _cache_put(chunk_id: str, replicas: List[pb.Endpoint], version: int, checksum: int, size: int) -> None:
    _chunk_cache[chunk_id] = (time.time(), list(replicas), int(version), int(checksum), int(size))


def _try_read_replica(ep: pb.Endpoint, chunk_id: str) -> bytes:
    with _connect(ep) as s:
        send_msg(s, {"op": "READ", "chunk_id": chunk_id})
        hdr = recv_msg(s)
        if hdr.get("status") != "ok":
            code = hdr.get("code", "ERR")
            raise RuntimeError(f"{code}: {hdr.get('error', 'read_failed')}")
        size = int(hdr["size"])
        expected = int(hdr.get("checksum", 0))
        payload = recv_exact(s, size)

        actual = zlib.crc32(payload) & 0xFFFFFFFF
        if expected and actual != expected:
            raise RuntimeError(f"CORRUPT: expected {expected}, got {actual}")
        return payload



def download_file(remote_path: str, out_path: str) -> None:
    def _get_plan_nonempty(timeout_s: float = 10.0) -> pb.GetFilePlanResponse:
        deadline = time.time() + timeout_s
        last_err = None
        while time.time() < deadline:
            try:
                plan = get_file_plan(remote_path)
                if getattr(plan, "chunks", None) and len(plan.chunks) > 0:
                    return plan
            except Exception as e:
                last_err = e
            time.sleep(0.25)
        raise RuntimeError(f"GetFilePlan returned empty for too long. last={last_err}")

    plan = _get_plan_nonempty(timeout_s=12.0)
    expected_total = sum(int(fc.size) for fc in plan.chunks)

    parts: Dict[int, bytes] = {}

    for fc in plan.chunks:
        # Wait until replicas appear (volatile mapping)
        replicas = list(fc.replicas)
        if not replicas:
            deadline = time.time() + 12.0
            while time.time() < deadline:
                try:
                    plan2 = get_file_plan(remote_path)
                    match = None
                    for c2 in plan2.chunks:
                        if c2.chunk_id == fc.chunk_id:
                            match = c2
                            break
                    if match is not None and len(match.replicas) > 0:
                        replicas = list(match.replicas)
                        break
                except Exception:
                    pass
                time.sleep(0.25)

        if not replicas:
            raise RuntimeError(f"No replicas for chunk {fc.chunk_id} (mapping not rebuilt yet)")

        last_err: Optional[Exception] = None
        ok = False
        for attempt in range(min(RETRY_LIMIT, len(replicas))):
            try:
                blob = _try_read_replica(replicas[attempt], fc.chunk_id)
                parts[int(fc.index)] = blob
                ok = True
                break
            except Exception as e:
                last_err = e

        if not ok:
            raise RuntimeError(f"Failed to read chunk {fc.chunk_id}: {last_err}")

    out = b"".join(parts[i] for i in sorted(parts.keys()))

    # Hard safety check: never silently write an empty file for a non-empty input
    if expected_total > 0 and len(out) == 0:
        raise RuntimeError("Download produced 0 bytes but expected non-zero (plan/chunk fetch bug).")

    if expected_total and len(out) != expected_total:
        raise RuntimeError(f"Download size mismatch: got={len(out)} expected={expected_total}")

    tmp = out_path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(out)
    os.replace(tmp, out_path)

    print(f"[CLIENT] GET {remote_path} -> {out_path} bytes={len(out)}")



def read_range(remote_path: str, offset: int, length: int) -> bytes:
    """
    Chunking calculator requirement: map byte offsets -> chunk index + local offsets.
    Uses committed chunk sizes from master metadata.
    """
    plan = None
    last = None
    for i in range(RETRY_LIMIT):
        try:
            plan = get_file_plan(remote_path)
            break
        except Exception as e:
            last = e
            time.sleep(min(0.4 * (i + 1), 1.5))
    if plan is None:
        raise RuntimeError(f"GetFilePlan failed: {last}")

    ordered = sorted(plan.chunks, key=lambda c: c.index)

    starts: List[int] = []
    pos = 0
    for c in ordered:
        starts.append(pos)
        pos += int(c.size)

    end_off = offset + length
    out = bytearray()

    for i, c in enumerate(ordered):
        c_start = starts[i]
        c_end = c_start + int(c.size)
        if c_end <= offset:
            continue
        if c_start >= end_off:
            break

        replicas = list(c.replicas)
        blob = None
        for attempt in range(min(RETRY_LIMIT, len(replicas))):
            try:
                blob = _try_read_replica(replicas[attempt], c.chunk_id)
                break
            except Exception:
                continue
        if blob is None:
            raise RuntimeError(f"Failed to read chunk for range: {c.chunk_id}")

        lo = max(0, offset - c_start)
        hi = min(int(c.size), end_off - c_start)
        out.extend(blob[lo:hi])

    return bytes(out)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py mkdir <remote_dir>")
        print("  python client.py ls <remote_dir>")
        print("  python client.py stat <path>")
        print("  python client.py put <local_path> <remote_path>")
        print("  python client.py get <remote_path> <out_path>")
        print("  python client.py getrange <remote_path> <offset> <length> <out_path>")
        print("  python client.py rm <path>")
        print("  python client.py mv <src> <dst>")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "mkdir":
        mkdir(sys.argv[2])
        print("OK")
    elif cmd == "ls":
        d, f = listdir(sys.argv[2])
        print("dirs:", d)
        print("files:", f)
    elif cmd == "stat":
        print(stat(sys.argv[2]))
    elif cmd == "put":
        upload_file(sys.argv[2], sys.argv[3])
    elif cmd == "get":
        download_file(sys.argv[2], sys.argv[3])
    elif cmd == "getrange":
        rp = sys.argv[2]
        off = int(sys.argv[3])
        ln = int(sys.argv[4])
        outp = sys.argv[5]
        data = read_range(rp, off, ln)
        with open(outp, "wb") as f:
            f.write(data)
        print(f"[CLIENT] GETRANGE wrote {len(data)} bytes to {outp}")
    elif cmd == "rm":
        delete_path(sys.argv[2])
        print("OK")
    elif cmd == "mv":
        rename_path(sys.argv[2], sys.argv[3])
        print("OK")
    else:
        raise SystemExit(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
