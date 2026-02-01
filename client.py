# client.py - DistriFS CLI client
from __future__ import annotations

import os
import sys
import time
import socket
from typing import List, Tuple, Optional

import grpc

from utils import ensure_proto_generated, send_msg, recv_msg, recv_exact

ensure_proto_generated(force=bool(os.getenv("DISTRIFS_FORCE_PROTO")))

import distrifs_pb2 as pb
import distrifs_pb2_grpc as pb_grpc

MASTER_HOST = os.getenv("DISTRIFS_MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("DISTRIFS_MASTER_PORT", "9000"))
USER = os.getenv("DISTRIFS_USER", "user")

CHUNK_SIZE = int(os.getenv("DISTRIFS_CHUNK_SIZE", str(64 * 1024 * 1024)))
CACHE_TTL_S = float(os.getenv("DISTRIFS_CLIENT_CACHE_TTL_S", "10.0"))

_MASTER_ADDR = {"host": MASTER_HOST, "port": MASTER_PORT}
_PLAN_CACHE: dict[str, tuple[float, pb.GetFilePlanResponse]] = {}

def _stub():
    ch = grpc.insecure_channel(f"{_MASTER_ADDR['host']}:{_MASTER_ADDR['port']}")
    return pb_grpc.MasterServiceStub(ch)

def _md():
    return (("x-user", USER),)

def _die(msg: str, code: int = 2):
    print(msg, file=sys.stderr, flush=True)
    raise SystemExit(code)

def _update_leader(error_msg: str) -> bool:
    if not error_msg or not error_msg.startswith("NotLeader:"):
        return False
    target = error_msg.split(":", 1)[1].strip()
    if not target:
        return False
    if ":" not in target:
        return False
    host, port = target.rsplit(":", 1)
    if not host or not port:
        return False
    try:
        _MASTER_ADDR["host"] = host
        _MASTER_ADDR["port"] = int(port)
        return True
    except ValueError:
        return False

def _call_with_leader(fn):
    for _ in range(2):
        stub = _stub()
        resp = fn(stub)
        if getattr(resp, "status", None) is None:
            return resp
        if resp.status.ok:
            return resp
        if _update_leader(resp.status.error.message if resp.status.error else ""):
            continue
        return resp
    return resp

def _get_file_plan(remote_path: str, refresh: bool = False) -> pb.GetFilePlanResponse:
    now = time.time()
    if not refresh:
        cached = _PLAN_CACHE.get(remote_path)
        if cached and cached[0] > now:
            return cached[1]
    r = _call_with_leader(
        lambda stub: stub.GetFilePlan(pb.GetFilePlanRequest(path=remote_path), timeout=6.0, metadata=_md())
    )
    if not r.status.ok:
        _die(r.status.error.message if r.status.error else "GetFilePlan failed")
    _PLAN_CACHE[remote_path] = (now + CACHE_TTL_S, r.data)
    return r.data

# -------------------------
# Namespace commands
# -------------------------
def mkdir(remote_dir: str, mode: int = 0o700):
    r = _call_with_leader(lambda stub: stub.Mkdir(pb.MkdirRequest(path=remote_dir, mode=mode), timeout=6.0, metadata=_md()))
    if not r.status.ok:
        _die(r.status.error.message if r.status.error else "mkdir failed")

def ls(remote_dir: str):
    r = _call_with_leader(lambda stub: stub.Ls(pb.LsRequest(path=remote_dir), timeout=6.0, metadata=_md()))
    if not r.status.ok:
        _die(r.status.error.message if r.status.error else "ls failed")
    for e in r.data.entries:
        print(e)

def stat(path: str):
    r = _call_with_leader(lambda stub: stub.Stat(pb.StatRequest(path=path), timeout=6.0, metadata=_md()))
    if not r.status.ok:
        _die(r.status.error.message if r.status.error else "stat failed")
    d = r.data
    kind = "dir" if d.is_dir else "file"
    print(f"{kind} owner={d.owner} mode={oct(d.mode)} size={d.size} chunks={d.chunk_count}")

def rm(path: str, recursive: bool = False):
    r = _call_with_leader(lambda stub: stub.Rm(pb.RmRequest(path=path, recursive=recursive), timeout=6.0, metadata=_md()))
    if not r.status.ok:
        _die(r.status.error.message if r.status.error else "rm failed")

def mv(src: str, dst: str):
    r = _call_with_leader(lambda stub: stub.Mv(pb.MvRequest(src=src, dst=dst), timeout=6.0, metadata=_md()))
    if not r.status.ok:
        _die(r.status.error.message if r.status.error else "mv failed")

# -------------------------
# Data-plane I/O
# -------------------------
def _put_to_pipeline(pipeline: List[pb.HostPort], chunk_id: str, version: int, data: bytes) -> int:
    if not pipeline:
        raise RuntimeError("empty pipeline")
    first = pipeline[0]
    forward = [(hp.host, int(hp.port)) for hp in pipeline[1:]]
    hdr = {"op": "PUT", "chunk_id": chunk_id, "version": int(version), "size": len(data), "forward_to": forward}
    sock = socket.create_connection((first.host, int(first.port)), timeout=8.0)
    send_msg(sock, hdr)
    # stream bytes
    mv = memoryview(data)
    off = 0
    while off < len(data):
        n = 256 * 1024
        sock.sendall(mv[off:off+n])
        off += n
    ack = recv_msg(sock)
    sock.close()
    if not ack.get("ok", False):
        raise RuntimeError(ack.get("err", "put failed"))
    return int(ack.get("checksum", 0))

def _get_from_replica(host: str, port: int, chunk_id: str, version: int) -> bytes:
    sock = socket.create_connection((host, int(port)), timeout=8.0)
    send_msg(sock, {"op": "GET", "chunk_id": chunk_id, "version": int(version)})
    hdr = recv_msg(sock)
    if not hdr.get("ok", False):
        sock.close()
        raise RuntimeError(hdr.get("err", "get failed"))
    size = int(hdr["size"])
    data = recv_exact(sock, size) if size > 0 else b""
    sock.close()
    return data

def upload_file(local_path: str, remote_path: str) -> None:
    local_path = str(local_path)
    st = os.stat(local_path)
    total = int(st.st_size)
    total_chunks = max(1, math.ceil(total / CHUNK_SIZE))

    rf = int(os.getenv("DISTRIFS_RF", "3"))

    print(
        f"[CLIENT] PUT {local_path} -> {remote_path} bytes={total} chunks={total_chunks} chunk_size={CHUNK_SIZE}",
        flush=True,
    )

    with open(local_path, "rb") as f:
        for idx in range(total_chunks):
            buf = f.read(CHUNK_SIZE) or b""

            # Wait up to 15s for enough active DataNodes (master enforces CP)
            deadline = time.time() + 15.0
            last_err = None
            while True:
                try:
                    assign = assign_chunk(remote_path, idx)
                    break
                except grpc.RpcError as e:
                    last_err = e
                    if (
                        e.code() in (grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.UNAVAILABLE)
                        and time.time() < deadline
                    ):
                        time.sleep(0.5)
                        continue
                    raise

            if len(assign.pipeline) < rf:
                raise RuntimeError(
                    f"CP write denied: replication factor not satisfiable (need {rf} active DataNodes). last={last_err}"
                )

            node_ids = store_to_pipeline(assign.chunk_id, int(assign.version), buf, list(assign.pipeline))
            crc = zlib.crc32(buf) & 0xFFFFFFFF
            commit_chunk(remote_path, idx, assign.chunk_id, int(assign.version), crc, len(buf), node_ids)

    print("[CLIENT] PUT complete.", flush=True)

# -------------------------
# High-level file ops
# -------------------------
def put(local_path: str, remote_path: str):
    p = os.path.abspath(local_path)
    if not os.path.exists(p):
        _die(f"Local file missing: {p}")
    total = os.path.getsize(p)
    print(f"[CLIENT] PUT {p} -> {remote_path} bytes={total} chunks={max(1,(total + CHUNK_SIZE - 1)//CHUNK_SIZE)} chunk_size={CHUNK_SIZE}", flush=True)

    with open(p, "rb") as f:
        idx = 0
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            last_err = None
            for attempt in range(3):
                try:
                    ar = _call_with_leader(
                        lambda stub: stub.AssignChunk(
                            pb.AssignChunkRequest(path=remote_path, index=idx, max_size=CHUNK_SIZE),
                            timeout=6.0,
                            metadata=_md(),
                        )
                    )
                    if not ar.status.ok:
                        raise RuntimeError(ar.status.error.message if ar.status.error else "assign failed")
                    chunk_id = ar.data.chunk_id
                    version = int(ar.data.version)
                    pipeline = list(ar.data.pipeline)
                    checksum = _put_to_pipeline(pipeline, chunk_id, version, data)
                    cr = _call_with_leader(
                        lambda stub: stub.CommitChunk(
                            pb.CommitChunkRequest(
                                path=remote_path,
                                index=idx,
                                chunk_id=chunk_id,
                                version=version,
                                size=len(data),
                                checksum=checksum,
                                pipeline=pipeline,
                            ),
                            timeout=6.0,
                            metadata=_md(),
                        )
                    )
                    if not cr.status.ok:
                        raise RuntimeError(cr.status.error.message if cr.status.error else "commit failed")
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(0.2 * (attempt + 1))
            if last_err is not None:
                raise RuntimeError(f"Failed PUT chunk idx={idx}: {last_err}")
            idx += 1
    _PLAN_CACHE.pop(remote_path, None)
    print("[CLIENT] PUT complete.", flush=True)

def get(remote_path: str, out_path: str):
    plan = _get_file_plan(remote_path)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    with open(out_path, "wb") as out:
        for cp in plan.chunks:
            if not cp.replicas:
                raise RuntimeError(f"No replicas for chunk {cp.chunk_id}")
            last = None
            for hp in cp.replicas:
                try:
                    data = _get_from_replica(hp.host, int(hp.port), cp.chunk_id, int(cp.version))
                    # size sanity
                    if len(data) != int(cp.size):
                        raise RuntimeError("short read")
                    out.write(data)
                    last = None
                    break
                except Exception as e:
                    last = e
            if last is not None:
                refreshed = _get_file_plan(remote_path, refresh=True)
                replanned = next((p for p in refreshed.chunks if p.index == cp.index), None)
                if replanned and replanned.replicas:
                    last_retry = None
                    for hp in replanned.replicas:
                        try:
                            data = _get_from_replica(hp.host, int(hp.port), replanned.chunk_id, int(replanned.version))
                            if len(data) != int(replanned.size):
                                raise RuntimeError("short read")
                            out.write(data)
                            last_retry = None
                            break
                        except Exception as e:
                            last_retry = e
                    if last_retry is None:
                        continue
                    raise RuntimeError(f"Chunk read failed after re-plan: {replanned.chunk_id}: {last_retry}")
                raise RuntimeError(f"Chunk read failed: {cp.chunk_id}: {last}")
    print(f"[CLIENT] GET {remote_path} -> {out_path} bytes={plan.size}", flush=True)

def getrange(remote_path: str, offset: int, length: int, out_path: str):
    plan = _get_file_plan(remote_path)
    # compute which chunks to fetch
    remaining = length
    cur_off = 0
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "wb") as out:
        for cp in plan.chunks:
            if remaining <= 0:
                break
            chunk_start = cur_off
            chunk_end = cur_off + int(cp.size)
            cur_off = chunk_end
            if chunk_end <= offset:
                continue
            # need this chunk
            last = None
            data = None
            for hp in cp.replicas:
                try:
                    data = _get_from_replica(hp.host, int(hp.port), cp.chunk_id, int(cp.version))
                    last = None
                    break
                except Exception as e:
                    last = e
            if last is not None or data is None:
                refreshed = _get_file_plan(remote_path, refresh=True)
                replanned = next((p for p in refreshed.chunks if p.index == cp.index), None)
                if replanned and replanned.replicas:
                    last_retry = None
                    data = None
                    for hp in replanned.replicas:
                        try:
                            data = _get_from_replica(hp.host, int(hp.port), replanned.chunk_id, int(replanned.version))
                            last_retry = None
                            break
                        except Exception as e:
                            last_retry = e
                    if last_retry is not None or data is None:
                        raise RuntimeError(f"Chunk read failed after re-plan: {replanned.chunk_id}: {last_retry}")
                    cp = replanned
                else:
                    raise RuntimeError(f"Chunk read failed: {cp.chunk_id}: {last}")
            # slice
            s = max(0, offset - chunk_start)
            e = min(int(cp.size), s + remaining)
            out.write(data[s:e])
            remaining -= (e - s)
    print(f"[CLIENT] GETRANGE {remote_path} offset={offset} length={length} -> {out_path}", flush=True)

def usage():
    print("Usage:\n"
          "  python client.py mkdir <remote_dir>\n"
          "  python client.py ls <remote_dir>\n"
          "  python client.py stat <path>\n"
          "  python client.py put <local_path> <remote_path>\n"
          "  python client.py get <remote_path> <out_path>\n"
          "  python client.py getrange <remote_path> <offset> <length> <out_path>\n"
          "  python client.py rm <path>\n"
          "  python client.py mv <src> <dst>\n", flush=True)

def main():
    if len(sys.argv) < 2:
        usage()
        return
    cmd = sys.argv[1]
    try:
        if cmd == "mkdir" and len(sys.argv) == 3:
            mkdir(sys.argv[2])
        elif cmd == "ls" and len(sys.argv) == 3:
            ls(sys.argv[2])
        elif cmd == "stat" and len(sys.argv) == 3:
            stat(sys.argv[2])
        elif cmd == "put" and len(sys.argv) == 4:
            put(sys.argv[2], sys.argv[3])
        elif cmd == "get" and len(sys.argv) == 4:
            get(sys.argv[2], sys.argv[3])
        elif cmd == "getrange" and len(sys.argv) == 6:
            getrange(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])
        elif cmd == "rm" and len(sys.argv) == 3:
            rm(sys.argv[2], recursive=False)
        elif cmd == "mv" and len(sys.argv) == 4:
            mv(sys.argv[2], sys.argv[3])
        else:
            usage()
            raise SystemExit(2)
    except SystemExit:
        raise
    except Exception as e:
        _die(str(e), code=1)

if __name__ == "__main__":
    main()
