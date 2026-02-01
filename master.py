# master.py - DistriFS Master (metadata + coordinator)
from __future__ import annotations

import os
import time
import threading
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import grpc
from concurrent import futures

from utils import wal_append, wal_iter, ensure_proto_generated, read_master_epoch

# Generate pb2 on demand
ensure_proto_generated(force=bool(os.getenv("DISTRIFS_FORCE_PROTO")))

import distrifs_pb2 as pb
import distrifs_pb2_grpc as pb_grpc

if os.name == "nt" and os.getenv("DISTRIFS_IGNORE_CTRL_C") == "1":
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleCtrlHandler(None, True)
    except Exception:
        pass

# -------------------------
# Configuration
# -------------------------
MASTER_HOST = os.getenv("DISTRIFS_MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("DISTRIFS_MASTER_PORT", "9000"))
RF = int(os.getenv("DISTRIFS_RF", "3"))

HB_INTERVAL = float(os.getenv("DISTRIFS_HB_INTERVAL_S", "3.0"))
HB_TIMEOUT = float(os.getenv("DISTRIFS_HB_TIMEOUT_S", "9.0"))

WAL_PATH = os.getenv("DISTRIFS_WAL", "master.wal")
EPOCH_PATH = os.getenv("DISTRIFS_EPOCH_PATH", "master.epoch")
LEADER_LOCK = os.getenv("DISTRIFS_LEADER_LOCK", "master.leader.lock")

GC_INTERVAL = float(os.getenv("DISTRIFS_GC_INTERVAL_S", "10.0"))
REPAIR_INTERVAL = float(os.getenv("DISTRIFS_REPAIR_INTERVAL_S", "5.0"))

# WAL opcodes
OP_MKDIR = 1
OP_CREATE_FILE = 2
OP_ASSIGN_CHUNK = 3
OP_COMMIT_CHUNK = 4
OP_RM = 5
OP_MV = 6

# -------------------------
# Data model
# -------------------------
@dataclass
class NodeInfo:
    node_id: str
    host: str
    data_port: int
    control_port: int
    last_hb: float = field(default_factory=lambda: time.time())
    alive: bool = True

    def data_addr(self) -> Tuple[str, int]:
        return (self.host, self.data_port)

    def ctrl_addr(self) -> Tuple[str, int]:
        return (self.host, self.control_port)

@dataclass
class ChunkRecord:
    chunk_id: str
    version: int = 1
    size: int = 0
    checksum: int = 0  # CRC32
    replicas: Set[str] = field(default_factory=set)  # node_ids

@dataclass
class Inode:
    is_dir: bool
    owner: str
    mode: int
    children: Set[str] = field(default_factory=set)  # names, not full paths
    chunks: Dict[int, str] = field(default_factory=dict)  # index -> chunk_id

# -------------------------
# Master state
# -------------------------
STATE_LOCK = threading.RLock()

NODES: Dict[str, NodeInfo] = {}               # node_id -> NodeInfo
PATHS: Dict[str, Inode] = {}                  # full path -> inode
CHUNKS: Dict[str, ChunkRecord] = {}           # chunk_id -> chunk record
FILE_INDEX_TO_CHUNK: Dict[Tuple[str, int], str] = {}  # (path,index) -> chunk_id
CHUNK_REFCOUNT: Dict[str, int] = {}           # chunk_id -> count
GC_QUEUE: List[str] = []                      # chunk_ids pending delete

LEADER_EPOCH = 0
STOP = threading.Event()

def _ok(data=None):
    return pb.Status(ok=True), data

def _err(msg: str):
    return pb.Status(ok=False, error=pb.Error(message=msg)), None

# -------------------------
# Path helpers + permissions
# -------------------------
def _norm(p: str) -> str:
    if not p:
        return "/"
    if not p.startswith("/"):
        p = "/" + p
    # collapse multiple slashes
    while "//" in p:
        p = p.replace("//", "/")
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p

def _parent(p: str) -> str:
    p = _norm(p)
    if p == "/":
        return "/"
    i = p.rfind("/")
    return "/" if i == 0 else p[:i]

def _name(p: str) -> str:
    p = _norm(p)
    if p == "/":
        return "/"
    return p.split("/")[-1]

def _mode_bits(inode: Inode) -> Tuple[int,int,int]:
    # owner bits only for now: rwx in high triad
    owner = (inode.mode >> 6) & 0b111
    group = (inode.mode >> 3) & 0b111
    other = inode.mode & 0b111
    return owner, group, other

def _can_read(user: str, inode: Inode) -> bool:
    owner, group, other = _mode_bits(inode)
    bits = owner if user == inode.owner else other
    return bool(bits & 0b100)

def _can_write(user: str, inode: Inode) -> bool:
    owner, group, other = _mode_bits(inode)
    bits = owner if user == inode.owner else other
    return bool(bits & 0b010)

def _can_exec(user: str, inode: Inode) -> bool:
    owner, group, other = _mode_bits(inode)
    bits = owner if user == inode.owner else other
    return bool(bits & 0b001)

def _require_parent_dir(path: str) -> Tuple[Optional[Inode], Optional[str]]:
    path = _norm(path)
    par = _parent(path)
    inode = PATHS.get(par)
    if not inode or not inode.is_dir:
        return None, f"Parent directory missing: {par}"
    return inode, None

def _user_from_md(context) -> str:
    try:
        md = dict(context.invocation_metadata() or [])
        return md.get("x-user", "user") or "user"
    except Exception:
        return "user"

# -------------------------
# Leadership (lock + fencing epoch)
# -------------------------
def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False

def _acquire_leader_lock():
    global _LEADER_LOCK_FH

    path = LEADER_LOCK
    fh = open(path, "a+b")

    try:
        if os.name == "nt":
            import msvcrt
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)  # lock 1 byte
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return 0  # could not become leader

    epoch = int(time.time() * 1000)
    fh.seek(0)
    fh.truncate()
    fh.write(f"{os.getpid()} {epoch}\n".encode("utf-8"))
    fh.flush()
    os.fsync(fh.fileno())

    _LEADER_LOCK_FH = fh
    return epoch


def _release_leader_lock():
    global _LEADER_LOCK_FH
    if not _LEADER_LOCK_FH:
        return

    try:
        if os.name == "nt":
            import msvcrt
            _LEADER_LOCK_FH.seek(0)
            msvcrt.locking(_LEADER_LOCK_FH.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(_LEADER_LOCK_FH.fileno(), fcntl.LOCK_UN)
    finally:
        try:
            _LEADER_LOCK_FH.close()
        finally:
            _LEADER_LOCK_FH = None

# -------------------------
# WAL recovery
# -------------------------
def _ensure_root() -> None:
    if "/" not in PATHS:
        PATHS["/"] = Inode(is_dir=True, owner="root", mode=0o777, children=set(), chunks={})

def _apply_mkdir(path: str, owner: str, mode: int) -> None:
    path = _norm(path)
    _ensure_root()
    if path in PATHS:
        return
    par = _parent(path)
    if par not in PATHS:
        return
    PATHS[path] = Inode(is_dir=True, owner=owner, mode=mode, children=set(), chunks={})
    PATHS[par].children.add(_name(path))

def _apply_create_file(path: str, owner: str, mode: int) -> None:
    path = _norm(path)
    _ensure_root()
    if path in PATHS:
        return
    par = _parent(path)
    if par not in PATHS:
        return
    PATHS[path] = Inode(is_dir=False, owner=owner, mode=mode, children=set(), chunks={})
    PATHS[par].children.add(_name(path))

def _apply_assign_chunk(path: str, index: int, chunk_id: str, version: int) -> None:
    path = _norm(path)
    if path not in PATHS:
        return
    inode = PATHS[path]
    inode.chunks[index] = chunk_id
    FILE_INDEX_TO_CHUNK[(path, index)] = chunk_id
    if chunk_id not in CHUNKS:
        CHUNKS[chunk_id] = ChunkRecord(chunk_id=chunk_id, version=version)
        CHUNK_REFCOUNT[chunk_id] = CHUNK_REFCOUNT.get(chunk_id, 0) + 1
    else:
        CHUNKS[chunk_id].version = max(CHUNKS[chunk_id].version, version)

def _apply_commit_chunk(chunk_id: str, version: int, size: int, checksum: int, replicas: List[str]) -> None:
    if chunk_id not in CHUNKS:
        CHUNKS[chunk_id] = ChunkRecord(chunk_id=chunk_id, version=version)
        CHUNK_REFCOUNT[chunk_id] = CHUNK_REFCOUNT.get(chunk_id, 0) + 1
    c = CHUNKS[chunk_id]
    c.version = max(c.version, version)
    c.size = size
    c.checksum = checksum
    c.replicas = set(replicas)

def _apply_rm(path: str) -> None:
    path = _norm(path)
    if path not in PATHS or path == "/":
        return
    inode = PATHS[path]
    # decrement refcounts for file chunks
    if not inode.is_dir:
        for idx, cid in list(inode.chunks.items()):
            _dec_ref(cid)
            inode.chunks.pop(idx, None)
            FILE_INDEX_TO_CHUNK.pop((path, idx), None)
    # remove from parent
    par = _parent(path)
    if par in PATHS:
        PATHS[par].children.discard(_name(path))
    # remove inode
    del PATHS[path]

def _apply_mv(src: str, dst: str) -> None:
    src = _norm(src); dst = _norm(dst)
    if src not in PATHS or dst in PATHS or src == "/":
        return
    src_inode = PATHS[src]
    # detach
    src_par = _parent(src)
    if src_par in PATHS:
        PATHS[src_par].children.discard(_name(src))
    # attach
    dst_par = _parent(dst)
    if dst_par not in PATHS or not PATHS[dst_par].is_dir:
        return
    PATHS[dst] = src_inode
    PATHS[dst_par].children.add(_name(dst))
    # update chunk mappings for file
    if not src_inode.is_dir:
        for idx, cid in list(src_inode.chunks.items()):
            FILE_INDEX_TO_CHUNK.pop((src, idx), None)
            FILE_INDEX_TO_CHUNK[(dst, idx)] = cid
    # delete old key
    del PATHS[src]

def recover() -> None:
    _ensure_root()
    for op, payload in wal_iter(WAL_PATH) or []:
        try:
            if op == OP_MKDIR:
                _apply_mkdir(payload["path"], payload["owner"], payload["mode"])
            elif op == OP_CREATE_FILE:
                _apply_create_file(payload["path"], payload["owner"], payload["mode"])
            elif op == OP_ASSIGN_CHUNK:
                _apply_assign_chunk(payload["path"], int(payload["index"]), payload["chunk_id"], int(payload["version"]))
            elif op == OP_COMMIT_CHUNK:
                _apply_commit_chunk(payload["chunk_id"], int(payload["version"]), int(payload["size"]), int(payload["checksum"]), list(payload.get("replicas", [])))
            elif op == OP_RM:
                _apply_rm(payload["path"])
            elif op == OP_MV:
                _apply_mv(payload["src"], payload["dst"])
        except Exception:
            continue
    print("[MASTER] WAL replay complete; replica locations will be rebuilt via block reports.", flush=True)

# -------------------------
# Refcount + GC
# -------------------------
def _dec_ref(chunk_id: str) -> None:
    CHUNK_REFCOUNT[chunk_id] = CHUNK_REFCOUNT.get(chunk_id, 1) - 1
    if CHUNK_REFCOUNT[chunk_id] <= 0:
        CHUNK_REFCOUNT.pop(chunk_id, None)
        GC_QUEUE.append(chunk_id)

def _gc_loop() -> None:
    """Garbage-collect unreferenced chunks.

    Important for tests: we *drain* the GC queue each cycle, rather than deleting
    one chunk per interval (otherwise large files take too long to collect).
    """
    while not STOP.is_set():
        # Sleep in small quanta so STOP can interrupt promptly.
        slept = 0.0
        while slept < GC_INTERVAL and not STOP.is_set():
            dt = min(0.25, GC_INTERVAL - slept)
            time.sleep(dt)
            slept += dt
        if STOP.is_set():
            break

        # Drain queue snapshot under lock.
        batch: list[tuple[str, list[str]]] = []
        with STATE_LOCK:
            while GC_QUEUE:
                chunk_id = GC_QUEUE.pop(0)
                rec = CHUNKS.pop(chunk_id, None)
                if not rec:
                    continue
                batch.append((chunk_id, list(rec.replicas)))

        # Best-effort delete from known replicas (no lock held during RPC).
        for chunk_id, replicas in batch:
            for node_id in replicas:
                ni = None
                with STATE_LOCK:
                    ni = NODES.get(node_id)
                if not ni:
                    continue
                try:
                    ch = grpc.insecure_channel(f"{ni.host}:{ni.control_port}")
                    stub = pb_grpc.DataNodeControlStub(ch)
                    md = (("x-epoch", str(LEADER_EPOCH)),)
                    stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=chunk_id), timeout=4.0, metadata=md)
                except Exception:
                    pass

def _mark_dead_nodes() -> List[str]:
    now = time.time()
    dead = []
    for node_id, ni in list(NODES.items()):
        if ni.alive and (now - ni.last_hb) > HB_TIMEOUT:
            ni.alive = False
            dead.append(node_id)
    return dead

def _repair_loop() -> None:
    while not STOP.is_set():
        time.sleep(REPAIR_INTERVAL)
        with STATE_LOCK:
            dead = _mark_dead_nodes()
            if dead:
                # remove from replica sets
                for cid, rec in CHUNKS.items():
                    rec.replicas.difference_update(dead)

            # build list of under-replicated chunks
            under = []
            for cid, rec in CHUNKS.items():
                if rec.size <= 0:
                    continue
                if len([n for n in rec.replicas if NODES.get(n) and NODES[n].alive]) < RF:
                    under.append(cid)
        for cid in under[:20]:
            _repair_one(cid)

def _repair_one(chunk_id: str) -> None:
    with STATE_LOCK:
        rec = CHUNKS.get(chunk_id)
        if not rec:
            return
        sources = [n for n in rec.replicas if NODES.get(n) and NODES[n].alive]
        targets = [n for n in NODES.keys() if NODES[n].alive and n not in rec.replicas]
        if not sources or not targets:
            return
        src = NODES[sources[0]]
        tgt = NODES[targets[0]]
        version = rec.version
    try:
        ch = grpc.insecure_channel(f"{src.host}:{src.control_port}")
        stub = pb_grpc.DataNodeControlStub(ch)
        md = (("x-epoch", str(LEADER_EPOCH)),)
        r = stub.CopyChunk(pb.CopyChunkRequest(
            chunk_id=chunk_id,
            version=version,
            target_data=pb.HostPort(host=tgt.host, port=tgt.data_port),
        ), timeout=8.0, metadata=md)
        if r.ok:
            with STATE_LOCK:
                rec2 = CHUNKS.get(chunk_id)
                if rec2:
                    rec2.replicas.add(tgt.node_id)
    except Exception:
        pass

# -------------------------
# gRPC service
# -------------------------
class Master(pb_grpc.MasterServiceServicer):
    def RegisterDataNode(self, request, context):
        user = _user_from_md(context)  # unused but keeps pattern
        host = (request.host or "").strip()
        if not host:
            # derive from peer
            peer = context.peer() or ""
            if peer.startswith("ipv4:"):
                try:
                    host = peer.split(":")[1]
                except Exception:
                    host = "127.0.0.1"
            else:
                host = "127.0.0.1"
        data_port = int(request.data_port)
        control_port = int(request.control_port)
        node_id = f"node-{data_port}"

        with STATE_LOCK:
            NODES[node_id] = NodeInfo(node_id=node_id, host=host, data_port=data_port, control_port=control_port, last_hb=time.time(), alive=True)
        st, _ = _ok()
        return pb.RegisterDataNodeReply(status=st, data=pb.RegisterDataNodeResponse(node_id=node_id, epoch=LEADER_EPOCH))

    def Heartbeat(self, request, context):
        if int(request.epoch) != LEADER_EPOCH:
            st, _ = _err("stale epoch")
            return pb.HeartbeatReply(status=st, data=pb.HeartbeatResponse())
        node_id = request.node_id
        with STATE_LOCK:
            ni = NODES.get(node_id)
            if ni:
                ni.last_hb = time.time()
                ni.alive = True
        st, _ = _ok()
        return pb.HeartbeatReply(status=st, data=pb.HeartbeatResponse())

    def BlockReport(self, request, context):
        if int(request.epoch) != LEADER_EPOCH:
            st, _ = _err("stale epoch")
            return pb.BlockReportReply(status=st, data=pb.BlockReportResponse())
        node_id = request.node_id
        reported = dict(request.chunks)
        deletes: List[Tuple[str,int]] = []
        with STATE_LOCK:
            ni = NODES.get(node_id)
            if ni:
                ni.last_hb = time.time()
                ni.alive = True
            # incorporate reports
            for cid, ver in reported.items():
                rec = CHUNKS.get(cid)
                if not rec:
                    # unknown chunk -> schedule delete
                    deletes.append((cid, ver))
                    continue
                if int(ver) != rec.version:
                    deletes.append((cid, ver))
                    continue
                rec.replicas.add(node_id)
            # also: chunks master expects on node but not reported -> remove
            for cid, rec in CHUNKS.items():
                if node_id in rec.replicas and cid not in reported:
                    rec.replicas.discard(node_id)
        # best-effort delete stale/unknown on that node
        if deletes:
            ni = None
            with STATE_LOCK:
                ni = NODES.get(node_id)
            if ni:
                try:
                    ch = grpc.insecure_channel(f"{ni.host}:{ni.control_port}")
                    stub = pb_grpc.DataNodeControlStub(ch)
                    md = (("x-epoch", str(LEADER_EPOCH)),)
                    for cid, _ in deletes[:50]:
                        stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=cid), timeout=4.0, metadata=md)
                except Exception:
                    pass
        st, _ = _ok()
        return pb.BlockReportReply(status=st, data=pb.BlockReportResponse())

    # -------- Namespace ops --------
    def Mkdir(self, request, context):
        user = _user_from_md(context)
        path = _norm(request.path)
        mode = int(request.mode) if request.mode else 0o755
        with STATE_LOCK:
            _ensure_root()
            if path in PATHS:
                inode = PATHS[path]
                if inode.is_dir:
                    st, _ = _ok()
                    return pb.MkdirReply(status=st, data=pb.MkdirResponse())
                st, _ = _err("Path exists and is a file")
                return pb.MkdirReply(status=st, data=pb.MkdirResponse())

            par_inode, err = _require_parent_dir(path)
            if err:
                st, _ = _err(err)
                return pb.MkdirReply(status=st, data=pb.MkdirResponse())
            if not _can_exec(user, par_inode) or not _can_write(user, par_inode):
                st, _ = _err("PermissionDenied")
                return pb.MkdirReply(status=st, data=pb.MkdirResponse())

            wal_append(WAL_PATH, OP_MKDIR, {"path": path, "owner": user, "mode": mode})
            _apply_mkdir(path, user, mode)

        st, _ = _ok()
        return pb.MkdirReply(status=st, data=pb.MkdirResponse())

    def Ls(self, request, context):
        user = _user_from_md(context)
        path = _norm(request.path)
        with STATE_LOCK:
            inode = PATHS.get(path)
            if not inode or not inode.is_dir:
                st, _ = _err("No such directory")
                return pb.LsReply(status=st, data=pb.LsResponse())
            if not _can_exec(user, inode) or not _can_read(user, inode):
                st, _ = _err("PermissionDenied")
                return pb.LsReply(status=st, data=pb.LsResponse())
            entries = sorted(list(inode.children))
        st, _ = _ok()
        return pb.LsReply(status=st, data=pb.LsResponse(entries=entries))

    def Stat(self, request, context):
        user = _user_from_md(context)
        path = _norm(request.path)
        with STATE_LOCK:
            inode = PATHS.get(path)
            if not inode:
                st, _ = _err("No such file")
                return pb.StatReply(status=st, data=pb.StatResponse())
            if not _can_read(user, inode):
                st, _ = _err("PermissionDenied")
                return pb.StatReply(status=st, data=pb.StatResponse())
            if inode.is_dir:
                st, _ = _ok()
                return pb.StatReply(status=st, data=pb.StatResponse(is_dir=True, mode=inode.mode, owner=inode.owner, size=0, chunk_count=0))
            # file size = sum chunk sizes
            total = 0
            for idx, cid in inode.chunks.items():
                rec = CHUNKS.get(cid)
                if rec:
                    total += rec.size
            st, _ = _ok()
            return pb.StatReply(status=st, data=pb.StatResponse(is_dir=False, mode=inode.mode, owner=inode.owner, size=total, chunk_count=len(inode.chunks)))

    def Rm(self, request, context):
        user = _user_from_md(context)
        path = _norm(request.path)
        with STATE_LOCK:
            if path not in PATHS or path == "/":
                st, _ = _err("No such file")
                return pb.RmReply(status=st, data=pb.RmResponse())
            inode = PATHS[path]
            par = PATHS.get(_parent(path))
            if par and (not _can_write(user, par) or not _can_exec(user, par)):
                st, _ = _err("PermissionDenied")
                return pb.RmReply(status=st, data=pb.RmResponse())
            if inode.is_dir and inode.children and not request.recursive:
                st, _ = _err("Directory not empty")
                return pb.RmReply(status=st, data=pb.RmResponse())

            # recursive delete for dirs
            to_delete = [path]
            if inode.is_dir and request.recursive:
                stack = [path]
                while stack:
                    cur = stack.pop()
                    cur_inode = PATHS.get(cur)
                    if not cur_inode or not cur_inode.is_dir:
                        continue
                    for child in list(cur_inode.children):
                        cpath = cur.rstrip("/") + "/" + child if cur != "/" else "/" + child
                        to_delete.append(cpath)
                        if PATHS.get(cpath) and PATHS[cpath].is_dir:
                            stack.append(cpath)

            # WAL then apply deletes (files first)
            for p in sorted(to_delete, key=lambda x: -len(x)):
                wal_append(WAL_PATH, OP_RM, {"path": p})
                _apply_rm(p)

        st, _ = _ok()
        return pb.RmReply(status=st, data=pb.RmResponse())

    def Mv(self, request, context):
        user = _user_from_md(context)
        src = _norm(request.src); dst = _norm(request.dst)
        with STATE_LOCK:
            if src not in PATHS:
                st, _ = _err("No such file")
                return pb.MvReply(status=st, data=pb.MvResponse())
            if dst in PATHS:
                st, _ = _err("Destination exists")
                return pb.MvReply(status=st, data=pb.MvResponse())
            dst_par_inode, err = _require_parent_dir(dst)
            if err:
                st, _ = _err(err)
                return pb.MvReply(status=st, data=pb.MvResponse())
            if not _can_write(user, dst_par_inode) or not _can_exec(user, dst_par_inode):
                st, _ = _err("PermissionDenied")
                return pb.MvReply(status=st, data=pb.MvResponse())
            wal_append(WAL_PATH, OP_MV, {"src": src, "dst": dst})
            _apply_mv(src, dst)
        st, _ = _ok()
        return pb.MvReply(status=st, data=pb.MvResponse())

    # -------- Chunk ops --------
    def AssignChunk(self, request, context):
        user = _user_from_md(context)
        path = _norm(request.path)
        idx = int(request.index)
        max_size = int(request.max_size) if request.max_size else 0
        with STATE_LOCK:
            _ensure_root()
            # ensure parent dir exists and writable
            par_inode, err = _require_parent_dir(path)
            if err:
                st, _ = _err(err)
                return pb.AssignChunkReply(status=st, data=pb.AssignChunkResponse())
            if not _can_write(user, par_inode) or not _can_exec(user, par_inode):
                st, _ = _err("PermissionDenied")
                return pb.AssignChunkReply(status=st, data=pb.AssignChunkResponse())

            if path not in PATHS:
                wal_append(WAL_PATH, OP_CREATE_FILE, {"path": path, "owner": user, "mode": 0o644})
                _apply_create_file(path, user, 0o644)

            inode = PATHS[path]
            if inode.is_dir:
                st, _ = _err("Is a directory")
                return pb.AssignChunkReply(status=st, data=pb.AssignChunkResponse())

            # choose active nodes
            active = [ni for ni in NODES.values() if ni.alive]
            if len(active) < RF:
                st, _ = _err("CP write denied: not enough active DataNodes for replication factor")
                return pb.AssignChunkReply(status=st, data=pb.AssignChunkResponse())
            # deterministic but spread: sort by port then rotate by idx
            active.sort(key=lambda n: n.data_port)
            rot = idx % len(active)
            pick = (active[rot:] + active[:rot])[:RF]

            # existing chunk?
            existing_cid = inode.chunks.get(idx)
            if existing_cid:
                rec = CHUNKS.get(existing_cid)
                if not rec:
                    rec = ChunkRecord(chunk_id=existing_cid, version=1)
                    CHUNKS[existing_cid] = rec
                    CHUNK_REFCOUNT[existing_cid] = CHUNK_REFCOUNT.get(existing_cid, 0) + 1
                rec.version += 1
                version = rec.version
                chunk_id = existing_cid
            else:
                chunk_id = f"chunk-{uuid.uuid4().hex}"
                version = 1
                wal_append(WAL_PATH, OP_ASSIGN_CHUNK, {"path": path, "index": idx, "chunk_id": chunk_id, "version": version})
                _apply_assign_chunk(path, idx, chunk_id, version)

            pipeline = [pb.HostPort(host=n.host, port=n.data_port) for n in pick]
        st, _ = _ok()
        return pb.AssignChunkReply(status=st, data=pb.AssignChunkResponse(chunk_id=chunk_id, version=version, pipeline=pipeline))

    def CommitChunk(self, request, context):
        user = _user_from_md(context)
        path = _norm(request.path)
        idx = int(request.index)
        chunk_id = request.chunk_id
        version = int(request.version)
        size = int(request.size)
        checksum = int(request.checksum)
        # pipeline nodes by port -> node_id
        pipeline = [(hp.host, int(hp.port)) for hp in request.pipeline]
        with STATE_LOCK:
            inode = PATHS.get(path)
            if not inode or inode.is_dir:
                st, _ = _err("No such file")
                return pb.CommitChunkReply(status=st, data=pb.CommitChunkResponse())
            if inode.owner != user and not _can_write(user, inode):
                st, _ = _err("PermissionDenied")
                return pb.CommitChunkReply(status=st, data=pb.CommitChunkResponse())

            # map pipeline to node_ids
            node_ids = []
            for host, port in pipeline:
                node_ids.append(f"node-{port}")
            # WAL then apply
            wal_append(WAL_PATH, OP_COMMIT_CHUNK, {"chunk_id": chunk_id, "version": version, "size": size, "checksum": checksum, "replicas": node_ids})
            _apply_commit_chunk(chunk_id, version, size, checksum, node_ids)
        st, _ = _ok()
        return pb.CommitChunkReply(status=st, data=pb.CommitChunkResponse())

    def GetFilePlan(self, request, context):
        user = _user_from_md(context)
        path = _norm(request.path)
        with STATE_LOCK:
            inode = PATHS.get(path)
            if not inode or inode.is_dir:
                st, _ = _err("No such file")
                return pb.GetFilePlanReply(status=st, data=pb.GetFilePlanResponse())
            if not _can_read(user, inode):
                st, _ = _err("PermissionDenied")
                return pb.GetFilePlanReply(status=st, data=pb.GetFilePlanResponse())
            plans = []
            total = 0
            for idx in sorted(inode.chunks.keys()):
                cid = inode.chunks[idx]
                rec = CHUNKS.get(cid)
                if not rec:
                    continue
                total += rec.size
                replicas = []
                for node_id in list(rec.replicas):
                    ni = NODES.get(node_id)
                    if ni and ni.alive:
                        replicas.append(pb.HostPort(host=ni.host, port=ni.data_port))
                plans.append(pb.ChunkPlan(index=idx, chunk_id=cid, version=rec.version, size=rec.size, checksum=rec.checksum, replicas=replicas))
        st, _ = _ok()
        return pb.GetFilePlanReply(status=st, data=pb.GetFilePlanResponse(size=total, chunks=plans))

# -------------------------
# Server main
# -------------------------
def serve():
    global LEADER_EPOCH
    recover()

    LEADER_EPOCH = _acquire_leader_lock()
    if not LEADER_EPOCH:
        return
    print("[MASTER] Acquired leader lock; starting service.", flush=True)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pb_grpc.add_MasterServiceServicer_to_server(Master(), server)

    # IMPORTANT: this while True MUST be indented inside serve()
    while True:
        try:
            server.add_insecure_port(f"{MASTER_HOST}:{MASTER_PORT}")
            break
        except RuntimeError as e:
            if STOP.is_set():
                raise
            print(f"[MASTER] bind retry {MASTER_HOST}:{MASTER_PORT}: {e}", flush=True)
            time.sleep(0.25)

    # background workers (after bind succeeds)
    threading.Thread(target=_repair_loop, daemon=True).start()
    threading.Thread(target=_gc_loop, daemon=True).start()

    server.start()
    print(f"[MASTER] LEADER listening on {MASTER_HOST}:{MASTER_PORT} RF={RF} epoch={LEADER_EPOCH}", flush=True)

    try:
        while not STOP.is_set():
            time.sleep(0.5)
    finally:
        STOP.set()
        server.stop(0)
        _release_leader_lock()

if __name__ == "__main__":
    serve()
