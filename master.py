import os
import time
import threading
import uuid
import sys
import importlib.util
from typing import Any, Dict, List, Tuple, Set, Optional

import grpc
from concurrent import futures

from utils import wal_append, wal_iter, ensure_proto_generated, storage_root, ensure_dir

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
print("[MASTER] USING:", pb_grpc.__file__)


MASTER_HOST = os.getenv("DISTRIFS_MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("DISTRIFS_MASTER_PORT", "9000"))

WAL_PATH = os.getenv("DISTRIFS_MASTER_WAL", os.path.join(storage_root(), "master.wal"))

REPLICATION_FACTOR = int(os.getenv("DISTRIFS_REPLICATION_FACTOR", "3"))
HEARTBEAT_INTERVAL = int(os.getenv("DISTRIFS_HEARTBEAT_INTERVAL", "3"))
HEARTBEAT_TIMEOUT = int(os.getenv("DISTRIFS_HEARTBEAT_TIMEOUT", str(HEARTBEAT_INTERVAL * 3)))  # default 9s
RECOVERY_INTERVAL = int(os.getenv("DISTRIFS_RECOVERY_INTERVAL", "2"))
PENDING_TIMEOUT = int(os.getenv("DISTRIFS_PENDING_TIMEOUT", "60"))

# Leader election / split-brain fencing (single leader via OS lock)
LOCK_PATH = os.getenv("DISTRIFS_MASTER_LOCK_PATH", os.path.join(storage_root(), "master.lock"))

# ------------------------------
# In-memory state (RAM)
# ------------------------------
DIRS: Dict[str, Dict[str, Any]] = {}   # path -> {owner, mode, dirs:set(fullpaths), files:set(fullpaths)}
FILES: Dict[str, Dict[str, Any]] = {}  # path -> {owner, mode, chunks:[chunk_id], versions:[int], checksums:[int], sizes:[int], total_size:int}

# chunk registry for COMMITTED data; replicas are VOLATILE and rebuilt by block reports after restart
CHUNK_REGISTRY: Dict[str, Dict[str, Any]] = {}  # chunk_id -> {version:int, checksum:int, size:int, replicas:set(node_id)}

# pending assignments (durable via WAL Assign; cleared on Commit or timeout)
PENDING: Dict[Tuple[str, int], Dict[str, Any]] = {}  # (path, index) -> {chunk_id, version, pipeline_node_ids, ts}

ACTIVE_NODES: Dict[str, Dict[str, Any]] = {}  # node_id -> {host, data_port, control_port, last_seen}

# Locks
DIRS_LOCK = threading.RLock()
FILES_LOCK = threading.RLock()
CHUNK_LOCK = threading.RLock()
PENDING_LOCK = threading.RLock()
NODES_LOCK = threading.Lock()

# WAL opcodes
OP_MKDIR = 1
OP_CREATE_FILE_META = 2
OP_ASSIGN = 3
OP_COMMIT = 4
OP_DELETE_PATH = 5
OP_RENAME_PATH = 6


def _norm(p: str) -> str:
    if not p:
        return "/"
    if not p.startswith("/"):
        p = "/" + p
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p


def _parent(p: str) -> str:
    p = _norm(p)
    if p == "/":
        return "/"
    parts = p.split("/")
    if len(parts) <= 2:
        return "/"
    return "/".join(parts[:-1])


def _caller(context) -> str:
    # Permission enforcement via gRPC metadata, no proto changes needed.
    try:
        md = dict(context.invocation_metadata() or [])
        u = md.get("x-user") or md.get("user") or "user"
        return str(u)
    except Exception:
        return "user"


def _perm_check(mode: int, owner: str, user: str, need: str) -> None:
    """
    Very small POSIX-ish model: owner bits if user==owner else "other" bits.
    need: any of {'r','w','x'}.
    """
    if user == "root":
        return
    if user == owner:
        shift = 6
    else:
        shift = 0
    bits = (int(mode) >> shift) & 0b111
    req = 0
    if "r" in need:
        req |= 0b100
    if "w" in need:
        req |= 0b010
    if "x" in need:
        req |= 0b001
    if (bits & req) != req:
        raise RuntimeError("PermissionDenied")


def ensure_root() -> None:
    with DIRS_LOCK:
        if "/" not in DIRS:
            DIRS["/"] = {"owner": "root", "mode": 0o777, "dirs": set(), "files": set()}


def _ensure_parent_dir(path: str, user: Optional[str] = None, need_on_parent: str = "x") -> None:
    ensure_root()
    par = _parent(path)
    with DIRS_LOCK:
        if par not in DIRS:
            raise RuntimeError(f"Parent directory missing: {par}")
        if user is not None:
            d = DIRS[par]
            # To create/delete/rename within parent: need write+exec. To traverse: exec.
            _perm_check(int(d["mode"]), str(d["owner"]), user, need_on_parent)


def _is_dir(path: str) -> bool:
    with DIRS_LOCK:
        return _norm(path) in DIRS


def _is_file(path: str) -> bool:
    with FILES_LOCK:
        return _norm(path) in FILES


def _apply_mkdir(path: str, owner: str, mode: int) -> None:
    path = _norm(path)
    ensure_root()
    par = _parent(path)
    with DIRS_LOCK:
        if path in DIRS:
            return
        if par not in DIRS:
            raise RuntimeError(f"Parent dir missing: {par}")
        DIRS[path] = {"owner": owner, "mode": int(mode), "dirs": set(), "files": set()}
        DIRS[par]["dirs"].add(path)


def _apply_create_file_meta(path: str, owner: str, mode: int) -> None:
    path = _norm(path)
    ensure_root()
    par = _parent(path)
    with DIRS_LOCK:
        if par not in DIRS:
            raise RuntimeError(f"Parent directory missing: {par}")
    with FILES_LOCK:
        if path in FILES:
            return
        FILES[path] = {
            "owner": owner,
            "mode": int(mode),
            "chunks": [],
            "versions": [],
            "checksums": [],
            "sizes": [],
            "total_size": 0,
        }
    with DIRS_LOCK:
        DIRS[par]["files"].add(path)


def _apply_assign_pending(path: str, idx: int, chunk_id: str, version: int, pipeline_node_ids: List[str], ts: float) -> None:
    path = _norm(path)
    key = (path, int(idx))
    with PENDING_LOCK:
        PENDING[key] = {
            "chunk_id": chunk_id,
            "version": int(version),
            "pipeline_node_ids": list(pipeline_node_ids),
            "ts": float(ts),
        }


def _apply_commit(payload: Dict[str, Any], node_ids_for_memory: Optional[List[str]] = None) -> None:
    """
    Important for "volatile location registry":
      - WAL commit payload does NOT persist node_ids
      - But during live operation, we DO populate replicas in memory from node_ids passed in (from client).
      - On restart, replicas are rebuilt by BlockReport disk scans.
    """
    path = _norm(payload["path"])
    idx = int(payload["index"])
    chunk_id = payload["chunk_id"]
    version = int(payload["version"])
    checksum = int(payload["checksum"])
    size = int(payload["size"])

    with FILES_LOCK:
        if path not in FILES:
            raise RuntimeError("Commit for unknown file")
        meta = FILES[path]
        while len(meta["chunks"]) <= idx:
            meta["chunks"].append("")
            meta["versions"].append(1)
            meta["checksums"].append(0)
            meta["sizes"].append(0)

        meta["chunks"][idx] = chunk_id
        meta["versions"][idx] = version
        meta["checksums"][idx] = checksum
        meta["sizes"][idx] = size
        meta["total_size"] = int(sum(meta["sizes"]))

    with CHUNK_LOCK:
        reg = CHUNK_REGISTRY.setdefault(chunk_id, {"version": version, "checksum": 0, "size": 0, "replicas": set()})
        reg["version"] = version
        reg["checksum"] = checksum
        reg["size"] = size
        if node_ids_for_memory:
            for nid in node_ids_for_memory:
                reg["replicas"].add(nid)

    with PENDING_LOCK:
        PENDING.pop((path, idx), None)


def _apply_delete_path(path: str) -> None:
    path = _norm(path)
    if path == "/":
        raise RuntimeError("Refuse delete /")

    # file delete
    with FILES_LOCK:
        fmeta = FILES.get(path)
    if fmeta is not None:
        # remove from dir index
        with DIRS_LOCK:
            par = _parent(path)
            if par in DIRS:
                DIRS[par]["files"].discard(path)

        # best-effort chunk deletions on replicas
        chunks = list(fmeta.get("chunks", []))
        with FILES_LOCK:
            FILES.pop(path, None)

        for cid in chunks:
            if not cid:
                continue
            # remove registry entry after best-effort deletions
            replicas: List[str] = []
            with CHUNK_LOCK:
                reg = CHUNK_REGISTRY.get(cid)
                if reg:
                    replicas = list(reg.get("replicas", []))
            for nid in replicas:
                try:
                    _delete_chunk_on_node(nid, cid)
                except Exception:
                    pass
            with CHUNK_LOCK:
                CHUNK_REGISTRY.pop(cid, None)
        return

    # directory delete (must be empty)
    with DIRS_LOCK:
        if path not in DIRS:
            raise RuntimeError("No such path")
        d = DIRS[path]
        if d["dirs"] or d["files"]:
            raise RuntimeError("DirectoryNotEmpty")
        par = _parent(path)
        if par in DIRS:
            DIRS[par]["dirs"].discard(path)
        DIRS.pop(path, None)


def _apply_rename_path(src: str, dst: str) -> None:
    src = _norm(src)
    dst = _norm(dst)
    if src == "/" or dst == "/":
        raise RuntimeError("Invalid rename")

    # Only support:
    # - file rename
    # - empty directory rename
    if _is_file(src):
        with FILES_LOCK:
            f = FILES.pop(src)
            FILES[dst] = f
        with DIRS_LOCK:
            sp = _parent(src)
            dp = _parent(dst)
            if sp in DIRS:
                DIRS[sp]["files"].discard(src)
            if dp not in DIRS:
                raise RuntimeError("Parent directory missing")
            DIRS[dp]["files"].add(dst)
        return

    if _is_dir(src):
        with DIRS_LOCK:
            d = DIRS[src]
            if d["dirs"] or d["files"]:
                raise RuntimeError("RenameNonEmptyDirNotSupported")
            sp = _parent(src)
            dp = _parent(dst)
            if dp not in DIRS:
                raise RuntimeError("Parent directory missing")
            DIRS.pop(src)
            DIRS[dst] = d
            if sp in DIRS:
                DIRS[sp]["dirs"].discard(src)
            DIRS[dp]["dirs"].add(dst)
        return

    raise RuntimeError("No such path")


def recover_from_wal() -> None:
    ensure_root()
    for op, payload in wal_iter(WAL_PATH):
     try:
        if op == OP_MKDIR:
            _apply_mkdir(payload["path"], payload["owner"], int(payload["mode"]))
        elif op == OP_CREATE_FILE_META:
            _apply_create_file_meta(payload["path"], payload["owner"], int(payload["mode"]))
        elif op == OP_ASSIGN:
            _apply_assign_pending(
                payload["path"],
                int(payload["index"]),
                payload["chunk_id"],
                int(payload["version"]),
                list(payload.get("pipeline_node_ids", [])),
                float(payload.get("ts", time.time())),
            )
        elif op == OP_COMMIT:
            _apply_commit(payload, node_ids_for_memory=None)
        elif op == OP_DELETE_PATH:
            _apply_delete_path(payload["path"])
        elif op == OP_RENAME_PATH:
            _apply_rename_path(payload["src"], payload["dst"])
     except Exception as e:
        # Never crash the master on a bad WAL record; skip and continue.
        print(f"[MASTER] WAL replay: skipping corrupt/old record op={op} payload_keys={list(payload.keys())} err={e}")
        continue


    # ensure replica sets start empty; block reports rebuild them.
    with CHUNK_LOCK:
        for reg in CHUNK_REGISTRY.values():
            reg["replicas"] = set()

    print("[MASTER] WAL replay complete; replica locations will be rebuilt via block reports.")


def _node_snapshot() -> List[Tuple[str, Dict[str, Any]]]:
    with NODES_LOCK:
        return list(ACTIVE_NODES.items())


def _select_pipeline() -> Tuple[List[str], List[pb.Endpoint]]:
    items = _node_snapshot()
    if len(items) < REPLICATION_FACTOR:
        raise RuntimeError("CP write denied: not enough active DataNodes for replication factor")

    # Simple but not pathological: rotate by current time tick
    start = int(time.time()) % len(items)
    rotated = items[start:] + items[:start]
    chosen = rotated[:REPLICATION_FACTOR]

    node_ids = [nid for nid, _ in chosen]
    pipeline = [pb.Endpoint(host=info["host"], port=int(info["data_port"])) for _, info in chosen]
    return node_ids, pipeline


def _node_endpoints(node_id: str) -> Tuple[str, int, int]:
    with NODES_LOCK:
        n = ACTIVE_NODES[node_id]
        return n["host"], int(n["data_port"]), int(n["control_port"])


def _delete_chunk_on_node(node_id: str, chunk_id: str) -> None:
    host, _, ctrl = _node_endpoints(node_id)
    channel = grpc.insecure_channel(f"{host}:{ctrl}")
    stub = pb_grpc.DataNodeControlStub(channel)
    stub.DeleteChunk(pb.DeleteChunkRequest(chunk_id=chunk_id), timeout=8.0)
    with CHUNK_LOCK:
        if chunk_id in CHUNK_REGISTRY:
            CHUNK_REGISTRY[chunk_id]["replicas"].discard(node_id)


def _replicate_one(chunk_id: str) -> None:
    with CHUNK_LOCK:
        reg = CHUNK_REGISTRY.get(chunk_id)
        if not reg:
            return
        replicas: Set[str] = set(reg["replicas"])
        version = int(reg["version"])
        if 0 == len(replicas) or len(replicas) >= REPLICATION_FACTOR:
            return

    items = _node_snapshot()
    active_ids = [nid for nid, _ in items]

    src = next(iter(replicas))
    candidates = [nid for nid in active_ids if nid not in replicas]
    if not candidates:
        return
    tgt = candidates[0]

    shost, _, sctrl = _node_endpoints(src)
    thost, tdata, _ = _node_endpoints(tgt)

    channel = grpc.insecure_channel(f"{shost}:{sctrl}")
    stub = pb_grpc.DataNodeControlStub(channel)
    stub.ReplicateChunk(
        pb.ReplicateChunkRequest(
            chunk_id=chunk_id,
            version=version,
            target=pb.Endpoint(host=thost, port=int(tdata)),
        ),
        timeout=12.0,
    )

    with CHUNK_LOCK:
        CHUNK_REGISTRY[chunk_id]["replicas"].add(tgt)
    print(f"[MASTER] Re-replicated {chunk_id}: {src} -> {tgt}")


def dead_node_monitor_loop() -> None:
    while True:
        time.sleep(1.0)
        now = time.time()
        dead: List[str] = []
        with NODES_LOCK:
            for nid, info in list(ACTIVE_NODES.items()):
                if now - float(info["last_seen"]) > HEARTBEAT_TIMEOUT:
                    dead.append(nid)
            for nid in dead:
                ACTIVE_NODES.pop(nid, None)

        if dead:
            print(f"[MASTER] Dead nodes: {dead}")
            with CHUNK_LOCK:
                for _, reg in CHUNK_REGISTRY.items():
                    for nid in dead:
                        reg["replicas"].discard(nid)


def passive_recovery_loop() -> None:
    while True:
        time.sleep(RECOVERY_INTERVAL)
        with CHUNK_LOCK:
            under = [cid for cid, reg in CHUNK_REGISTRY.items()
                     if 0 < len(reg["replicas"]) < REPLICATION_FACTOR]
        for cid in under:
            try:
                _replicate_one(cid)
            except Exception as e:
                print(f"[MASTER] Replication failed for {cid}: {e}")


def pending_janitor_loop() -> None:
    while True:
        time.sleep(1.0)
        now = time.time()
        expired: List[Tuple[str, int, Dict[str, Any]]] = []
        with PENDING_LOCK:
            for (path, idx), info in list(PENDING.items()):
                if now - float(info["ts"]) > PENDING_TIMEOUT:
                    expired.append((path, idx, info))
                    PENDING.pop((path, idx), None)

        # Best-effort cleanup: delete the assigned chunk from the originally planned pipeline.
        for path, idx, info in expired:
            chunk_id = info["chunk_id"]
            pipeline = list(info.get("pipeline_node_ids", []))
            print(f"[MASTER] Pending write expired for {path} idx={idx} chunk={chunk_id}; cleanup")
            for nid in pipeline:
                try:
                    _delete_chunk_on_node(nid, chunk_id)
                except Exception:
                    pass


class Master(pb_grpc.MasterServiceServicer):
    # -------------------
    # DataNode RPCs
    # -------------------
    def RegisterDataNode(self, request: pb.RegisterRequest, context) -> pb.RegisterResponse:
        peer = context.peer()  # e.g. "ipv4:127.0.0.1:54321" or "ipv6:[::1]:54321"
        host = "127.0.0.1"

        try:
            if peer.startswith("ipv4:"):
                # "ipv4:IP:PORT"
                rest = peer[len("ipv4:"):]
                # split from the right: IP may contain ":" only in ipv6; ipv4 safe
                host = rest.rsplit(":", 1)[0]
            elif peer.startswith("ipv6:"):
                rest = peer[len("ipv6:"):]
                # formats can be "[::1]:PORT" or "::1:PORT"
                if rest.startswith("["):
                    host = rest.split("]")[0].lstrip("[")
                else:
                    host = rest.rsplit(":", 1)[0]
                # Prefer ipv4 localhost for your Windows demo unless you explicitly want ipv6
                if host in ("::1", "0:0:0:0:0:0:0:1"):
                    host = "127.0.0.1"
            else:
                # fallback: try last-resort parse
                if ":" in peer:
                    host = peer.rsplit(":", 1)[0]
        except Exception:
            host = "127.0.0.1"

        node_id = f"dn-{host}-{int(request.data_port)}"
        with NODES_LOCK:
            ACTIVE_NODES[node_id] = {
                "host": host,
                "data_port": int(request.data_port),
                "control_port": int(request.control_port),
                "last_seen": time.time(),
            }
        print(f"[MASTER] Registered {node_id} peer={peer} data={request.data_port} ctrl={request.control_port}")
        return pb.RegisterResponse(node_id=node_id)




    def Heartbeat(self, request: pb.HeartbeatRequest, context) -> pb.Empty:
        with NODES_LOCK:
            if request.node_id in ACTIVE_NODES:
                ACTIVE_NODES[request.node_id]["last_seen"] = time.time()
        return pb.Empty()

    def BlockReport(self, request: pb.BlockReportRequest, context) -> pb.Empty:
        nid = request.node_id

        with CHUNK_LOCK:
            committed_versions = {cid: int(reg["version"]) for cid, reg in CHUNK_REGISTRY.items()}

        with PENDING_LOCK:
            pending_versions = {info["chunk_id"]: int(info["version"]) for info in PENDING.values()}

        for cv in request.chunks:
            cid = cv.chunk_id
            ver = int(cv.version)
            committed_ver = committed_versions.get(cid)
            pending_ver = pending_versions.get(cid)

            # delete anything older than committed
            if committed_ver is not None and ver < committed_ver:
                threading.Thread(target=_delete_chunk_on_node, args=(nid, cid), daemon=True).start()
                continue

            # if a node reports a version higher than known committed/pending -> unknown/stale
            max_known = committed_ver if committed_ver is not None else None
            if pending_ver is not None:
                max_known = pending_ver if max_known is None else max(max_known, pending_ver)
            if max_known is not None and ver > max_known:
                threading.Thread(target=_delete_chunk_on_node, args=(nid, cid), daemon=True).start()
                continue

            # rebuild replica locations for committed chunks
            if committed_ver is not None and ver == committed_ver:
                with CHUNK_LOCK:
                    if cid in CHUNK_REGISTRY:
                        CHUNK_REGISTRY[cid]["replicas"].add(nid)

        return pb.Empty()

    # -------------------
    # Client RPCs
    # -------------------
    def Mkdir(self, request: pb.MkdirRequest, context) -> pb.Empty:
        user = _caller(context)
        path = _norm(request.path)
        _ensure_parent_dir(path, user=user, need_on_parent="wx")

        wal_append(WAL_PATH, OP_MKDIR, {"path": path, "owner": request.owner, "mode": int(request.mode)})
        _apply_mkdir(path, request.owner, int(request.mode))
        return pb.Empty()

    def ListDir(self, request: pb.ListDirRequest, context) -> pb.ListDirResponse:
        user = _caller(context)
        path = _norm(request.path)
        ensure_root()
        with DIRS_LOCK:
            if path not in DIRS:
                raise RuntimeError("No such directory")
            d = DIRS[path]
            _perm_check(int(d["mode"]), str(d["owner"]), user, "rx")
            dirs = sorted([p.split("/")[-1] for p in d["dirs"]])
            files = sorted([p.split("/")[-1] for p in d["files"]])
        return pb.ListDirResponse(dirs=dirs, files=files)

    def Stat(self, request: pb.StatRequest, context) -> pb.StatResponse:
        user = _caller(context)
        path = _norm(request.path)
        ensure_root()
        with DIRS_LOCK:
            if path in DIRS:
                d = DIRS[path]
                _perm_check(int(d["mode"]), str(d["owner"]), user, "x")
                return pb.StatResponse(type="dir", owner=d["owner"], mode=int(d["mode"]), chunks=0, size=0)
        with FILES_LOCK:
            if path in FILES:
                f = FILES[path]
                # for stat, require read or at least execute on parent; we enforce 'x' on parent
                _ensure_parent_dir(path, user=user, need_on_parent="x")
                committed = len([c for c in f["chunks"] if c])
                return pb.StatResponse(type="file", owner=f["owner"], mode=int(f["mode"]), chunks=int(committed), size=int(f["total_size"]))
        raise RuntimeError("No such path")

    def DeletePath(self, request: pb.DeletePathRequest, context) -> pb.Empty:
        user = _caller(context)
        path = _norm(request.path)
        if path == "/":
            raise RuntimeError("Refuse delete /")

        # Need write on parent. Also need write on file/dir itself (basic model).
        _ensure_parent_dir(path, user=user, need_on_parent="wx")

        # Check target permissions
        with DIRS_LOCK:
            if path in DIRS:
                d = DIRS[path]
                _perm_check(int(d["mode"]), str(d["owner"]), user, "w")
        with FILES_LOCK:
            if path in FILES:
                f = FILES[path]
                _perm_check(int(f["mode"]), str(f["owner"]), user, "w")

        wal_append(WAL_PATH, OP_DELETE_PATH, {"path": path})
        _apply_delete_path(path)
        return pb.Empty()

    def RenamePath(self, request: pb.RenamePathRequest, context) -> pb.Empty:
        user = _caller(context)
        src = _norm(request.src)
        dst = _norm(request.dst)

        _ensure_parent_dir(src, user=user, need_on_parent="wx")
        _ensure_parent_dir(dst, user=user, need_on_parent="wx")

        # must have write on source and destination parent; enforce write on source object too
        with DIRS_LOCK:
            if src in DIRS:
                d = DIRS[src]
                _perm_check(int(d["mode"]), str(d["owner"]), user, "w")
        with FILES_LOCK:
            if src in FILES:
                f = FILES[src]
                _perm_check(int(f["mode"]), str(f["owner"]), user, "w")

        wal_append(WAL_PATH, OP_RENAME_PATH, {"src": src, "dst": dst})
        _apply_rename_path(src, dst)
        return pb.Empty()

    def AssignChunk(self, request: pb.AssignChunkRequest, context) -> pb.AssignChunkResponse:
        user = _caller(context)
        path = _norm(request.path)
        idx = int(request.index)
        _ensure_parent_dir(path, user=user, need_on_parent="wx")  # create/overwrite needs write+exec on parent

        pipeline_node_ids, pipeline = _select_pipeline()

        # ensure file meta exists
        with FILES_LOCK:
            if path not in FILES:
                wal_append(WAL_PATH, OP_CREATE_FILE_META, {"path": path, "owner": user, "mode": 0o644})
                _apply_create_file_meta(path, owner=user, mode=0o644)
            meta = FILES[path]
            _perm_check(int(meta["mode"]), str(meta["owner"]), user, "w")

            while len(meta["chunks"]) <= idx:
                meta["chunks"].append("")
                meta["versions"].append(1)
                meta["checksums"].append(0)
                meta["sizes"].append(0)

            existing_chunk_id = meta["chunks"][idx]

            committed_ver = int(meta["versions"][idx]) if existing_chunk_id else 0

        # UUID chunk id allocation (word-for-word chunk files named by UUID)
        if existing_chunk_id:
            chunk_id = existing_chunk_id
        else:
            chunk_id = f"chunk-{uuid.uuid4().hex}"

        # version bump for overwrite; first write uses version 1
        version = 1 if committed_ver == 0 else committed_ver + 1

        ts = time.time()
        wal_append(WAL_PATH, OP_ASSIGN, {
            "path": path,
            "index": idx,
            "chunk_id": chunk_id,
            "version": version,
            "pipeline_node_ids": pipeline_node_ids,
            "ts": ts,
        })
        _apply_assign_pending(path, idx, chunk_id, version, pipeline_node_ids, ts)

        return pb.AssignChunkResponse(chunk_id=chunk_id, version=version, pipeline=pipeline)

    def CommitChunk(self, request: pb.CommitChunkRequest, context) -> pb.Empty:
        user = _caller(context)
        path = _norm(request.path)
        idx = int(request.index)
        key = (path, idx)

        # need write on file
        with FILES_LOCK:
            if path not in FILES:
                raise RuntimeError("No such file")
            f = FILES[path]
            _perm_check(int(f["mode"]), str(f["owner"]), user, "w")

        # validate against pending
        with PENDING_LOCK:
            info = PENDING.get(key)
        if not info:
            raise RuntimeError("Commit rejected: no pending assignment (expired or never assigned)")
        if request.chunk_id != info["chunk_id"] or int(request.version) != int(info["version"]):
            raise RuntimeError("Commit rejected: chunk_id/version mismatch with pending assignment")

        node_ids = list(request.node_ids)
        if len(node_ids) < REPLICATION_FACTOR:
            raise RuntimeError("Commit rejected: replication factor not satisfied")

        payload_for_wal = {
            "path": path,
            "index": idx,
            "chunk_id": request.chunk_id,
            "version": int(request.version),
            "checksum": int(request.checksum),
            "size": int(request.size),
            # node_ids deliberately NOT persisted in WAL (location registry is volatile)
        }

        wal_append(WAL_PATH, OP_COMMIT, payload_for_wal)
        _apply_commit(payload_for_wal, node_ids_for_memory=node_ids)
        return pb.Empty()

def GetFilePlan(self, request: pb.GetFilePlanRequest, context) -> pb.GetFilePlanResponse:
    path = _norm(request.path)

    with FILES_LOCK:
        f = FILES.get(path)
        if not f:
            raise RuntimeError("File not found")
        chunk_ids = list(f["chunks"])

    out: List[pb.FileChunk] = []
    committed_count = 0

    for idx, cid in enumerate(chunk_ids):
        if not cid:
            continue

        # Only publish committed chunks
        with META_LOCK:
            m = CHUNK_META.get(cid)
            if not m or not m.get("committed"):
                continue
            ver = int(m["version"])
            crc = int(m["checksum"])
            size = int(m["size"])
            committed_count += 1

        # Replicas are volatile; may be empty temporarily (e.g., after restart / node churn)
        with LOCS_LOCK:
            node_ids = list(CHUNK_LOCS.get(cid, []))

        replicas: List[pb.Endpoint] = []
        with NODES_LOCK:
            for nid in node_ids:
                info = ACTIVE_NODES.get(nid)
                if info:
                    replicas.append(pb.Endpoint(host=info["host"], port=int(info["data_port"])))

        out.append(pb.FileChunk(
            index=int(idx),
            chunk_id=cid,
            version=ver,
            checksum=crc,
            size=size,
            replicas=replicas,  # may be empty; client will wait/retry
        ))

    # If the file has committed chunks, we must return them (even if replicas currently empty).
    # If there are no committed chunks at all, that's an actual error.
    if committed_count == 0:
        raise RuntimeError("No committed chunks available")

    return pb.GetFilePlanResponse(chunks=out)



def _try_acquire_lock():
    """
    Cross-platform best-effort leader election lock.
    On POSIX uses fcntl.flock; on Windows uses msvcrt.locking.
    """
    ensure_dir(os.path.dirname(LOCK_PATH) or ".")
    f = open(LOCK_PATH, "a+b")
    try:
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                f.close()
                return None
        else:
            import fcntl
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                f.close()
                return None
        return f
    except Exception:
        try:
            f.close()
        except Exception:
            pass
        return None


def serve_as_leader() -> None:
    recover_from_wal()
    threading.Thread(target=dead_node_monitor_loop, daemon=True).start()
    threading.Thread(target=passive_recovery_loop, daemon=True).start()
    threading.Thread(target=pending_janitor_loop, daemon=True).start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
    pb_grpc.add_MasterServiceServicer_to_server(Master(), server)
    server.add_insecure_port(f"{MASTER_HOST}:{MASTER_PORT}")
    server.start()
    print(f"[MASTER] LEADER listening on {MASTER_HOST}:{MASTER_PORT} RF={REPLICATION_FACTOR}")
    server.wait_for_termination()


def main() -> None:
    # Standby loop: only one master holds the lock and serves => prevents split brain.
    while True:
        lock = _try_acquire_lock()
        if lock is None:
            print("[MASTER] STANDBY: waiting for leader lock...")
            time.sleep(1.0)
            continue

        try:
            print("[MASTER] Acquired leader lock; starting service.")
            serve_as_leader()
        except Exception as e:
            print(f"[MASTER] Leader crashed/exited: {e}")
            time.sleep(0.5)
        finally:
            try:
                lock.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
