# datanode.py - DistriFS DataNode (chunk server)
from __future__ import annotations

import os
import time
import threading
import socket
import zlib
import pickle
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import grpc
from concurrent import futures

from utils import ensure_proto_generated, send_msg, recv_msg, recv_exact, atomic_write, storage_root

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
HOST = os.getenv("DISTRIFS_DATANODE_HOST", "127.0.0.1")
MASTER_HOST = os.getenv("DISTRIFS_MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.getenv("DISTRIFS_MASTER_PORT", "9000"))

DATA_PORT = int(os.getenv("DISTRIFS_DATANODE_DATA_PORT", "9101"))
CTRL_PORT = int(os.getenv("DISTRIFS_DATANODE_CTRL_PORT", "9201"))

DATA_DIR = os.getenv("DISTRIFS_STORAGE_DIR")
if not DATA_DIR:
    DATA_DIR = os.path.join(storage_root(), f"node-{DATA_PORT}")

HB_INTERVAL = float(os.getenv("DISTRIFS_HB_INTERVAL_S", "3.0"))
BR_INTERVAL = float(os.getenv("DISTRIFS_BLOCKREPORT_INTERVAL_S", "30.0"))

# -------------------------
# Local state
# -------------------------
NODE_ID = f"node-{DATA_PORT}"
EPOCH_SEEN = 0
EPOCH_LOCK = threading.Lock()
STOP = threading.Event()

def _chunk_path(chunk_id: str) -> str:
    return os.path.join(DATA_DIR, chunk_id)

def _meta_path(chunk_id: str) -> str:
    return os.path.join(DATA_DIR, chunk_id + ".meta")

def _load_meta(chunk_id: str) -> Optional[Dict]:
    try:
        with open(_meta_path(chunk_id), "rb") as f:
            return pickle.loads(f.read())
    except Exception:
        return None

def _save_meta(chunk_id: str, meta: Dict) -> None:
    atomic_write(_meta_path(chunk_id), pickle.dumps(meta, protocol=pickle.HIGHEST_PROTOCOL))

def _verify_fence(md) -> bool:
    global EPOCH_SEEN
    try:
        epoch = int(dict(md).get("x-epoch", "0"))
    except Exception:
        epoch = 0
    with EPOCH_LOCK:
        if epoch < EPOCH_SEEN:
            return False
        if epoch > EPOCH_SEEN:
            EPOCH_SEEN = epoch
    return True

# -------------------------
# Data-plane server
# -------------------------
def _handle_conn(conn: socket.socket, addr) -> None:
    try:
        hdr = recv_msg(conn)
        op = hdr.get("op")
        if op == "PUT" or op == "REPL":
            _handle_put(conn, hdr)
        elif op == "GET":
            _handle_get(conn, hdr)
        else:
            send_msg(conn, {"ok": False, "err": f"unknown op {op}"})
    except Exception as e:
        try:
            send_msg(conn, {"ok": False, "err": str(e)})
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _stream_to_next(forward_to: List[Tuple[str,int]], hdr: Dict) -> Optional[socket.socket]:
    if not forward_to:
        return None
    nh, np = forward_to[0]
    rest = forward_to[1:]
    s = socket.create_connection((nh, np), timeout=6.0)
    h2 = dict(hdr)
    h2["forward_to"] = rest
    send_msg(s, h2)
    return s

def _handle_put(conn: socket.socket, hdr: Dict) -> None:
    chunk_id = hdr["chunk_id"]
    version = int(hdr.get("version", 1))
    size = int(hdr["size"])
    forward_to = hdr.get("forward_to") or []
    # forward_to elements may be pb HostPorts (converted) or tuples
    fwd: List[Tuple[str,int]] = []
    for x in forward_to:
        if isinstance(x, tuple):
            fwd.append((x[0], int(x[1])))
        elif isinstance(x, dict):
            fwd.append((x["host"], int(x["port"])))
        else:
            # assume (host,port) list-like
            fwd.append((x[0], int(x[1])))

    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = _chunk_path(chunk_id) + ".tmp"
    out = open(tmp_path, "wb")

    next_sock = None
    try:
        next_sock = _stream_to_next(fwd, hdr) if fwd else None
    except Exception:
        next_sock = None

    crc = 0
    remaining = size
    try:
        while remaining > 0:
            n = 64 * 1024 if remaining > 64 * 1024 else remaining
            data = recv_exact(conn, n)
            out.write(data)
            crc = zlib.crc32(data, crc)
            remaining -= n
            if next_sock is not None:
                next_sock.sendall(data)
        out.flush()
        os.fsync(out.fileno())
        out.close()

        # wait downstream ack first (pipeline acks)
        if next_sock is not None:
            ack = recv_msg(next_sock)
            try:
                next_sock.close()
            except Exception:
                pass
            if not ack.get("ok", False):
                send_msg(conn, {"ok": False, "err": ack.get("err", "downstream failed")})
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                return

        # finalize locally
        final_path = _chunk_path(chunk_id)
        os.replace(tmp_path, final_path)
        _save_meta(chunk_id, {"version": version, "size": size, "checksum": int(crc) & 0xFFFFFFFF})
        send_msg(conn, {"ok": True, "checksum": int(crc) & 0xFFFFFFFF, "version": version})
    except Exception as e:
        try:
            out.close()
        except Exception:
            pass
        try:
            if next_sock is not None:
                next_sock.close()
        except Exception:
            pass
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        send_msg(conn, {"ok": False, "err": str(e)})

def _handle_get(conn: socket.socket, hdr: Dict) -> None:
    chunk_id = hdr["chunk_id"]
    want_version = int(hdr.get("version", 0))
    meta = _load_meta(chunk_id)
    if not meta:
        send_msg(conn, {"ok": False, "err": "NoSuchChunk"})
        return
    if want_version and int(meta.get("version", 0)) != want_version:
        send_msg(conn, {"ok": False, "err": "StaleChunk"})
        return
    path = _chunk_path(chunk_id)
    try:
        with open(path, "rb") as f:
            # verify checksum
            crc = 0
            while True:
                data = f.read(256 * 1024)
                if not data:
                    break
                crc = zlib.crc32(data, crc)
            if (int(crc) & 0xFFFFFFFF) != int(meta.get("checksum", 0)):
                send_msg(conn, {"ok": False, "err": "DataCorrupt"})
                return

        # stream again (avoid holding all in memory)
        size = int(meta.get("size", 0))
        send_msg(conn, {"ok": True, "size": size})
        with open(path, "rb") as f:
            while True:
                data = f.read(256 * 1024)
                if not data:
                    break
                conn.sendall(data)
    except Exception as e:
        send_msg(conn, {"ok": False, "err": str(e)})

def _data_server() -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, DATA_PORT))
    s.listen(64)
    print(f"[DATANODE] data-plane listening on {HOST}:{DATA_PORT} dir={DATA_DIR}", flush=True)
    s.settimeout(0.5)
    while not STOP.is_set():
        try:
            conn, addr = s.accept()
        except socket.timeout:
            continue
        threading.Thread(target=_handle_conn, args=(conn, addr), daemon=True).start()
    try:
        s.close()
    except Exception:
        pass

# -------------------------
# Control-plane service
# -------------------------
class Control(pb_grpc.DataNodeControlServicer):
    def CopyChunk(self, request, context):
        if not _verify_fence(context.invocation_metadata()):
            return pb.ControlStatus(ok=False, message="fenced")
        chunk_id = request.chunk_id
        want_version = int(request.version)
        meta = _load_meta(chunk_id)
        if not meta:
            return pb.ControlStatus(ok=False, message="no such chunk")
        if int(meta.get("version", 0)) != want_version:
            return pb.ControlStatus(ok=False, message="stale version")
        target = request.target_data
        # connect and send replication PUT (op REPL)
        try:
            path = _chunk_path(chunk_id)
            size = int(meta.get("size", 0))
            hdr = {"op": "REPL", "chunk_id": chunk_id, "version": want_version, "size": size, "forward_to": []}
            sock = socket.create_connection((target.host, int(target.port)), timeout=6.0)
            send_msg(sock, hdr)
            with open(path, "rb") as f:
                while True:
                    data = f.read(256 * 1024)
                    if not data:
                        break
                    sock.sendall(data)
            ack = recv_msg(sock)
            sock.close()
            if not ack.get("ok", False):
                return pb.ControlStatus(ok=False, message="target failed")
            return pb.ControlStatus(ok=True, message="ok")
        except Exception as e:
            return pb.ControlStatus(ok=False, message=str(e))

    def DeleteChunk(self, request, context):
        if not _verify_fence(context.invocation_metadata()):
            return pb.ControlStatus(ok=False, message="fenced")
        chunk_id = request.chunk_id
        try:
            p = _chunk_path(chunk_id)
            m = _meta_path(chunk_id)
            if os.path.exists(p):
                os.remove(p)
            if os.path.exists(m):
                os.remove(m)
            return pb.ControlStatus(ok=True, message="ok")
        except Exception as e:
            return pb.ControlStatus(ok=False, message=str(e))

def _ctrl_server() -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_DataNodeControlServicer_to_server(Control(), server)
    server.add_insecure_port(f"{HOST}:{CTRL_PORT}")
    server.start()
    print(f"[DATANODE] control-plane listening on {HOST}:{CTRL_PORT}", flush=True)
    return server

# -------------------------
# Master registration + heartbeats + block reports
# -------------------------
def _master_stub():
    ch = grpc.insecure_channel(f"{MASTER_HOST}:{MASTER_PORT}")
    return pb_grpc.MasterServiceStub(ch)

def _register() -> int:
    global EPOCH_SEEN
    stub = _master_stub()
    r = stub.RegisterDataNode(pb.RegisterDataNodeRequest(host=HOST, data_port=DATA_PORT, control_port=CTRL_PORT), timeout=6.0)
    if not r.status.ok:
        raise RuntimeError(r.status.error.message if r.status.error else "register failed")
    with EPOCH_LOCK:
        EPOCH_SEEN = int(r.data.epoch)
    return EPOCH_SEEN

def _hb_loop():
    while not STOP.is_set():
        time.sleep(HB_INTERVAL)
        try:
            stub = _master_stub()
            with EPOCH_LOCK:
                epoch = EPOCH_SEEN
            r = stub.Heartbeat(pb.HeartbeatRequest(node_id=NODE_ID, epoch=epoch), timeout=4.0)
            if not r.status.ok:
                # stale epoch -> re-register
                _register()
        except Exception:
            # try re-register
            try:
                _register()
            except Exception:
                pass

def _scan_chunks() -> Dict[str, int]:
    os.makedirs(DATA_DIR, exist_ok=True)
    out: Dict[str, int] = {}
    for name in os.listdir(DATA_DIR):
        if not name.startswith("chunk-"):
            continue
        if name.endswith(".meta") or name.endswith(".tmp"):
            continue
        meta = _load_meta(name)
        if not meta:
            continue
        out[name] = int(meta.get("version", 0))
    return out

def _br_loop():
    while not STOP.is_set():
        time.sleep(BR_INTERVAL)
        try:
            stub = _master_stub()
            with EPOCH_LOCK:
                epoch = EPOCH_SEEN
            chunks = _scan_chunks()
            r = stub.BlockReport(pb.BlockReportRequest(node_id=NODE_ID, epoch=epoch, chunks=chunks), timeout=8.0)
            if not r.status.ok:
                _register()
        except Exception:
            pass

# -------------------------
# Entry
# -------------------------
def serve():
    # args override ports
    import sys
    global DATA_PORT, CTRL_PORT, NODE_ID, DATA_DIR
    if len(sys.argv) >= 2:
        DATA_PORT = int(sys.argv[1])
    if len(sys.argv) >= 3:
        CTRL_PORT = int(sys.argv[2])
    NODE_ID = f"node-{DATA_PORT}"
    DATA_DIR = os.getenv("DISTRIFS_STORAGE_DIR") or os.path.join(storage_root(), f"node-{DATA_PORT}")
    os.makedirs(DATA_DIR, exist_ok=True)

    # start servers
    ctrl = _ctrl_server()
    t = threading.Thread(target=_data_server, daemon=True)
    t.start()

    # register
    _register()
    threading.Thread(target=_hb_loop, daemon=True).start()
    threading.Thread(target=_br_loop, daemon=True).start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        STOP.set()
        ctrl.stop(0)

if __name__ == "__main__":
    serve()
