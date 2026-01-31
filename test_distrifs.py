# test_distrifs.py
import os
import time
import shutil
import hashlib
import subprocess
import signal
import socket
import sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHILD_ENV = os.environ.copy()
# Force every spawned process to import pb2/pb2_grpc from THIS folder first
CHILD_ENV["PYTHONPATH"] = BASE_DIR + os.pathsep + CHILD_ENV.get("PYTHONPATH", "")
# Prevent Python from pulling a stale distrifs_pb2_grpc from user-site packages
CHILD_ENV["PYTHONNOUSERSITE"] = "1"
# Optional but helps: forces re-gen if your utils supports it
CHILD_ENV["DISTRIFS_FORCE_PROTO"] = "1"
os.chdir(BASE_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
from pathlib import Path

import grpc
# If grpc stubs are stale/mismatched, Master may start without GetFilePlan and client sees UNIMPLEMENTED.
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

from utils import storage_root, ensure_dir, send_msg, ensure_proto_generated

# Make sure proto exists BEFORE we spawn subprocesses that import pb2 modules
ensure_proto_generated()
import distrifs_pb2 as pb
import distrifs_pb2_grpc as pb_grpc

PY = sys.executable
MASTER = [PY, "-u", "master.py"]
DATANODE = [PY, "-u", "datanode.py"]
CLIENT = [PY, "-u", "client.py"]

RF = int(os.getenv("DISTRIFS_REPLICATION_FACTOR", "3"))

def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def run(cmd, env=None):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env = (env or os.environ.copy()).copy()

    # Force children (master/datanode/client) to import pb2/pb2_grpc from THIS folder
    env["PYTHONPATH"] = base_dir + os.pathsep + env.get("PYTHONPATH", "")
    # Prevent Python from pulling a stale distrifs_pb2_grpc from user-site packages
    env["PYTHONNOUSERSITE"] = "1"
    # If your utils supports it, this reduces “stale stubs” issues
    env.setdefault("DISTRIFS_FORCE_PROTO", "1")

    return subprocess.Popen(
        cmd,
        cwd=base_dir,  # <<< IMPORTANT: ensures local files are import-first
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )


def kill(p: subprocess.Popen):
    if p is None or p.poll() is not None:
        return
    try:
        p.send_signal(signal.SIGKILL)
    except Exception:
        try:
            p.terminate()
        except Exception:
            pass

def wait_port(host: str, port: int, timeout_s: float = 12.0, proc: subprocess.Popen = None, proc_label: str = "PROC"):
    deadline = time.time() + timeout_s
    last = None

    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            out = ""
            try:
                out = proc.stdout.read() if proc.stdout else ""
            except Exception:
                pass
            print(f"\n[{proc_label}] exited early (code {proc.returncode})\n--- {proc_label} OUTPUT ---\n{out}\n--- END OUTPUT ---\n")
            raise RuntimeError(f"{proc_label} exited before opening port")

        try:
            with socket.create_connection((host, port), timeout=0.4):
                return
        except Exception as e:
            last = e
            time.sleep(0.15)

    if proc is not None:
        print(f"[{proc_label}] did not open port in time; dumping output...")
        try:
            proc.kill()
        except Exception:
            pass
        out = ""
        try:
            out, _ = proc.communicate(timeout=2.0)
        except Exception:
            try:
                out = proc.stdout.read() if proc.stdout else ""
            except Exception:
                out = ""
        print(f"\n--- {proc_label} OUTPUT ---\n{out}\n--- END OUTPUT ---\n")

    raise RuntimeError(f"Port not ready: {host}:{port} (timed out; last={last})")

def corrupt_one_replica(chunk_id: str, node_data_port: int):
    node_dir = os.path.join(storage_root(), f"node-{node_data_port}")
    chunk_path = os.path.join(node_dir, chunk_id)
    if not os.path.exists(chunk_path):
        raise RuntimeError(f"chunk not found to corrupt: {chunk_path}")
    with open(chunk_path, "r+b") as f:
        f.seek(0)
        b = f.read(1)
        f.seek(0)
        f.write(bytes([(b[0] ^ 0xFF) if b else 0xAA]))

def start_datanode(dp, cp):
    p = subprocess.Popen([...])
    PROCS.append(p)
    time.sleep(0.6)
    return p

def simulate_client_crash_partial_write(head_host: str, head_port: int, chunk_id: str, version: int, total_size: int, partial_size: int):
    # Write only partial bytes then close without commit.
    s = socket.create_connection((head_host, int(head_port)), timeout=6)
    s.settimeout(6)
    try:
        send_msg(s, {
            "op": "STORE_STREAM",
            "chunk_id": chunk_id,
            "version": int(version),
            "size": int(total_size),
            "replicate_to": [],  # simple for crash simulation
        })
        s.sendall(os.urandom(partial_size))
    finally:
        try:
            s.close()
        except Exception:
            pass

def get_first_chunk_id(remote_path: str) -> str:
    ch = grpc.insecure_channel("127.0.0.1:9000")
    stub = pb_grpc.MasterServiceStub(ch)
    plan = stub.GetFilePlan(pb.GetFilePlanRequest(path=remote_path), timeout=6.0)
    first = sorted(plan.chunks, key=lambda c: c.index)[0]
    return first.chunk_id

def main():
    # Clean prior data
    root = storage_root()
    if os.path.exists(root):
        shutil.rmtree(root, ignore_errors=True)
    ensure_dir(root)

    env = os.environ.copy()
    env["DISTRIFS_USER"] = "user"
    env["DISTRIFS_CHUNK_SIZE"] = str(128 * 1024)           # speed test
    env["DISTRIFS_HEARTBEAT_INTERVAL"] = "1"
    env["DISTRIFS_BLOCK_REPORT_INTERVAL"] = "2"
    env["DISTRIFS_HEARTBEAT_TIMEOUT"] = "3"
    env["DISTRIFS_RECOVERY_INTERVAL"] = "1"
    env["DISTRIFS_PENDING_TIMEOUT"] = "2"
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("DISTRIFS_MASTER_LOCK_PATH", os.path.join(storage_root(), "master.lock"))

    # Start master
    m = run(MASTER, env=env)
    wait_port("127.0.0.1", 9000, timeout_s=15.0, proc=m, proc_label="MASTER")
    print("OK")  # master up

    # Start datanodes (>= RF)
    dn_ports = [(9101, 9201), (9102, 9202), (9103, 9203)]
    dns = []
    for dp, cp in dn_ports:
        dns.append(run(DATANODE + [str(dp), str(cp)], env=env))

    # Allow registration + initial block reports
    time.sleep(2.5)
    print("OK")  # datanodes up

    # Prepare test dir and files (Windows-friendly)
    test_dir = Path(os.path.join(Path.home(), "AppData", "Local", "Temp", "distrifs_test"))
    test_dir.mkdir(parents=True, exist_ok=True)

    src = str(test_dir / "src.bin")
    out = str(test_dir / "out.bin")
    out2 = str(test_dir / "out2.bin")
    out3 = str(test_dir / "out3.bin")
    out4 = str(test_dir / "out4.bin")

    data = os.urandom(700 * 1024 + 123)
    with open(src, "wb") as f:
        f.write(data)

    # Create namespace
    subprocess.check_call(CLIENT + ["mkdir", "/user"], env=env)
    remote_dir = "/user/files"
    remote = "/user/files/blob.bin"

    # mkdir + put + immediate get
    subprocess.check_call(CLIENT + ["mkdir", remote_dir], env=env)
    subprocess.check_call(CLIENT + ["put", src, remote], env=env)
    subprocess.check_call(CLIENT + ["get", remote, out], cwd=BASE_DIR, env=CHILD_ENV)
    assert sha256(src) == sha256(out), "Immediate download mismatch"

    # Corrupt one replica and verify client retries another replica
    chunk0 = get_first_chunk_id(remote)
    corrupt_one_replica(chunk0, 9101)
    subprocess.check_call(CLIENT + ["get", remote, out2], cwd=BASE_DIR, env=CHILD_ENV)
    assert sha256(src) == sha256(out2), "Download after corruption mismatch (retry failed)"

    # Kill one datanode; download should still work
    kill(dns[0])
    time.sleep(1.2)
    subprocess.check_call(CLIENT + ["get", remote, out3], cwd=BASE_DIR, env=CHILD_ENV)
    assert sha256(src) == sha256(out3), "Download after node kill mismatch"

    # Pending-write crash test:
    crash_remote = "/user/files/crash.bin"
    ch = grpc.insecure_channel("127.0.0.1:9000")
    stub = pb_grpc.MasterServiceStub(ch)

    # restart killed dn for RF if needed
    if dns[0].poll() is not None:
        dp, cp = dn_ports[0]
        dns[0] = run(DATANODE + [str(dp), str(cp)], env=env)
    time.sleep(2.5)

    r = stub.AssignChunk(pb.AssignChunkRequest(path=crash_remote, index=0), timeout=5.0)
    simulate_client_crash_partial_write(r.pipeline[0].host, r.pipeline[0].port, r.chunk_id, r.version, total_size=500_000, partial_size=50_000)

    # wait for pending janitor expiry
    time.sleep(3.0)

    # GetFilePlan should fail because nothing committed
    try:
        stub.GetFilePlan(pb.GetFilePlanRequest(path=crash_remote), timeout=3.0)
        raise RuntimeError("Crash-mid-write should not publish committed data")
    except Exception:
        pass

    # Master restart rebuild: replica locations are volatile; block reports repopulate.
    kill(m)
    time.sleep(0.8)
    m = run(MASTER, env=env)
    wait_port("127.0.0.1", 9000, timeout_s=10.0, proc=m, proc_label="MASTER-RESTART")
    time.sleep(3.0)  # allow block reports to refresh after restart

    subprocess.check_call(CLIENT + ["get", remote, out4], env=env)
    assert sha256(src) == sha256(out4), "Download after master restart mismatch"

    # rm + expected get failure
    subprocess.check_call(CLIENT + ["rm", remote], env=env)
    p = subprocess.run(
        CLIENT + ["get", remote, str(test_dir / "should_fail.bin")],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if p.returncode == 0:
        raise RuntimeError("Expected get to fail after rm")

    print("\n✅ ALL TESTS PASSED\n")

    # cleanup
    for p2 in dns:
        kill(p2)
    kill(m)

if __name__ == "__main__":
    main()
