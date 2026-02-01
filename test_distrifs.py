# test_distrifs.py - end-to-end smoke test for DistriFS
from __future__ import annotations

import os
import sys
import time
import shutil
import socket
import hashlib
import tempfile
import subprocess
from pathlib import Path
from utils import ensure_proto_generated, send_msg, storage_root

BASE_DIR = Path(__file__).resolve().parent

PY = sys.executable

def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

def _wait_port(host: str, port: int, timeout: float = 12.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.3):
                return
        except Exception as e:
            last = e
            time.sleep(0.1)
    raise RuntimeError(f"port not ready: {host}:{port} last={last}")

def _wait_port_down(host: str, port: int, timeout: float = 10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                # still up
                time.sleep(0.10)
                continue
        except OSError:
            return
    raise RuntimeError(f"port still open: {host}:{port}")

def wait_port_closed(host: str, port: int, timeout_s: float = 12.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.3):
                # Still open
                time.sleep(0.15)
                continue
        except Exception:
            # Connection refused / timed out => port is effectively down
            return
    raise RuntimeError(f"Port still open: {host}:{port} (timed out waiting for close)")

def sha256_file(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

def _start_master(env):
    p = subprocess.Popen([PY, "-u", "master.py"], cwd=BASE_DIR, env=env)
    _wait_port(env.get("DISTRIFS_MASTER_HOST","127.0.0.1"), int(env.get("DISTRIFS_MASTER_PORT","9000")), timeout=15.0)
    return p

def _start_datanode(env, data_port: int, ctrl_port: int, storage_dir: Path):
    e = dict(env)
    e["DISTRIFS_DATANODE_DATA_PORT"] = str(data_port)
    e["DISTRIFS_DATANODE_CTRL_PORT"] = str(ctrl_port)
    e["DISTRIFS_STORAGE_DIR"] = str(storage_dir)
    p = subprocess.Popen([PY, "-u", "datanode.py", str(data_port), str(ctrl_port)], cwd=BASE_DIR, env=e)
    _wait_port(e.get("DISTRIFS_DATANODE_HOST","127.0.0.1"), data_port, timeout=15.0)
    return p

def _run_client(args, env, user="user"):
    e = dict(env)
    e["DISTRIFS_USER"] = user
    subprocess.check_call([PY, "-u", "client.py"] + args, cwd=BASE_DIR, env=e)

def kill(p: subprocess.Popen):
    if p is None or p.poll() is not None:
        return
    try:
        # Try graceful first
        p.terminate()
        p.wait(timeout=2.0)
        return
    except Exception:
        pass
    try:
        # Force kill (works on Windows)
        p.kill()
        p.wait(timeout=5.0)
    except Exception:
        pass

def main():
    import os
    import time
    import socket
    import shutil
    import tempfile
    import subprocess
    from pathlib import Path

    # --- helpers (local, so you don't need to hunt for missing defs) ---
    def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    def _wait_port_down(host: str, port: int, timeout: float = 15.0) -> None:
        t0 = time.time()
        last = None
        while time.time() - t0 < timeout:
            if not _port_open(host, port, timeout=0.25):
                return
            time.sleep(0.15)
        raise RuntimeError(f"port still open (didn't go down): {host}:{port} last={last}")

    def _wait_until(predicate, timeout_s: float, interval_s: float = 0.25, label: str = "condition"):
        t0 = time.time()
        while time.time() - t0 < timeout_s:
            if predicate():
                return True
            time.sleep(interval_s)
        return False

    # --- test body ---
    tmp = Path(tempfile.mkdtemp(prefix="distrifs_test_"))
    root = tmp / "root"
    data = root / "data"
    data.mkdir(parents=True, exist_ok=True)

    # Make sure BASE_DIR / PY / sha256_file / _start_master / _start_datanode / _run_client / _wait_port
    # exist in your file (they already do in your current version).
    #
    # This main() assumes:
    # - BASE_DIR is Path(...)
    # - PY is path to venv python
    # - sha256_file is imported from utils
    # - _start_master(env) returns a Popen
    # - _start_datanode(env, dp, cp, dir) returns a Popen
    # - _run_client(args, env, user="...") runs client.py and raises CalledProcessError on failure
    # - _wait_port(host, port, timeout=...) exists

    # clean leader lock + wal for a deterministic run
    for f in ["master.leader.lock", "master.wal", "master.epoch"]:
        try:
            (BASE_DIR / f).unlink()
        except Exception:
            pass

    env = os.environ.copy()
    env["PYTHONNOUSERSITE"] = "1"
    env["DISTRIFS_PROTO_PATH"] = str(BASE_DIR / "distrifs.proto")
    env["DISTRIFS_FORCE_PROTO"] = "1"
    env["DISTRIFS_MASTER_HOST"] = "127.0.0.1"
    env["DISTRIFS_MASTER_PORT"] = "9000"
    env["DISTRIFS_RF"] = "3"
    env["DISTRIFS_CHUNK_SIZE"] = str(128 * 1024)
    env["DISTRIFS_GC_INTERVAL_S"] = "2.0"
    env["DISTRIFS_REPAIR_INTERVAL_S"] = "1.0"
    env["DISTRIFS_BLOCKREPORT_INTERVAL_S"] = "2.0"
    env["DISTRIFS_IGNORE_CTRL_C"] = "1"

    m1 = None
    m2 = None
    dns = []

    try:
        # --- start master + datanodes ---
        m1 = _start_master(env)

        ports = [(9101, 9201), (9102, 9202), (9103, 9203)]
        for dp, cp in ports:
            dns.append(_start_datanode(env, dp, cp, data / f"node-{dp}"))

        # --- namespace + io ---
        _run_client(["mkdir", "/user"], env)
        _run_client(["mkdir", "/user/files"], env)

        src = tmp / "src.bin"
        out = tmp / "out.bin"
        out2 = tmp / "out2.bin"
        out3 = tmp / "out3.bin"
        src.write_bytes(os.urandom(700_000))

        remote = "/user/files/blob.bin"
        _run_client(["put", str(src), remote], env)
        _run_client(["get", remote, str(out)], env)
        assert sha256_file(src) == sha256_file(out), "download mismatch"

        # --- corrupt exactly one replica and ensure read heals by switching ---
        node1 = data / "node-9101"
        chunk_files = sorted([
            p for p in node1.glob("chunk-*")
            if p.is_file() and (not p.name.endswith(".meta")) and (not p.name.endswith(".tmp"))
        ])
        assert chunk_files, "no chunk files found to corrupt"
        corrupt = chunk_files[0]
        b = bytearray(corrupt.read_bytes())
        b[0] ^= 0xFF
        corrupt.write_bytes(bytes(b))

        _run_client(["get", remote, str(out2)], env)
        assert sha256_file(src) == sha256_file(out2), "download mismatch after corruption"

        # --- range read ---
        _run_client(["getrange", remote, "1000", "20000", str(out3)], env)
        assert out3.read_bytes() == src.read_bytes()[1000:1000 + 20000], "range mismatch"

        # --- permission checks: alice creates /secret, bob cannot list ---
        _run_client(["mkdir", "/secret"], env, user="alice")
        try:
            _run_client(["ls", "/secret"], env, user="bob")
            raise AssertionError("expected PermissionDenied")
        except subprocess.CalledProcessError:
            pass

        # --- master failover simulation (IMPORTANT: stop m1 BEFORE starting m2) ---
        # Stop m1
        if m1 and m1.poll() is None:
            m1.terminate()
            try:
                m1.wait(timeout=10.0)
            except Exception:
                m1.kill()
                m1.wait(timeout=5.0)

        # Ensure port is actually free before starting m2
        _wait_port_down(env.get("DISTRIFS_MASTER_HOST", "127.0.0.1"), int(env.get("DISTRIFS_MASTER_PORT", "9000")), timeout=15.0)

        # Start m2 (should acquire lock + bind cleanly)
        m2 = subprocess.Popen([PY, "-u", "master.py"], cwd=BASE_DIR, env=env)
        _wait_port(env.get("DISTRIFS_MASTER_HOST", "127.0.0.1"), int(env.get("DISTRIFS_MASTER_PORT", "9000")), timeout=20.0)

        # basic op after failover
        _run_client(["stat", remote], env)

        # --- GC: rm and ensure chunks disappear (poll instead of fixed sleep) ---
        _run_client(["rm", remote], env)

        def _all_chunks_gone() -> bool:
            for dp, _ in ports:
                node_dir = data / f"node-{dp}"
                leftovers = [
                    p for p in node_dir.glob("chunk-*")
                    if p.is_file() and (not p.name.endswith(".meta")) and (not p.name.endswith(".tmp"))
                ]
                if leftovers:
                    return False
            return True

        ok = _wait_until(_all_chunks_gone, timeout_s=25.0, interval_s=0.5, label="gc sweep")
        if not ok:
            # show a small sample of leftovers for debugging
            samples = {}
            for dp, _ in ports:
                node_dir = data / f"node-{dp}"
                leftovers = [
                    p for p in node_dir.glob("chunk-*")
                    if p.is_file() and (not p.name.endswith(".meta")) and (not p.name.endswith(".tmp"))
                ]
                if leftovers:
                    samples[str(node_dir)] = [str(x) for x in leftovers[:5]]
            raise AssertionError(f"GC left chunks (after waiting): {samples}")

        print("OK", flush=True)

    finally:
        # cleanup processes
        for p in dns:
            try:
                p.terminate()
            except Exception:
                pass
        for p in dns:
            try:
                p.wait(timeout=5.0)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass

        for mp in [m2, m1]:
            if mp is None:
                continue
            try:
                mp.terminate()
            except Exception:
                pass
            try:
                mp.wait(timeout=5.0)
            except Exception:
                try:
                    mp.kill()
                except Exception:
                    pass

        shutil.rmtree(tmp, ignore_errors=True)



if __name__ == "__main__":
    main()

