# chaos_monkey.py - chaos test for DistriFS (Windows-friendly)
from __future__ import annotations

import os
import sys
import time
import shutil
import socket
import random
import hashlib
import tempfile
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PY = sys.executable


def _wait_port(host: str, port: int, timeout: float = 15.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.35):
                return
        except Exception as e:
            last = e
            time.sleep(0.12)
    raise RuntimeError(f"port not ready: {host}:{port} last={last}")


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def _run_client(args: list[str], env: dict, user: str = "user") -> None:
    e = dict(env)
    e["DISTRIFS_USER"] = user
    subprocess.check_call([PY, "-u", "client.py"] + args, cwd=BASE_DIR, env=e)


def _start_master(env: dict) -> subprocess.Popen:
    p = subprocess.Popen([PY, "-u", "master.py"], cwd=BASE_DIR, env=env)
    _wait_port(env.get("DISTRIFS_MASTER_HOST", "127.0.0.1"),
               int(env.get("DISTRIFS_MASTER_PORT", "9000")),
               timeout=20.0)
    return p


def _start_datanode(env: dict, data_port: int, ctrl_port: int, storage_dir: Path) -> subprocess.Popen:
    e = dict(env)
    e["DISTRIFS_DATANODE_DATA_PORT"] = str(data_port)
    e["DISTRIFS_DATANODE_CTRL_PORT"] = str(ctrl_port)
    e["DISTRIFS_STORAGE_DIR"] = str(storage_dir)
    p = subprocess.Popen([PY, "-u", "datanode.py", str(data_port), str(ctrl_port)], cwd=BASE_DIR, env=e)
    _wait_port(e.get("DISTRIFS_DATANODE_HOST", "127.0.0.1"), data_port, timeout=20.0)
    return p


def _kill_proc(p: subprocess.Popen) -> None:
    if p is None or p.poll() is not None:
        return
    try:
        p.terminate()
        p.wait(timeout=2.0)
        return
    except Exception:
        pass
    try:
        p.kill()
        p.wait(timeout=5.0)
    except Exception:
        pass


def _alive(procs: list[subprocess.Popen]) -> int:
    return sum(1 for p in procs if p is not None and p.poll() is None)


def main():
    # ---- config ----
    RF = int(os.environ.get("DISTRIFS_RF", "3"))
    NODE_COUNT = int(os.environ.get("DISTRIFS_CHAOS_NODES", str(max(RF + 2, 5))))  # keep > RF
    FILES = int(os.environ.get("DISTRIFS_CHAOS_FILES", "40"))
    KILL_EVERY = float(os.environ.get("DISTRIFS_CHAOS_KILL_EVERY_S", "1.0"))
    VERIFY_PROB = float(os.environ.get("DISTRIFS_CHAOS_VERIFY_PROB", "0.25"))
    MASTER_HOST = os.environ.get("DISTRIFS_MASTER_HOST", "127.0.0.1")
    MASTER_PORT = int(os.environ.get("DISTRIFS_MASTER_PORT", "9000"))

    tmp = Path(tempfile.mkdtemp(prefix="distrifs_chaos_"))
    root = tmp / "root"
    data_root = root / "data"
    data_root.mkdir(parents=True, exist_ok=True)

    # Clean leader/WAL so runs are deterministic
    for f in ["master.leader.lock", "master.wal", "master.epoch"]:
        try:
            (BASE_DIR / f).unlink()
        except Exception:
            pass

    env = os.environ.copy()
    env["PYTHONNOUSERSITE"] = "1"
    env["DISTRIFS_PROTO_PATH"] = str(BASE_DIR / "distrifs.proto")
    env["DISTRIFS_FORCE_PROTO"] = "1"
    env["DISTRIFS_MASTER_HOST"] = MASTER_HOST
    env["DISTRIFS_MASTER_PORT"] = str(MASTER_PORT)
    env["DISTRIFS_RF"] = str(RF)
    env["DISTRIFS_CHUNK_SIZE"] = str(256 * 1024)
    env["DISTRIFS_REPAIR_INTERVAL_S"] = "1.0"
    env["DISTRIFS_GC_INTERVAL_S"] = "2.0"
    env["DISTRIFS_BLOCKREPORT_INTERVAL_S"] = "2.0"
    env["DISTRIFS_IGNORE_CTRL_C"] = "1"

    master = None
    datanodes: list[subprocess.Popen] = []
    ports: list[tuple[int, int]] = []

    try:
        # Start master
        master = _start_master(env)

        # Start datanodes
        base_data_port = 9101
        base_ctrl_port = 9201
        for i in range(NODE_COUNT):
            dp = base_data_port + i
            cp = base_ctrl_port + i
            ports.append((dp, cp))
            dn_dir = data_root / f"node-{dp}"
            dn_dir.mkdir(parents=True, exist_ok=True)
            datanodes.append(_start_datanode(env, dp, cp, dn_dir))

        # Create namespace
        _run_client(["mkdir", "/chaos"], env)

        uploaded: dict[str, tuple[Path, str]] = {}  # remote -> (local_path, sha256)

        last_kill = 0.0

        for i in range(1, FILES + 1):
            # Ensure CP precondition can be met
            while _alive(datanodes) < RF:
                print(f"[CHAOS] waiting for DataNodes: alive={_alive(datanodes)} RF={RF}", flush=True)
                time.sleep(0.25)

            # Generate random file
            local = tmp / f"f-{i}.bin"
            size = random.randint(200_000, 2_000_000)
            local.write_bytes(os.urandom(size))
            remote = f"/chaos/f-{i}.bin"
            print(f"[CHAOS] uploading {remote} (alive={_alive(datanodes)}/{NODE_COUNT})", flush=True)

            # PUT with retry on CP failure / transient master issues
            for attempt in range(1, 21):
                try:
                    _run_client(["put", str(local), remote], env)
                    uploaded[remote] = (local, _sha256_file(local))
                    break
                except subprocess.CalledProcessError as e:
                    msg = str(e)
                    # If master died, restart it
                    if "Connection refused" in msg or "UNAVAILABLE" in msg:
                        print("[CHAOS] master seems down; restarting master", flush=True)
                        if master is not None:
                            _kill_proc(master)
                        master = _start_master(env)
                        time.sleep(0.25)
                        continue

                    # If CP guardrail hit, wait for enough nodes and retry
                    if "CP write denied" in msg or "not enough active DataNodes" in msg:
                        time.sleep(0.35)
                        continue

                    # Otherwise, surface the real error
                    raise

            # Randomly verify reads
            if uploaded and random.random() < VERIFY_PROB:
                r = random.choice(list(uploaded.keys()))
                src_path, src_hash = uploaded[r]
                out = tmp / "verify.bin"
                try:
                    _run_client(["get", r, str(out)], env)
                    got = _sha256_file(out)
                    if got != src_hash:
                        raise RuntimeError(f"[CHAOS] VERIFY FAIL {r}: expected {src_hash} got {got}")
                    print(f"[CHAOS] verify ok {r}", flush=True)
                except subprocess.CalledProcessError as e:
                    print(f"[CHAOS] verify get failed {r}: {e}", flush=True)

            # Chaos kill + restart (never drop below RF)
            now = time.time()
            if now - last_kill >= KILL_EVERY and _alive(datanodes) > RF:
                last_kill = now
                candidates = [idx for idx, p in enumerate(datanodes) if p.poll() is None]
                if candidates:
                    victim_idx = random.choice(candidates)
                    victim_port = ports[victim_idx][0]
                    print(f"[CHAOS] kill datanode data_port={victim_port}", flush=True)
                    _kill_proc(datanodes[victim_idx])

                    # Restart it after a short delay
                    time.sleep(0.25)
                    dp, cp = ports[victim_idx]
                    dn_dir = data_root / f"node-{dp}"
                    datanodes[victim_idx] = _start_datanode(env, dp, cp, dn_dir)

        print("OK", flush=True)

    finally:
        # Cleanup
        for p in datanodes:
            _kill_proc(p)
        if master is not None:
            _kill_proc(master)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
