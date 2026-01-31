import os
import time
import random
import subprocess
import signal
import hashlib
import sys
import socket
from pathlib import Path

PY = sys.executable  # CRITICAL: use venv interpreter
MASTER = [PY, "master.py"]
DATANODE = [PY, "datanode.py"]
CLIENT = [PY, "client.py"]

RF = int(os.getenv("DISTRIFS_REPLICATION_FACTOR", "3"))

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def run(cmd, env=None):
    return subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def kill(p: subprocess.Popen):
    if p.poll() is not None:
        return
    try:
        p.send_signal(signal.SIGKILL)
    except Exception:
        try:
            p.terminate()
        except Exception:
            pass

def wait_port(host: str, port: int, timeout_s: float = 8.0):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.4):
                return
        except Exception as e:
            last = e
            time.sleep(0.15)
    raise RuntimeError(f"Port not ready: {host}:{port} ({last})")

def main():
    env = os.environ.copy()
    env.setdefault("DISTRIFS_CHUNK_SIZE", str(256 * 1024))  # speed demo
    env.setdefault("DISTRIFS_USER", "user")
    env.setdefault("DISTRIFS_MASTER_LOCK_PATH", os.path.join(env.get("DISTRIFS_STORAGE_ROOT", ""), "master.lock") or None)

    m = run(MASTER, env=env)
    wait_port("127.0.0.1", 9000, timeout_s=10.0)

    base_data = 9300
    base_ctrl = 9400
    dns = []
    ports = []
    for i in range(max(RF, 3) + 1):
        dp = base_data + i + 1
        cp = base_ctrl + i + 1
        ports.append((dp, cp))
        dns.append(run(DATANODE + [str(dp), str(cp)], env=env))
    time.sleep(2.5)

    subprocess.check_call(CLIENT + ["mkdir", "/chaos"], env=env)

    registry = {}
    iter_no = 0
    tmpdir = Path(os.path.join(Path.home(), "AppData", "Local", "Temp", "distrifs_chaos"))
    tmpdir.mkdir(parents=True, exist_ok=True)

    try:
        while True:
            iter_no += 1

            size = random.randint(32 * 1024, 2 * 1024 * 1024)
            blob = os.urandom(size)
            h = sha256_bytes(blob)

            local = tmpdir / f"f-{iter_no}.bin"
            with open(local, "wb") as f:
                f.write(blob)

            remote = f"/chaos/f-{iter_no}.bin"
            subprocess.check_call(CLIENT + ["put", str(local), remote], env=env)
            registry[remote] = h

            if random.random() < 0.45:
                idx = random.randrange(len(dns))
                kill(dns[idx])
                time.sleep(0.15)
                dp, cp = ports[idx]
                dns[idx] = run(DATANODE + [str(dp), str(cp)], env=env)

            if registry and random.random() < 0.60:
                r = random.choice(list(registry.keys()))
                out = tmpdir / "out.bin"
                subprocess.check_call(CLIENT + ["get", r, str(out)], env=env)
                with open(out, "rb") as f:
                    rh = sha256_bytes(f.read())
                if rh != registry[r]:
                    raise RuntimeError(f"CORRUPTION DETECTED for {r}")

            if iter_no % 10 == 0:
                print(f"[CHAOS] iter={iter_no} files={len(registry)}")

            time.sleep(0.12)

    finally:
        for p in dns:
            kill(p)
        kill(m)

if __name__ == "__main__":
    main()
