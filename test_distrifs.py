import subprocess
import time
import os
import socket
import sys
import random
import signal

# -------------------------------
# Configuration
# -------------------------------
MASTER_HOST = "127.0.0.1"
MASTER_PORT = 9000
DATANODE_PORTS = [9100, 9101, 9102]
TEST_FILE = "test.txt"
CHAOS_INTERVAL = 5
DATANODE_DOWN_TIME = 3

processes = []

# -------------------------------
# Helper Functions
# -------------------------------

def wait_for_port(host, port, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    raise TimeoutError(f"Port {port} on {host} not open after {timeout}s")

def cleanup():
    """
    Terminate all subprocesses (Master, DataNodes, Client, Chaos Monkey)
    """
    for p in processes:
        try:
            if p.poll() is None:
                p.terminate()
        except Exception:
            pass
    time.sleep(1)
    processes.clear()

def create_test_file():
    with open(TEST_FILE, "w") as f:
        f.write("Hello DistriFS!\n" * 20)
    print(f"[TEST] Test file '{TEST_FILE}' ready.")

# -------------------------------
# Start Components
# -------------------------------

def start_master():
    print("[TEST] Starting Master...")
    master = subprocess.Popen([sys.executable, "master.py"])
    processes.append(master)

    # Wait for Master to actually bind
    wait_for_port(MASTER_HOST, MASTER_PORT, timeout=30)
    print("[TEST] Master ready.")
    return master

def start_datanode(port):
    proc = subprocess.Popen([sys.executable, "datanode.py", str(port)])
    processes.append(proc)

    # Wait briefly for registration retries
    time.sleep(1)
    print(f"[TEST] DataNode {port} started.")
    return proc

def start_client_upload(file):
    print(f"[TEST] Uploading file '{file}' via Client...")
    client = subprocess.Popen([sys.executable, "client.py", file])
    processes.append(client)
    client.wait()
    print(f"[TEST] Client upload complete.")

# -------------------------------
# Chaos Monkey Simulation
# -------------------------------

def chaos_monkey_loop():
    """
    Simulate Chaos Monkey in the test:
    Randomly kill and restart DataNodes continuously.
    """
    print("[TEST] Starting Chaos Monkey...")
    end_time = time.time() + 20  # run chaos for 20 seconds
    while time.time() < end_time:
        port = random.choice(DATANODE_PORTS)
        dn_proc = next((p for p in processes if str(port) in " ".join(p.args)), None)
        if dn_proc and dn_proc.poll() is None:
            print(f"[CHAOS_MONKEY] Killing DataNode {port}")
            dn_proc.terminate()
            time.sleep(DATANODE_DOWN_TIME)
            print(f"[CHAOS_MONKEY] Restarting DataNode {port}")
            start_datanode(port)
        time.sleep(CHAOS_INTERVAL)
    print("[CHAOS_MONKEY] Chaos simulation finished.")

# -------------------------------
# Main Test Function
# -------------------------------

def run_test():
    cleanup()
    create_test_file()

    master = start_master()
    time.sleep(2)  # ensure Master is fully ready on Windows

    # Start all DataNodes
    for port in DATANODE_PORTS:
        start_datanode(port)
    time.sleep(3)  # give DataNodes time to register

    # Upload file
    start_client_upload(TEST_FILE)

    # Run Chaos Monkey simulation
    chaos_monkey_loop()

    # Final Cleanup
    cleanup()
    print("[TEST] All processes terminated. Test completed successfully.")

# -------------------------------
# Entry Point
# -------------------------------

if __name__ == "__main__":
    run_test()