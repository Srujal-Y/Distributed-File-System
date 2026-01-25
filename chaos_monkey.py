# -------------------------------
# chaos_monkey.py - 8-week spec
# -------------------------------

import subprocess
import time
import random
import socket
import os
import sys

# -------------------------------
# Configuration
# -------------------------------
MASTER_HOST = "127.0.0.1"
DATANODE_PORTS = [9100, 9101, 9102]  # ports of your DataNodes
CHECK_INTERVAL = 5                   # seconds between chaos events
DOWN_TIME = 3                        # seconds DataNode stays down

# Store subprocess objects for running DataNodes
DATANODE_PROCS = {}

# -------------------------------
# Helper Functions
# -------------------------------

def wait_for_port(host, port, timeout=15):
    """
    Wait until a TCP port is open (Master or DataNode ready).
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    raise TimeoutError(f"Port {port} on {host} not open after {timeout}s")

def start_datanode(port):
    """
    Start a DataNode process and store it in DATANODE_PROCS.
    """
    proc = subprocess.Popen([sys.executable, "datanode.py", str(port)])
    DATANODE_PROCS[port] = proc
    print(f"[CHAOS_MONKEY] Started DataNode {port}")
    return proc

def stop_datanode(port):
    """
    Terminate a running DataNode process.
    """
    proc = DATANODE_PROCS.get(port)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        print(f"[CHAOS_MONKEY] Stopped DataNode {port}")

# -------------------------------
# Chaos Monkey Logic
# -------------------------------

def chaos_loop():
    """
    Continuously kill and restart random DataNodes.
    """
    while True:
        # Pick a random DataNode to kill
        port = random.choice(DATANODE_PORTS)
        print(f"[CHAOS_MONKEY] Killing DataNode {port}")
        stop_datanode(port)

        # Keep it down for a short random interval
        time.sleep(DOWN_TIME)

        # Restart the DataNode
        print(f"[CHAOS_MONKEY] Restarting DataNode {port}")
        start_datanode(port)

        # Wait before next chaos event
        time.sleep(CHECK_INTERVAL)

# -------------------------------
# Main
# -------------------------------

if __name__ == "__main__":
    # Ensure all DataNodes are started initially
    for port in DATANODE_PORTS:
        start_datanode(port)

    print("[CHAOS_MONKEY] All DataNodes started. Beginning chaos...")
    chaos_loop()
