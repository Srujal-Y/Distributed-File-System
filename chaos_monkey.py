import subprocess
import time
import random
import socket
import os
import sys

MASTER_HOST = "127.0.0.1"
DATANODE_PORTS = [9100, 9101, 9102]  
CHECK_INTERVAL = 5                   
DOWN_TIME = 3                       

DATANODE_PROCS = {}


def wait_for_port(host, port, timeout=15):
   
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    raise TimeoutError(f"Port {port} on {host} not open after {timeout}s")

def start_datanode(port):
   
    proc = subprocess.Popen([sys.executable, "datanode.py", str(port)])
    DATANODE_PROCS[port] = proc
    print(f"[CHAOS_MONKEY] Started DataNode {port}")
    return proc

def stop_datanode(port):
    
    proc = DATANODE_PROCS.get(port)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        print(f"[CHAOS_MONKEY] Stopped DataNode {port}")



def chaos_loop():
    """
    Continuously kill and restart random DataNodes.
    """
    while True:
        
        port = random.choice(DATANODE_PORTS)
        print(f"[CHAOS_MONKEY] Killing DataNode {port}")
        stop_datanode(port)

        
        time.sleep(DOWN_TIME)

       
        print(f"[CHAOS_MONKEY] Restarting DataNode {port}")
        start_datanode(port)

       
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    
    for port in DATANODE_PORTS:
        start_datanode(port)

    print("[CHAOS_MONKEY] All DataNodes started. Beginning chaos...")
    chaos_loop()
