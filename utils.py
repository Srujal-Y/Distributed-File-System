# -------------------------------
# utils.py
# -------------------------------

import pickle
import os
import uuid
import threading

WAL_LOCK = threading.Lock()  # Ensure WAL writes are thread-safe

# -------------------------------
# Pickle helpers
# -------------------------------

def save_pickle(path, obj):
    """
    Save an object to a file using pickle.
    """
    with open(path, "wb") as f:
        pickle.dump(obj, f)

def load_pickle(path):
    """
    Load an object from a pickle file.
    """
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)

# -------------------------------
# WAL helpers
# -------------------------------

def append_wal_entry(wal_path, entry):
    """
    Append a single WAL entry to disk.
    Each entry is pickled separately.
    """
    with WAL_LOCK:
        with open(wal_path, "ab") as f:
            pickle.dump(entry, f)
            f.flush()
            os.fsync(f.fileno())

def read_wal(wal_path):
    """
    Generator to read WAL entries one by one.
    """
    if not os.path.exists(wal_path):
        return
    with open(wal_path, "rb") as f:
        while True:
            try:
                entry = pickle.load(f)
                yield entry
            except EOFError:
                break
            except Exception as e:
                print(f"[UTILS] Error reading WAL entry: {e}")
                break

# -------------------------------
# Chunk ID generation
# -------------------------------

def generate_chunk_id(filename):
    """
    Generate a unique chunk ID using filename + UUID.
    """
    return f"{filename}-{uuid.uuid4()}"
