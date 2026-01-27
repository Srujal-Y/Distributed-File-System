import pickle
import os
import uuid
import threading

WAL_LOCK = threading.Lock()  

def save_pickle(path, obj):
     
    with open(path, "wb") as f:
        pickle.dump(obj, f)

def load_pickle(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)



def append_wal_entry(wal_path, entry):
    
    with WAL_LOCK:
        with open(wal_path, "ab") as f:
            pickle.dump(entry, f)
            f.flush()
            os.fsync(f.fileno())

def read_wal(wal_path):
    
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


def generate_chunk_id(filename):
    return f"{filename}-{uuid.uuid4()}"
