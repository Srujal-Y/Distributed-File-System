# DistriFS — A Fault-Tolerant Distributed File System (Python)

DistriFS is a **user-space distributed file system** inspired by **GFS/HDFS**, implemented **entirely in Python**.
It is designed to be a *systems* project (not an API wrapper): the Master is the metadata store, the DataNodes store raw chunk blobs, and the Client orchestrates chunking, streaming, replication, retries, and recovery.

> Philosophy: **hardware fails; software must survive**.  
> CAP stance: **CP** for writes — a write is **denied** unless it can be safely replicated.

## Architecture

### Master (metadata + control plane)
- In-memory namespace tree (directories, files, owners, POSIX-like modes)
- File → chunk mapping and chunk → replica locations
- **Write-Ahead Log (WAL)** for durability (mutations logged before in-memory updates)
- Heartbeat tracking + under-replication repair loop
- Leader lock + fencing epoch for failover simulation (single leader at a time)

### DataNodes (chunk servers)
- Store raw chunk blobs on local disk (`chunk-<uuid>` + metadata sidecar)
- Data-plane TCP for bulk reads/writes
- Control-plane gRPC for:
  - registration / heartbeats / block reports
  - chunk copy / delete (repair + GC)
- **CRC32 integrity checks** on write and on read (bit-rot detection)

### Client (CLI)
- Chunking and streaming I/O
- Pipelined replication (client → DN1 → DN2 → DN3)
- Retry logic on failures (re-plan from master, switch replicas)
- CLI operations: `mkdir`, `ls`, `stat`, `put`, `get`, `getrange`, `rm`, `mv`

## Fault Tolerance Features
- Replication factor `RF` (default 3)
- Missing-heartbeat detection + background repair
- Block reports rebuild the master's location registry after restart
- Corruption detection via CRC32; client retries other replica
- Stale replica handling via version checks from block reports
- Chunk GC + reference counting (deletes unreferenced chunks)

## Running Locally

### Install
```bash
python -m venv .venv
# Windows:
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### Run the integration test
```bash
python test_distrifs.py
```

### Run chaos monkey
```bash
python chaos_monkey.py
```

## Configuration (env vars)
- `DISTRIFS_MASTER_HOST`, `DISTRIFS_MASTER_PORT`
- `DISTRIFS_RF` (replication factor)
- `DISTRIFS_CHUNK_SIZE`
- `DISTRIFS_HEARTBEAT_INTERVAL_S`, `DISTRIFS_BLOCKREPORT_INTERVAL_S`
- `DISTRIFS_GC_INTERVAL_S`, `DISTRIFS_REPAIR_INTERVAL_S`

## Notes
This is a learning-focused implementation. It intentionally avoids external databases (no Redis/Postgres/etcd).
All persistence is handled via the master's WAL plus DataNode local disks.


