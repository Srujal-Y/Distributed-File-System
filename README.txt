# DistriFS — Fault-Tolerant Distributed File System (Python)

DistriFS is a user-space distributed file system inspired by GFS/HDFS ideas: a single **Master (metadata/coordination)** and multiple **DataNodes (chunk servers)**. It supports **chunking**, **replication**, **durable metadata via WAL**, **heartbeats + block reports**, **client-driven reads/writes**, and **failure-aware behavior** (CP semantics: deny unsafe writes).

This project is intentionally “systems engineering” rather than “API development”: the system itself is the state store—no Redis/Postgres/etc.

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture](#architecture)
- [Protocols: Control Plane vs Data Plane](#protocols-control-plane-vs-data-plane)
- [How Writes Work](#how-writes-work)
- [How Reads Work](#how-reads-work)
- [Durability (Master WAL)](#durability-master-wal)
- [Fault Tolerance & Recovery](#fault-tolerance--recovery)
- [Permissions & Security Model](#permissions--security-model)
- [Chunk GC & Reference Counting](#chunk-gc--reference-counting)
- [Leader Lock + Fencing (Failover Simulation)](#leader-lock--fencing-failover-simulation)
- [Repository Layout](#repository-layout)
- [Setup & Installation](#setup--installation)
- [Quickstart](#quickstart)
- [Tests](#tests)
- [Chaos Monkey Demo](#chaos-monkey-demo)
- [Configuration (Environment Variables)](#configuration-environment-variables)
- [Operational Notes](#operational-notes)
- [Troubleshooting](#troubleshooting)
- [Limitations](#limitations)

---

## Key Features

### Core FS capabilities
- Hierarchical namespace (directories + files)
- `mkdir`, `ls`, `stat`, `mv`, `rm`
- File upload/download:
  - `put <local> <remote>`
  - `get <remote> <out>`
  - `getrange <remote> <offset> <length> <out>`

### Distributed storage
- Fixed-size **chunking** (configurable chunk size)
- **Replication factor** configurable (default 3)
- **CP semantics**: writes are denied if safe replication is not possible

### Master responsibilities
- Namespace + metadata held in RAM for low-latency lookups
- **Write-Ahead Log (WAL)** for durable metadata mutations
- Tracks DataNode liveness via heartbeats
- Maintains chunk-to-replica location registry via block reports
- Repair loop for under-replicated chunks

### DataNode responsibilities
- Stores chunks as local files (`chunk-<id>`) plus metadata sidecars
- Handles read/write I/O on the data plane
- Sends:
  - periodic heartbeats
  - periodic block reports (full inventory scan)

### Correctness / integrity
- Per-chunk integrity (checksum/CRC) to detect corruption/bit-rot
- Client retries and replica switching on corrupt reads
- Safe write behavior:
  - temp files + atomic rename on successful commit

### “Hardcore” additions
- Chunk GC with reference counting and tombstoning
- Stronger permission checks (owner/mode + enforced auth metadata)
- Master failover simulation with leader lock + fencing (epoch)

---

## Architecture

DistriFS uses a Master–DataNode architecture:

- **Master (metadata service)**  
  Stores the file tree, chunk mappings, and replica locations. Coordinates writes and repairs. Does *not* store bulk file bytes.

- **DataNodes (chunk servers)**  
  Store raw chunk files on disk and serve data reads/writes.

- **Client (CLI/SDK)**  
  Implements chunking and reconstructs files. Talks to Master for metadata/plans and to DataNodes for bulk data transfer.

**Core philosophy:** assume components fail; software must recover safely.

---

## Protocols: Control Plane vs Data Plane

### Control Plane (gRPC + Protobuf)
Used for:
- DataNode registration
- heartbeats
- block reports
- metadata operations (mkdir/ls/stat/mv/rm)
- write/read planning (assigning chunks, replica selection, etc.)

### Data Plane (TCP streams)
Used for:
- transferring chunk bytes (read/write)
- pipelined replication streams

This split avoids gRPC overhead for large binary transfers while keeping strict schemas for coordination.

---

## How Writes Work

1. **Client requests a write plan** from Master (control plane)
2. Master:
   - validates permissions
   - determines chunk IDs (or allocates) and replica placements (RF nodes)
   - returns a plan (primary + pipeline replicas)
3. Client writes each chunk on the **data plane**:
   - streams chunk bytes to the primary DataNode
   - primary stores and forwards to the next replica (pipeline)
4. Acknowledgements flow backward through the pipeline:
   - last replica → … → primary → client
5. DataNodes commit chunk data using:
   - write to temp file
   - fsync
   - atomic rename
6. Master records metadata mutation in WAL before committing in-memory state.

If there are not enough active DataNodes to satisfy RF, Master denies the write (**CP** behavior).

---

## How Reads Work

1. Client requests a read plan from Master:
   - chunk list + replica locations
2. Client reads chunks directly from DataNodes
3. On corruption / failure:
   - client retries another replica
   - optionally triggers repair/healing paths

For `getrange`, the client:
- computes chunk boundaries
- reads only required byte ranges from affected chunks
- stitches into the output.

---

## Durability (Master WAL)

The Master holds metadata in-memory for speed. To survive crashes, it uses a **Write-Ahead Log (WAL)**:

- Every metadata mutation is appended to `master.wal`
- WAL is fsynced before applying the mutation to in-memory structures
- On restart:
  - Master replays WAL to rebuild metadata state
  - chunk location registry is rebuilt via DataNode block reports

This ensures metadata survives power loss or process crashes.

---

## Fault Tolerance & Recovery

### Heartbeats
DataNodes send periodic heartbeats. If heartbeats are missed beyond a threshold, a node is considered inactive.

### Block Reports
DataNodes periodically scan disk and report all chunks they own. This:
- rebuilds the Master’s location registry after Master restart
- allows detection of stale chunks or missing replicas

### Repair (Under-Replication)
If a chunk falls below RF (node down / disk loss), the Master:
- selects a healthy source replica
- selects a new destination DataNode
- instructs replication to restore RF

### Consistency under failure
- If replication cannot be guaranteed, writes are denied (CP)
- Reads can proceed if at least one valid replica is available

---

## Permissions & Security Model

The Master enforces permissions for namespace and file operations.

- Objects have:
  - `owner`
  - `mode` (POSIX-like)
- Clients send user identity via request metadata (e.g., `x-user`)
- Permission checks occur on:
  - directory traversal
  - mkdir/ls/stat/mv/rm
  - write plan allocation
  - read plan access (if configured)

The test suite includes a negative-permissions scenario (e.g., Bob cannot list Alice’s private directory).

---

## Chunk GC & Reference Counting

DistriFS includes cleanup mechanisms to avoid leaking disk space:

- When a file is deleted:
  - Master marks chunks as dereferenced (refcount--)
  - chunks become eligible for GC after a delay window
- DataNodes periodically run GC:
  - delete chunk files whose refcount is 0 and tombstone is mature
  - keep chunk metadata sidecars consistent

This prevents orphaned chunk buildup after deletes and repeated tests.

---

## Leader Lock + Fencing (Failover Simulation)

To simulate Master failover safely:

- Only one Master instance can hold leadership at a time via a **leader lock**
- Leadership includes a monotonically increasing **epoch**
- RPCs that mutate state are fenced:
  - requests include the leader epoch
  - stale leaders cannot commit writes even if still running

The test suite launches a second master, terminates the first, and verifies:
- the second acquires the lock
- the service becomes available again
- metadata ops still work

---

## Repository Layout

Typical layout:

- `master.py` — Master service (metadata, WAL, repair, GC coordination, leader epoch)
- `datanode.py` — DataNode service (chunk I/O, replication pipeline, block reports)
- `client.py` — CLI client (mkdir/ls/stat/put/get/getrange/rm/mv)
- `utils.py` — helpers (proto generation, hashing, framing, filesystem paths, etc.)
- `distrifs.proto` — gRPC/Protobuf schema
- `test_distrifs.py` — integration tests (end-to-end)
- `chaos_monkey.py` — stress test: uploads while randomly killing DataNodes

---

## Setup & Installation

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1

pip install -U pip
pip install grpcio grpcio-tools protobuf

