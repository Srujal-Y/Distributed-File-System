\# DistriFS Wire Protocol Specification (v1)



This document defines the authoritative wire-level protocol used by DistriFS.

All communication uses TCP sockets and Python pickle-serialized dictionaries.

There are two planes:



\- Control Plane (Metadata \& coordination)

\- Data Plane (Bulk data transfer)



The system is explicitly CP (Consistency over Availability).

Any operation that cannot be safely replicated MUST fail.



---



\## 1. Control Plane Protocol



\### 1.1 DataNode → Master



\#### REGISTER

Sent once at DataNode startup.



{

&nbsp; op: "REGISTER",

&nbsp; port: int

}



Response:

{

&nbsp; status: "ok",

&nbsp; node\_id: str

}



---



\#### HEARTBEAT

Sent every HEARTBEAT\_INTERVAL seconds.



{

&nbsp; op: "HEARTBEAT",

&nbsp; node\_id: str

}



Response:

{

&nbsp; status: "ok"

}



Failure to receive heartbeats for > DEAD\_AFTER seconds

causes the Master to evict the DataNode.



---



\#### BLOCK\_REPORT

Sent every BLOCK\_REPORT\_INTERVAL seconds.

Reports all chunks currently stored on disk.



{

&nbsp; op: "BLOCK\_REPORT",

&nbsp; node\_id: str,

&nbsp; chunks: {

&nbsp;   chunk\_id: version:int

&nbsp; }

}



Response:

{

&nbsp; status: "ok"

}



The Master compares reported versions with authoritative versions

to detect stale replicas.



---



\### 1.2 Client → Master



\#### ASSIGN (Write Intent)

Requests a new chunk assignment.



{

&nbsp; op: "ASSIGN",

&nbsp; filename: str

}



Response:

{

&nbsp; chunk\_ids: \[str],

&nbsp; assigned\_nodes: \[\[(host:str, port:int)]],

&nbsp; versions: \[int]

}



If fewer than REPLICATION\_FACTOR DataNodes are available,

the Master MUST reject the request.



---



\#### COMMIT (Write Completion)

Sent after a successful pipeline write.



{

&nbsp; op: "COMMIT",

&nbsp; chunk\_id: str,

&nbsp; checksum: int,

&nbsp; size: int,

&nbsp; version: int

}



Response:

{

&nbsp; status: "ok"

}



The Master records this metadata durably in the WAL.



---



\#### GET\_FILE (Read Plan)

Fetches metadata needed for reading a file.



{

&nbsp; op: "GET\_FILE",

&nbsp; filename: str

}



Response:

{

&nbsp; filename: str,

&nbsp; chunks: \[

&nbsp;   {

&nbsp;     chunk\_id: str,

&nbsp;     replicas: \[(host:str, port:int)],

&nbsp;     version: int,

&nbsp;     checksum: int,

&nbsp;     size: int

&nbsp;   }

&nbsp; ]

}



---



\## 2. Data Plane Protocol



\### 2.1 STORE (Client or DataNode → DataNode)



Writes a chunk to disk. May include pipeline forwarding targets.



{

&nbsp; op: "STORE",

&nbsp; chunk\_id: str,

&nbsp; content: bytes,

&nbsp; version: int,

&nbsp; replicate\_to: \[(host:str, port:int)]

}



Behavior:

\- DataNode MUST write data atomically (temp file + rename).

\- DataNode MUST forward data to replicate\_to nodes before ACK.

\- ACK is sent only after all downstream replicas ACK.



Response:

{

&nbsp; status: "ok",

&nbsp; meta: {

&nbsp;   version: int,

&nbsp;   checksum: int,

&nbsp;   size: int

&nbsp; }

}



---



\### 2.2 READ (Client → DataNode)



Reads a chunk.



{

&nbsp; op: "READ",

&nbsp; chunk\_id: str

}



Response on success:

{

&nbsp; status: "ok",

&nbsp; chunk\_id: str,

&nbsp; content: bytes,

&nbsp; meta: {

&nbsp;   version: int,

&nbsp;   checksum: int,

&nbsp;   size: int

&nbsp; }

}



Response on corruption:

{

&nbsp; status: "error",

&nbsp; error: "CORRUPT"

}



---



\### 2.3 REPLICATE (Master → DataNode)



Instructs a DataNode to copy a chunk to another DataNode.



{

&nbsp; op: "REPLICATE",

&nbsp; chunk\_id: str,

&nbsp; target\_host: str,

&nbsp; target\_port: int

}



Response:

{

&nbsp; status: "ok" | "error"

}



---



\### 2.4 DELETE (Master → DataNode)



Deletes a stale replica.



{

&nbsp; op: "DELETE",

&nbsp; chunk\_id: str

}



Response:

{

&nbsp; status: "ok"

}



---



\## 3. Failure Guarantees



\- A write is acknowledged \*\*only if fully replicated\*\*.

\- Partial writes are discarded via atomic temp files.

\- Corrupted chunks are detected via CRC32.

\- Stale replicas are deleted using version comparison.

\- The Master WAL is the source of truth and is replayed on restart.



Any deviation from this protocol is considered a bug.



