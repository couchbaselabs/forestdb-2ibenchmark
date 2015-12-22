# forestdb-2ibenchmark

This test simulates Couchbase's secondary indexing workload. It writes directly
into forestdb database(s).

### Overview of the test case as of 12/22/2015

The test has two phases: initial and incremental

#### Initial workload

Using one thread, perform create workloads only and for every N(=300) seconds
performs a fdb_commit.

#### Incremental workload

Writer thread performs create, updates, and delete workload, as well as doing
in-memory snapshot creation and fdb_commit. In memory snapshots are kept open
and inserted to a queue, protected by a lock, and shared with the reader
thread.

Reader thread, keeps the number of inmemory snapshots short at 5 by closing the
old ones up to the last five, and range reads using the 5th snapshot using an
iterator (currently reads from the beginning of the iterator and does not jump
into the middle of the snapshot tree). I also throttled the number of read ops
with respect to the ops done by the writer thread. Otherwise the ratio between
read and mutation ops can be as large as hundreds.

Compactor thread issues compact_up_to periodically, keeping the last 5
persisted snapshots across compactions.

Stats thread: right now periodically prints number of operations to track
progress so that I know it is not hung.
