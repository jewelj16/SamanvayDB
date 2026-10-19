# SamanvayDB - Fault-Tolerant Sharded Key-Value Store

A fault-tolerant, sharded, multi-Raft key-value store with an LSM-tree storage engine, built from scratch in Go. SamanvayDB replicates data across nodes using the Raft consensus protocol, persists it through a crash-safe log-structured merge-tree, and distributes keys across shards for horizontal scalability.
