# Final deliverables self-audit

Every item from the plan's final checklist, with the tests that prove it.
Each box is ticked only if those tests exist **and pass**. Items marked
*(adapted)* were built differently from the original plan on purpose; the
architecture doc explains why.

| ✔ | Capability | Evidence (tests) |
|---|---|---|
| ☑ | **KV API** (PUT/GET/DELETE, CAS, TTL, locks; bytes keys/values; error taxonomy) | `tests/unit/kv`, `internal/kvserver`, `tests/integration/routing_test.go` (`TestSmoke_CRUD`), `cmd/kvctl` (`TestKvctl_DataCommands`) |
| ☑ | **MemTable** *(adapted: in-memory map engine)* | `internal/storage/memkv` (CRUD, concurrency, deterministic snapshot/restore, corruption rejection) |
| ☑ | **WAL** *(adapted: the fsynced Raft log)* | `internal/raft/persistence_test.go` (survives crash after each fsync point, torn tail, corrupt middle record, truncate/compact) |
| ☑ | **SSTables** *(adapted: Raft snapshots of the map)* | `TestSnapshot_TriggeredAtThresholdAndLogTruncated`, `TestFileStorage_SnapshotWriteIsAtomic` |
| ☐ | **Bloom filters** | Not applicable: there are no on-disk files to filter. Dropped with the LSM tree. |
| ☑ | **Compaction** *(adapted: Raft log compaction after snapshots)* | `TestFileStorage_TruncateAndCompact`, `TestStress_KeyCount` (log tail stays bounded) |
| ☑ | **Crash recovery** | `TestNode_RecoversFromFileStorage`, `TestShard_Phase4_LeaderKillMidWrite`, `TestLocalCluster_KillIsRealAndRestartRecovers` (real `kill -9`), `TestCorrectness_RecoveryIsBitForBit` |
| ☑ | **Raft** (election, Pre-Vote, replication, current-term commit rule, check-quorum) | `internal/raft/*_test.go`, including `TestCommit_NeverCommitsPriorTermEntryByCount` and the failure-scenario suite in `scenarios_test.go` |
| ☑ | **Raft persistence** | `internal/raft/persistence_test.go` |
| ☑ | **Snapshots** (+ InstallSnapshot, restart replays only the tail) | `TestSnapshot_FarBehindFollowerUsesInstallSnapshot`, `TestSnapshot_RestartReplaysOnlyTail` |
| ☑ | **Raft + storage integration** (apply only after commit, determinism, apply lag) | `internal/shard/shard_test.go` (`TestShard_UncommittedWriteNeverVisible`, `TestShard_FollowerApplyLagBounded`, `TestShard_Phase4_LeaderKillMidWrite`), `TestFSM_DeterministicAcrossReplicas` |
| ☑ | **Sharding** (hash routing, versioned shard map, redirects) | `internal/router` tests, `TestPhase5_DeterministicMultiShardRouting`, `TestRedirect_FollowerRedirectsAndHintIsCached` |
| ☑ | **Multi-Raft** (independent groups, per-shard dirs, shard-ID dispatch) | `TestManager_IndependentRaftGroups`, `TestManager_PerShardStorageIsolation`, `TestDispatch_ShardIDRoutesOnSharedConnection`, `TestPhase6_NineNodeThreeShardFailureIsolation` |
| ☑ | **Leader failover** | `TestFailure_LeaderCrash`, `TestLeaderKillMidStream_ClusterLevel`, `TestPhase9_SeededLeaderKillScenario` |
| ☑ | **Node recovery** | `TestFailure_FollowerCrashAndCatchUp`, `TestCorrectness_RecoveryIsBitForBit`, `TestProcessChaos_SeededKillsAndPartitions` |
| ☑ | **Network partition simulation** | simulator: `TestFailure_MinorityPartition`, `TestFailure_SplitBrainWalkthrough`. Real cluster: `TestFaults_PartitionBlocksRPCsUntilHealed`, `TestFaults_EnforcedOnSendAndReceive` |
| ☑ | **Shard migration** *(adapted: learner-based cutover)* | `internal/migration` state-machine tests, `TestPhase7_ZeroGapMigrationUnderLoad`, `TestMigration_AbortLeavesSourceAuthoritative` |
| ☑ | **Membership changes** (learner → voter, remove, replace, decommission) | `TestMembership_AddLearnerPromoteRemoveUnderLoad`, `TestMembership_RemovingLeaderStepsDown`, `TestConfChange_*`, `TestPhase8_MembershipChangesUnderLoad`, `TestRebalance_AddNodeSpreadsReplicas` |
| ☑ | **Metrics** | `internal/metrics` tests, `TestPhase10_MetricsTellTheFailoverStory` |
| ☑ | **Benchmarks** | `benchmarks/` (matrix + micro), results in `docs/benchmarks.md` |
| ☑ | **CLI** | `cmd/kvctl/kvctl_test.go` (data, introspection, faults, JSON output, exit codes) |
| ☑ | **Documentation** | `README.md`, `docs/architecture.md`, `docs/SETUP.md`, `docs/benchmarks.md`, this file |
| ☑ | **Final demo** | `scripts/demo.sh` (unattended, exits 0); its steps are also automated as the Phase 6, 7, 9 and 10 integration tests |

Extensions from Phase 13, all built and tested: compare-and-swap
(`TestShard_ConcurrentCASExactlyOneWins`), TTL keys
(`TestFSM_TTLExpiryRevalidatesAtApply`, `TestShard_TTLExpiryConsistentAcrossReplicas`),
and distributed locks with fencing tokens and auto-renewing handles
(`internal/shard` lock tests, `tests/integration/locks_test.go`, including
the GC-pause fencing capstone `TestLocks_FencingTokenCapstone`).

Exactly-once client sessions, added after the linearizability checker
caught a retried DELETE applying twice across a leader kill:
`TestFSM_SessionDedupPreventsDoubleApply`, `TestFSM_SessionCachesCASOutcomeAndPrunes`,
`TestFSM_IdleSessionsExpireDeterministically`.

Correctness tier: `TestCorrectness_LinearizableUnderFaults` checks a history
of about 10,000 concurrent operations, recorded during leader kills and
partitions, with the checker in `internal/linearizability`.
