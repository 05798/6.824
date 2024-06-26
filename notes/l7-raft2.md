# Raft pt. 2

## Fast backup
* Log reconcilliation can potentially involve erasing/overwriting the entirety of the log
    * e.g. one of the followers never receives the `AppendEntries` messages
* To reconcile logs, the master must find the point the latest entry in the follower's log which agrees with their own log
* A naive strategy is to decrement the master's nextIndex entry for the follower until a consistent log entry is found
* This can involve a lot of RPC calls if the follower has many inconsistent logs
* We can optimise log reconciliation by augmenting the `AppendEntries` RPC reply with:
    * the term of the conflicting entry
    * the index of the first log entry with that term
* The leader can then skip to later of:
    * The first of its own log entry with the conflicting term
    * The index of the follower's first log entry with the conflicting term

## Persistent state
* Used for reboots
* For single failures, we could completely reconstruct the state using log reconciliation
* Persistent state is used to guard against simultaneous failures (e.g. power outage)
* A node persists three things:
    * the (entire) log (to reconstruct state)
    * the current term (in case the logs don't contain entries for terms that have passed)
    * the node it voted for in the current term (to stop double votes on reboot)
* The state is not actually persisted. It is reconstructed from the log (c.f. event sourcing)
* Writing to disk is expensive (~10ms) -- they're likely to be the limiting factor for performance
    * SSD (~0.1ms)
    * Battery backed DRAM
* We can potentially batch persistent operations by observing that we only need to persist things when we respond to an extern request
* Use `fsync` to flush to disk

## Volatile state
* Can be reconstructed from a node's own persistent state and append entry replies from other nodes

## Snapshots and log compaction:
* Optimisation to circumvent having to replay the entire log to reconstruct state
* Save the application state as at a known log entry
* Application state tends to be smaller than the log
* Log entries before the specificied log entry are discarded
* This can cause issues if the discarded log entries are needed to reconcile logs with a follower
* The master will send the snapshot rather than the logs in this situation

## Linearizability:
* Informally - the behavior you would expect from a non-replicated service
* Execution history is linearizable if:
    * There is a total order of operations that matches realtime for non-concurrent requests (matching real time means that if two operations do not overlap in time, then the first operation to complete is ordered first) AND
    * Each read sees the most recent write according to that order
* Linearizable roughly is interchangeable with the term strongly consistent
* If a system is linearizable, all clients will agree on the order of the write operations

### Retries
- If a client retries a request, the server must serve up the response to the first request
- This is done by keeping the original response and associating with the original request's identifier
- Client retries can be interpreted as a separate request or as a continuation of the original request
    - This can make the linearizability of a system dependent upon whether you count the number of requests from the perspective of the application or the transport layer