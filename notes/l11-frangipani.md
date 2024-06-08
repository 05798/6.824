# Frangipani

## Overview
* Each user has a workstation which runs the Frangipani system
* When users access the file system, this is delegated to Frangipani
* Frangipani makes RPC to Petal which stores the data backing the file system

* Shared filesystem among a small, trusted group of users
* Files cached on the workstations to reduce latency
* Write-back caching -- initial writes are only persisted in the cache and only persisted on Petal later
* Complexity lies in the Frangipani client
* Scales well because bottleneck tends to be on the client

## Cache coherence
* Lock server -- logically separate
    * Contains named locks for each file
* Each worksation also contains a lock table
* Locks can be in two states
    * Busy -- owner is actively modifying the file
    * Idle -- owner is not actively modifying the file
* Rules
    * No cached data without holding the lock
    * Cannot release lock with uncommitted modifications
* Coherence protocols:
    * `request` - request a lock from lock server
    * `grant` - grants a lock to a client
    * `revoke` - request a lock helf by a client
    * `release` - releases a lock held by a client to lock server
* Example:
    1. Two workstations `WS1` and `WS2` and lockserver `LS`
    1. `WS1` requests lock for file `Z` from `LS`
    1. `LS` grants lock for file `Z` to `WS1`
    1. `WS1` reads the data from Petal
    1. `WS1` writes any modifications to its cache
    1. `WS2` requests lock for file `Z`
    1. `LS` sends revoke request to `WS1`
    1. `WS1` writes the cached data back to Petal
    1. `WS1` releases the lock
    1. `LS` grants lock for file `Z` to `WS2`
* This protocols ensure that:
    1. Only one client is writing to a file at any instance in time
    1. Petal is always updated with the latest copy of a file before it is read into a client's cache
* Optimization: have read-only locks that can be granted to multiple clients. These must all be revoked before a write lock is granted.

## Atomciity
* Distributed transactions
* Reuses the same locks for cache coherence
* Protocol
    1. Acquires all of the locks required
    1. Writes all of the changes to petals
    1. Release the locks 
* Guarantees writes are only visible after transaction is completed

## Crash recovery
* Bad idea: automatically release the locks held by a client when it has crashed
* Bad but technically correct idea: not releasing the locks by a crashed client
* Write ahead logging (c.f. Aurora)
    1. First append log entry describing full set of operations it is about to do in Petal
    1. Only apply operations once log entry is committed
* Frangipani has one log per workstation. Most systems use a single global log
* Frangipani store logs on Petal -- unusual!
* Log entries contain:
    * Log sequence number -- monotonically increasing index
    * Array of operations containing
        * Block number
        * Version number
        * Data
* On receiving a revoke entry:
    1. Write logs to petal
    1. Write modified blocks associated with lock
    1. Release the lock
* Lock server uses leases on locks to detect crashed servers
* Lock server will ask another client to read the logs to ensure the log's operations are applied before the lock is released
* Invariant: transaction operations are either fully applied or fully specified in the log entries
* Version numbers used to guard against replaying out of date logs
