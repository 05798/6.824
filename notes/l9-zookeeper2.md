# Zookeeper pt.2 + CRAQ

## Applications
1. Test and set service in VMware FT
2. Publish configuration information
3. Elect a leader

## Z-nodes
* Zookeeper structures resources much like a file system
* Zookeeper can be used to run several, possibly unrelated, applications simultaneously
* Each application has it's own top level directory
* The nodes on this file system tree are called z-nodes
* z-nodes have version numbers
* There are 3 types of z-nodes:
    * Regular - can write and delete
    * Ephemeral - is tied to a client and deleted if the client is dead
    * Sequential - has a number suffix appended on by Zookeeper. The suffix is monotonically increasing and never repeated so clients can concurrently create a file with the same name

## API
* `create(path, data, z_node_type) -> isSuccess`
    * Only one client can create on a given path. If the file already exists, an error is returned
* `delete(path, version)`
* `exists(path, watch) -> doesExist`
    * If `watch` is set, Zookeeper notifies the client if the file is changed
* `get_data(path, watch) -> data, version`
* `set_data(path, data, version) -> isSuccess`
* `list(prefix) -> listOfFilesWithPrefix`

## Example -- incrementing a counter
```python
def increment_counter():
    while True:
        x, version = get_data(path)
        # set_data is only successful if version is the latest version
        if set_data(path, x + 1, version):
            break
        # If we got here, we tried to a set using stale data
```

* This can be problematic if lots of clients are trying to increment the counter concurrently
* This can be addressed with exponential or randomised backoffs for retires (adaptive retries?)
* This is an atomic action -- sometimes called mini-transactions

## Example -- locks
```python
def acquire_lock():
    while True:
        if create(path, data, z_node_type="ephemeral") is True:
            # We created the file so we acquire the lock
            return
        if not exists(path, watch=True):
            # The file was deleted after our create call
            continue
        # It is important to check the return of exists()
        # Otherwise if the file was deleted after our create() call, we would miss the delete notification
        await watch()
```

* `acquire_lock` suffers from the herd effect -- multiple clients trying to mutate the data simultaneously can take `O(n^2)` time complexity. There are `O(n)` failed retries for every successful lock acquisition.

```python
def acquire_lock() -> bool:
    suffix = create(path, data, z_node_type="sequential+ephemeral")
    while True:
        suffixes = list_(path)
        if min(suffixes) == suffix:
            return
        prior_suffix = # get the next lower suffix from suffixes 
        if not exists(prior_suffix, watch=True):
            return
        await watch()
```

* This allows `n` servers to acquire the lock in `O(n)` time complexity
* This is known as a scalable lock
* Question -- why do we need to relist the suffixes on each loop iteration
    * Because the client preceding us might have died and their lock file might have been deleted. This means we need to redetermine our preceding client.
* The scalable lock is not guaranteed to be atomic. If a client fails while updating the data, it will release the lock and allow the next client to access the protected resources.

# Chained replication with apportioned queries - CRAQ
* Read from any replica but provides linearizability

## Chained replication

### Summary
* Chain of servers (linked list) with a head and tail server
* Client sends writes to the head
* Heads are propagated down the chain
* Tail sends the response to the client
* Clients send read requests to the tail who responds immediately
* If a write is observable, then that write has been processed by every server

### Comments
* Quite commonly used
* Failure recovery is much simpler to reason about (linked list structure)
* In Raft, leader has to send requests to every follower. In chained replication, the head only has to send 1 request.
* In Raft, reads and writes are both processed by the leader. In chained replication, the tail serves the read requests.
* Not resilient to network partition e.g. the chain is split in two and each segment could try to become the authoritative instance
* Usually used in conjunction with a configuration manager
* The configuration manager monitors liveness of the chained replicas. If it detects one is dead, it sends another configuration to the remaining replicas.
* Configuration managers use replication schemes such as Raft, Paxos or Zookeeper that are resistant to network partition
* Can have a weak link in the chain e.g. lagging server which aren't a problem with consensus systems