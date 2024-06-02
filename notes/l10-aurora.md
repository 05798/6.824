# Aurora

## History

### EC2
* Physical machine has virtual machine monitor
* Customers can run virtual machines
* Locally attached storage
* Good for stateless web servers
* EC2 was used to host databases -- not fault tolerant
* Periodic snapshots were available but meant some operations could be lost

### EBS
* Pair of servers each with a local attached storage
* Chained replication
* Good for stateful applications e.g. databases
* Each EBS volume can only be used by a single EC2 instance
* Lots of network traffic
* Both servers in the same datacentre (availability zone) -- not fault tolerant

### RDS (see figure 2 in the paper)
* Replication across AZs
* Database running on EC2 with attached EBS volume
* Replica running in different AZ
* Write operations are applied to replica before primary responds
* Expensive since sending full page writes across the network uses a lot of data

## Dataabase

* Typically store data as B-trees on disk
* Blocks of a B-tree sometimes called a page (8-16kB)
* Write-ahead log stored on disk
* In-memory cache containing some pages

### Transactions
* Sequence of operations that are atomic
* Typically locks the data that is being operated on
* Sequence of actions looks similar to:
    1. Fetch pages containing data from disk
    1. Modifies the pages in the in-memory cache (operations are not committed to disk at this point)
    1. Append to the write-ahead log for each page modified:
        * Previous state -- allows partially applied transactions to be reversed
        * Current state
        * Transaction ID
    1. Append a commit log entry to the write-ahead log
    1. Writes the cache to disk
* The write-ahead log allows the changes made in the volatile cache to be recovered after a crash
* The cache is used to batch operations

### Aurora
* 6 replicas -- 2 per AZ
* This would appear to be just as slow (if not slower) since there are now more replicas
* 2 main insights
    1. Send log entries to the replicas, not pages
        * Log entries << Pages (10B << 8kB)
        * Storage is now application specific
    2. Only a quorum (4) replicas need to respond for leader to continue
        * Don't need to wait for laggard replicas
* Claims 35x performance increase

## Quorum replication
* `N` replicas
* `W < N` replicas needed for writes
* `R < N` replicas needed for reads
* `R + W = N + 1` -- this ensures that at least one server is in both the read and write quorum
* A read may consult a quorum where only 1 replica has the most recent write. How do we figure out which replica from the quorum we should trust?
    * Writes need to a version number attached. The reader uses the value with the last quorum number
* Can adjust `R` and `W` to favour read or write performance e.g. set `R = 1` and `W = N` to heavily favour reads. Note this would make writes not fault tolerant.
* Aurora uses `N = 6`, `W = 4` and `R = 3`

## Fault tolerance goals
* Write if one AZ was dead
* Read if one AZ and one other dead server
* Survive transiently slow/unavailable replicas
* Fast rereplication
    * Server failures tends to be correlated
    * We need to replicate fast before the next failure

## Logs
* Logs are grouped by page
* Logs are not applied immediately to the page, only when the page is being read (lazy writes)
* Logs are numbered/indexed
* The leader tracks the highest log index of each replica. Having a log index of `i` implies that it has all the entries preceding `i` also.
* Ordinarily, when a read request is received, the leader just reads from any replica with the highest log index. So it doesn't need to read from a quorum.
* Quorum reads are used for crash recovery

## Scalability
* Shards in blocks of 10GB stored in protection groups of 6 replicas
* Log records are routed to the right protection group

## Recovery
* A particular node will hold shards from multiple databases
* If the node fails, all of the shards need to be reloaded which could take a long time
* The protection groups that the node participates in generally do not overlap (other than the node itself)
* This means they can replicate data in parallel since the shards that need to be replicated are (probably) not stored on the same nodes

## Optimizing for reads
* Single server for writes
* Read only replicas (up to 15)
* Leader sends logs to read-only replicas to keep cache up to date
    * Uncommitted transactions are not applied
* Reads are eventually consistent not strongly consistent


### Summary
1. Structure of transactions is often the crux of implementing distributed database systems
1. Using quorum systems
1. Using application specific storage versus general purpose storage for performance benefit
1. Practical considerations
    1. Designing for failure of entire AZs
    1. WIthstanding transient slowness/unavailability
    1. Network as the main bottleneck for performance
