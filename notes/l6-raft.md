# Raft

## Split brain
* Two servers think they are the masters
* Typically caused by network partition
* External test and set server decides which server is the primary due to network partition

* Techniques to address split brain:
    * Build networks that are (almost) resistant to network partition. This typically involves carefully physical setups of servers
* Human intervention to manually designate a master
* Majority vote

## Consensus systems
* Odd number of servers
* Majority is required to progress e.g. elect a leader, commit a log entry
* There cannot be more than one partition with a majority
* Majority means majority of all servers -- not just a majority of the live servers
* `2f + 1` servers can survive `f` failures
* Any two majorities has a minimum overlap of 1 server
* For consecutive majorities, this means that information from the previous majority is persisted e.g. Raft term number
* Paxos and viewstamped replication were early majority vote systems

## Raft
* Raft is a replicated state machine protocol for achieving fault tolerance
* The sequence for serving a request looks roughly like:
1. Client sends request to application
1. Application send request to the leader's Raft layer
1. Leader's raft layer sends append entries requests to the follower's Raft layers
1. Leader receives a successful response from a majority -- the request is now committed
1. Application applies operations to the state machine
1. Replicas applies operations once the leader sends next append entries request
    * Alternatively, could be a separate commit message -- implementation detail
    * Depends on traffic frequency. If traffic is frequent, this is typically unnecessary.

### Log
* The log is persisted to non-volatile memory
* The log is the mechanism that the leader uses to communicate with followers
* The leader assigns an index to all operations (linearizes concurrent operations)
* The log can be replayed to recreate state after a crash (similar to event sourcing)
* The logs of the leader and followers can transiently differ in its uncommitted entries
* Committed entries can not change

### Leader election
* Leaders are not strictly necessary for majority systems
* Leader systems reduce network traffic
* Leaders are used in Raft for understandability
* Each term has <= 1 leader
* Elections are initiated by any server after it has not heard from the current leader
* Elections may be called even if there is a viable leader in the current term
* Only the leader can send an `AppendEntries` message
* An `AppendEntries` message is sent to assert leadership by causing other servers to reset their timeouts
* It is possible for an election to have no winners if there are 3 candidates
* Raft uses randomized timeouts to reduce the probability of multiple candidates in a single election
    * This works because the first candidate will typically win the election since non-candidates vote for the first candidate that contacts them
    * A larger timeout range reduces the probabibility of split Elections
    * Longer timeouts cause recoveries to be slower 
    * The 'expected' gap between the timeout should be at least the round trip time between servers
    * This timeout should be refreshed every time the timeout is reset
    * Using a static timeout could cause repeated failed elections if two servers unluckily choose the same timeout
