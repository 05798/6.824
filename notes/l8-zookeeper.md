# Zookeeper

## Performance considerations of replicated systems
* Adding extra servers in Raft don't increase performance (likely decreases performance in reality)
* Many real world systems have much higher read volumes than write
* Idea (!) -> send read requests to replicas
* This generally makes systems not linearizable (e.g. send read request to a partitioned server in Raft)
* Zookeeper doesn't offer linearizability

## Zookeeper

### Guarantees
1. All state changing operations are linearizable (e.g. writes) regardless of the client
2. FIFO client order
    * the client specifies an explicit logical order on the writes (probably by assigning an order to the request)
    * a client read request will never observe state that is older than the previous latest observed state
    * A clients request history is linearizable (not sure about this?)

* Zookeeper allows clients to watch values which will send the client a notification if that value changes
* The watch notification is guaranteed to happen before any further reads of that value
