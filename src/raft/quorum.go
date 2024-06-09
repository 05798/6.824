package raft

import "sync"

type QuorumOperation struct {
	participantCount int
	mu 				 sync.Mutex
	successCount     int
	responseCount    int
}

func (e *QuorumOperation) majority() int {
	return (e.participantCount + 1) / 2
}

func (e *QuorumOperation) isPending() bool {
	majorityCount := e.majority()
	return (e.successCount < majorityCount) && ((e.responseCount-e.successCount) <= majorityCount)
}

func (e *QuorumOperation) isSuccess() bool {
	return e.successCount >= e.majority()
}
