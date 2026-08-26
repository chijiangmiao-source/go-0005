package domain

import "fmt"

type BatchState string

const (
	StateReceived  BatchState = "RECEIVED"
	StateValidated BatchState = "VALIDATED"
	StateRejected  BatchState = "REJECTED"
	StateReserved  BatchState = "RESERVED"
	StateRunning   BatchState = "RUNNING"
	StateDegraded  BatchState = "DEGRADED"
	StateCompleted BatchState = "COMPLETED"
	StateAborted   BatchState = "ABORTED"
)

var legalTransitions = map[BatchState]map[BatchState]bool{
	StateReceived:  {StateValidated: true, StateRejected: true},
	StateValidated: {StateReserved: true, StateRejected: true},
	StateReserved:  {StateRunning: true, StateDegraded: true, StateAborted: true},
	StateRunning:   {StateDegraded: true, StateCompleted: true, StateAborted: true},
	StateDegraded:  {StateRunning: true, StateCompleted: true, StateAborted: true},
}

func (s BatchState) Terminal() bool {
	return s == StateRejected || s == StateCompleted || s == StateAborted
}

func CanTransition(from, to BatchState) bool {
	next, ok := legalTransitions[from]
	return ok && next[to]
}

func ValidateTransition(from, to BatchState) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("illegal state transition %s to %s", from, to)
	}
	return nil
}
