package reservation

import "errors"

var ErrInjectedCommit = errors.New("injected reservation commit failure")

type FaultPoint string

const (
	FaultNone             FaultPoint = ""
	FaultAfterReservation FaultPoint = "after_reservation"
)

type FaultInjector struct {
	Point FaultPoint
}

func (f FaultInjector) Check(point FaultPoint) error {
	if f.Point != "" && f.Point == point {
		return ErrInjectedCommit
	}
	return nil
}
