package reservation

import (
	"fmt"
	"sync"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type Store interface {
	Resources() map[string]domain.ResourceSpec
	Reservations() []domain.Reservation
	SaveReservation(domain.Reservation) error
}

type rollbackStore interface {
	ReleaseBatchReservations(batchID string) error
}

type Conflict struct {
	ResourceID string           `json:"resource_id"`
	Window     domain.TimeRange `json:"window"`
	Requested  int              `json:"requested"`
	Capacity   int              `json:"capacity"`
}

func (c Conflict) Error() string {
	return fmt.Sprintf("resource %s capacity exceeded: requested %d of %d", c.ResourceID, c.Requested, c.Capacity)
}

type Reserver struct {
	mu     sync.Mutex
	store  Store
	faults FaultInjector
}

func NewReserver(store Store) *Reserver {
	return &Reserver{store: store}
}

func NewReserverWithFaults(store Store, faults FaultInjector) *Reserver {
	return &Reserver{store: store, faults: faults}
}

func (r *Reserver) Reserve(batchID string, window domain.TimeRange, reqs []domain.ResourceRequirement) ([]domain.Reservation, *Conflict, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := window.ValidateTrialWindow(); err != nil {
		return nil, nil, err
	}
	specs := r.store.Resources()
	existing := r.store.Reservations()
	demand := map[string]int{}
	for _, req := range reqs {
		if req.Quantity <= 0 {
			return nil, nil, fmt.Errorf("resource %s quantity must be positive", req.ResourceID)
		}
		demand[req.ResourceID] += req.Quantity
	}
	for resourceID, quantity := range demand {
		usage := CalculateUsage(resourceID, window, existing, specs)
		if usage.Capacity <= 0 {
			return nil, &Conflict{ResourceID: resourceID, Window: window, Requested: quantity, Capacity: 0}, nil
		}
		if usage.Used+quantity > usage.Capacity {
			return nil, &Conflict{ResourceID: resourceID, Window: window, Requested: usage.Used + quantity, Capacity: usage.Capacity}, nil
		}
	}
	out := BuildReservations(batchID, window, reqs)
	for _, res := range out {
		if err := r.store.SaveReservation(res); err != nil {
			// 存储在写入某条预留后故障时，已写入的预留必须回滚，
			// 否则会占用对应时间窗并导致后续重试无故收到 RESOURCE_CONFLICT。
			r.releaseBatch(batchID)
			return nil, nil, err
		}
	}
	if err := r.faults.Check(FaultAfterReservation); err != nil {
		r.releaseBatch(batchID)
		return nil, nil, err
	}
	return out, nil, nil
}

// releaseBatch 在预留写入中途或提交前发生故障时释放该批次已写入的全部预留，
// 确保失败申请不会遗留资源占用。
func (r *Reserver) releaseBatch(batchID string) {
	if rollback, ok := r.store.(rollbackStore); ok {
		_ = rollback.ReleaseBatchReservations(batchID)
	}
}
