package domain

type FinalManifest struct {
	BatchID       string                `json:"batch_id"`
	ApplicationID string                `json:"application_id"`
	Window        TimeRange             `json:"window"`
	State         BatchState            `json:"state"`
	Reason        string                `json:"reason"`
	OrbitDigest   string                `json:"orbit_digest"`
	SeaDigest     string                `json:"sea_digest"`
	Resources     []ResourceRequirement `json:"resources"`
	Liveness      []DeviceLiveness      `json:"liveness"`
	EventSeq      int                   `json:"event_seq"`
	EventDigests  []string              `json:"event_digests"`
}

func BuildFinalManifest(batch TrialBatch, events []AuditEvent, live []DeviceLiveness) FinalManifest {
	digests := make([]string, 0, len(events))
	lastSeq := batch.LastEventSeq
	for _, event := range events {
		digests = append(digests, event.CanonicalDigest)
		if event.AggregateSeq > lastSeq {
			lastSeq = event.AggregateSeq
		}
	}
	return FinalManifest{
		BatchID:       batch.ID,
		ApplicationID: batch.ApplicationID,
		Window:        batch.Window.Normalize(),
		State:         batch.State,
		Reason:        batch.TerminationReason,
		OrbitDigest:   batch.OrbitDigest,
		SeaDigest:     batch.SeaDigest,
		Resources:     append([]ResourceRequirement(nil), batch.Resources...),
		Liveness:      append([]DeviceLiveness(nil), live...),
		EventSeq:      lastSeq,
		EventDigests:  digests,
	}
}

func FinalManifestDigest(batch TrialBatch, events []AuditEvent, live []DeviceLiveness) (string, error) {
	return DigestCanonical(BuildFinalManifest(batch, events, live))
}
