package constraints

import (
	"sort"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type EvaluationPoint struct {
	At     time.Time `json:"at"`
	Source string    `json:"source"`
}

func BuildEvaluationTimeline(w domain.TimeRange, posture []domain.PostureSample, env []domain.EnvelopeSample, sea []domain.SeaSample) []time.Time {
	seen := map[int64]time.Time{
		domain.NormalizeTime(w.Start).UnixMicro(): domain.NormalizeTime(w.Start),
		domain.NormalizeTime(w.End).UnixMicro():   domain.NormalizeTime(w.End),
	}
	add := func(t time.Time) {
		t = domain.NormalizeTime(t)
		if !t.Before(w.Start) && !t.After(w.End) {
			seen[t.UnixMicro()] = t
		}
	}
	for _, p := range posture {
		add(p.At)
	}
	for _, e := range env {
		add(e.At)
	}
	for _, s := range sea {
		add(s.At)
	}
	out := make([]time.Time, 0, len(seen))
	for _, t := range seen {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

func TimelinePoints(times []time.Time, source string) []EvaluationPoint {
	out := make([]EvaluationPoint, 0, len(times))
	for _, at := range times {
		out = append(out, EvaluationPoint{At: domain.NormalizeTime(at), Source: source})
	}
	return out
}
