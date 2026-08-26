package constraints

import "marine-survey-payload-window-orchestrator/internal/domain"

type Summary struct {
	Accepted bool              `json:"accepted"`
	Failures []domain.Decision `json:"failures"`
	Codes    []string          `json:"codes"`
}

func Summarize(decisions []domain.Decision) Summary {
	out := Summary{Accepted: true}
	seen := map[string]bool{}
	for _, decision := range decisions {
		if !seen[decision.Code] {
			out.Codes = append(out.Codes, decision.Code)
			seen[decision.Code] = true
		}
		if !decision.Passed {
			out.Accepted = false
			out.Failures = append(out.Failures, decision)
		}
	}
	return out
}

func FirstFailure(decisions []domain.Decision) (domain.Decision, bool) {
	for _, decision := range decisions {
		if !decision.Passed {
			return decision, true
		}
	}
	return domain.Decision{}, false
}
