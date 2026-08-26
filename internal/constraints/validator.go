package constraints

import (
	"sort"
	"time"

	"marine-survey-payload-window-orchestrator/internal/domain"
)

type Validator struct{}

func NewValidator() *Validator {
	return &Validator{}
}

func (v *Validator) Validate(app domain.ApplicationRequest, orbit domain.OrbitSnapshot, sea domain.SeaSnapshot) []domain.Decision {
	var out []domain.Decision
	add := func(code, message string, passed bool, at time.Time) {
		out = append(out, domain.Decision{Code: code, Message: message, Passed: passed, At: domain.NormalizeTime(at)})
	}
	window := app.Window.Normalize()
	if err := window.ValidateTrialWindow(); err != nil {
		add("D01_WINDOW_RANGE", "trial window must be a half-open UTC range lasting 2 to 120 minutes", false, window.Start)
		return out
	}
	add("D01_WINDOW_RANGE", "trial window duration is within the supported half-open range", true, window.Start)
	if !orbit.Valid.ContainsWindow(window) {
		add("SNAPSHOT_ORBIT_COVERAGE", "orbit snapshot does not fully cover the application window", false, window.Start)
	} else {
		add("SNAPSHOT_ORBIT_COVERAGE", "orbit snapshot covers the application window", true, window.Start)
	}
	if !sea.Valid.ContainsWindow(window) {
		add("SNAPSHOT_SEA_COVERAGE", "sea snapshot does not fully cover the application window", false, window.Start)
	} else {
		add("SNAPSHOT_SEA_COVERAGE", "sea snapshot covers the application window", true, window.Start)
	}
	if !sortedPosture(app.Posture) || len(app.Posture) < 2 {
		add("POSTURE_SORTED", "posture samples must be sorted and contain at least two points", false, window.Start)
		return out
	}
	if !sortedEnvelope(orbit.Envelope) || len(orbit.Envelope) < 2 {
		add("ORBIT_ENVELOPE_SORTED", "orbit envelope must be sorted and contain at least two points", false, window.Start)
		return out
	}
	if !sortedSea(sea.Samples) || len(sea.Samples) < 2 {
		add("SEA_SAMPLES_SORTED", "sea samples must be sorted and contain at least two points", false, window.Start)
		return out
	}
	for _, gap := range seaGaps(sea.Samples) {
		if sea.MaxSampleGap > 0 && gap > sea.MaxSampleGap {
			add("SEA_SAMPLE_GAP", "sea sample gap exceeds snapshot maximum", false, window.Start)
			break
		}
	}
	for _, t := range BuildEvaluationTimeline(window, app.Posture, orbit.Envelope, sea.Samples) {
		p, okP := interpolatePosture(app.Posture, t)
		e, okE := interpolateEnvelope(orbit.Envelope, t)
		s, okS := interpolateSea(sea.Samples, t)
		if !okP || !okE || !okS {
			add("NO_EXTRAPOLATION", "constraint evaluation cannot extrapolate outside provided samples", false, t)
			continue
		}
		if p.RollDeg < e.RollMinDeg || p.RollDeg > e.RollMaxDeg || p.PitchDeg < e.PitchMinDeg || p.PitchDeg > e.PitchMaxDeg {
			add("D02_POSTURE_ENVELOPE", "requested posture exceeds orbit capability envelope", false, t)
		}
		if s.SignificantWaveHeightM > app.SeaLimits.MaxWaveHeightM || s.WindSpeedMS > app.SeaLimits.MaxWindSpeedMS || s.HeaveM > app.SeaLimits.MaxHeaveM {
			add("SEA_THRESHOLD", "interpolated sea state exceeds requested threshold", false, t)
		}
	}
	limit := app.MaxAngularRateDegPerSecond
	if orbit.MaxAngularRateDegPerSecond > 0 && (limit == 0 || orbit.MaxAngularRateDegPerSecond < limit) {
		limit = orbit.MaxAngularRateDegPerSecond
	}
	for i := 1; i < len(app.Posture); i++ {
		prev, cur := app.Posture[i-1], app.Posture[i]
		seconds := cur.At.Sub(prev.At).Seconds()
		if seconds <= 0 {
			add("POSTURE_SORTED", "posture sample times must strictly increase", false, cur.At)
			continue
		}
		rate := max(abs(cur.RollDeg-prev.RollDeg), abs(cur.PitchDeg-prev.PitchDeg)) / seconds
		if limit > 0 && rate > limit {
			add("D03_ANGULAR_RATE", "derived angular rate exceeds requested or orbital limit", false, cur.At)
		}
	}
	if allPassed(out) {
		add("CONSTRAINTS_ACCEPTED", "application satisfies snapshot, posture, sea and rate constraints", true, window.Start)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].At.Equal(out[j].At) {
			return out[i].Code < out[j].Code
		}
		return out[i].At.Before(out[j].At)
	})
	return out
}

func allPassed(in []domain.Decision) bool {
	for _, d := range in {
		if !d.Passed {
			return false
		}
	}
	return true
}

func sortedPosture(v []domain.PostureSample) bool {
	for i := 1; i < len(v); i++ {
		if !v[i-1].At.Before(v[i].At) {
			return false
		}
	}
	return true
}

func sortedEnvelope(v []domain.EnvelopeSample) bool {
	for i := 1; i < len(v); i++ {
		if !v[i-1].At.Before(v[i].At) {
			return false
		}
	}
	return true
}

func sortedSea(v []domain.SeaSample) bool {
	for i := 1; i < len(v); i++ {
		if !v[i-1].At.Before(v[i].At) {
			return false
		}
	}
	return true
}

func seaGaps(v []domain.SeaSample) []time.Duration {
	out := make([]time.Duration, 0, len(v)-1)
	for i := 1; i < len(v); i++ {
		out = append(out, v[i].At.Sub(v[i-1].At))
	}
	return out
}

func interpolatePosture(v []domain.PostureSample, t time.Time) (domain.PostureSample, bool) {
	i, f, ok := bracket(len(v), func(n int) time.Time { return v[n].At }, t)
	if !ok {
		return domain.PostureSample{}, false
	}
	a, b := v[i], v[i+1]
	return domain.PostureSample{At: t, RollDeg: lerp(a.RollDeg, b.RollDeg, f), PitchDeg: lerp(a.PitchDeg, b.PitchDeg, f)}, true
}

func interpolateEnvelope(v []domain.EnvelopeSample, t time.Time) (domain.EnvelopeSample, bool) {
	i, f, ok := bracket(len(v), func(n int) time.Time { return v[n].At }, t)
	if !ok {
		return domain.EnvelopeSample{}, false
	}
	a, b := v[i], v[i+1]
	return domain.EnvelopeSample{At: t, RollMinDeg: lerp(a.RollMinDeg, b.RollMinDeg, f), RollMaxDeg: lerp(a.RollMaxDeg, b.RollMaxDeg, f), PitchMinDeg: lerp(a.PitchMinDeg, b.PitchMinDeg, f), PitchMaxDeg: lerp(a.PitchMaxDeg, b.PitchMaxDeg, f)}, true
}

func interpolateSea(v []domain.SeaSample, t time.Time) (domain.SeaSample, bool) {
	i, f, ok := bracket(len(v), func(n int) time.Time { return v[n].At }, t)
	if !ok {
		return domain.SeaSample{}, false
	}
	a, b := v[i], v[i+1]
	return domain.SeaSample{At: t, SignificantWaveHeightM: lerp(a.SignificantWaveHeightM, b.SignificantWaveHeightM, f), WindSpeedMS: lerp(a.WindSpeedMS, b.WindSpeedMS, f), HeaveM: lerp(a.HeaveM, b.HeaveM, f)}, true
}

func bracket(n int, at func(int) time.Time, t time.Time) (int, float64, bool) {
	t = domain.NormalizeTime(t)
	for i := 0; i < n-1; i++ {
		a, b := domain.NormalizeTime(at(i)), domain.NormalizeTime(at(i+1))
		if t.Equal(a) {
			return i, 0, true
		}
		if t.Equal(b) {
			return i, 1, true
		}
		if t.After(a) && t.Before(b) {
			return i, t.Sub(a).Seconds() / b.Sub(a).Seconds(), true
		}
	}
	return 0, 0, false
}

func lerp(a, b, f float64) float64 { return a + (b-a)*f }
func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
