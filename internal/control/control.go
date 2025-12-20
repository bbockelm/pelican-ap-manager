package control

import "time"

// Config holds control-loop tunables. Defaults mirror the design doc.
type Config struct {
	// Specification error guardrail (per-user)
	SpecErrorThreshold    float64       // fraction, e.g., 0.01 for 1%
	SpecErrorWindow       time.Duration // trailing window for spec-error rate
	SpecEWMAWindow        time.Duration // EWMA window for current start rate
	SpecMinRatePerMinute  float64       // floor on proposed rate
	SpecRecoverStepPerMin float64       // additive recovery step per minute when healthy
	SpecRecoverDelay      time.Duration // time healthy before recovering

	// AIMD for (source,destination)
	AdditiveIncreaseGBPerMin float64
	MultiplicativeDecrease   float64

	ErrorGreenThreshold  float64 // e.g., 0.005 (0.5%)
	ErrorYellowThreshold float64 // e.g., 0.05 (5%)

	CostGreenThresholdPercent  float64 // e.g., 10
	CostYellowThresholdPercent float64 // e.g., 30
	MinCapacityGBPerMin        float64 // floor capacity expressed in GB/min
	DefaultJobCostGB           float64 // fallback per-job cost when unknown
	MinJobStartPerMinute       float64 // floor when translating throughput to starts
}

// DefaultConfig returns the baseline control settings.
func DefaultConfig() Config {
	return Config{
		SpecErrorThreshold:         0.01,
		SpecErrorWindow:            15 * time.Minute,
		SpecEWMAWindow:             1 * time.Minute,
		SpecMinRatePerMinute:       1.0,
		SpecRecoverStepPerMin:      1.0,
		SpecRecoverDelay:           15 * time.Minute,
		AdditiveIncreaseGBPerMin:   1000,
		MultiplicativeDecrease:     0.5,
		ErrorGreenThreshold:        0.005,
		ErrorYellowThreshold:       0.05,
		CostGreenThresholdPercent:  10,
		CostYellowThresholdPercent: 30,
		MinCapacityGBPerMin:        10,
		DefaultJobCostGB:           10,
		MinJobStartPerMinute:       1,
	}
}

// UserSpecMetrics captures the inputs needed for the spec-error guardrail.
type UserSpecMetrics struct {
	User              string
	SpecErrorRate     float64   // fraction over SpecErrorWindow
	CurrentEWMAStarts float64   // starts/min from EWMA window
	LastHealthy       time.Time // when spec-error rate last fell below threshold
	// Future: carry explicit counts if needed by policy (TODO: other agent to supply stats)
}

// UserSpecState tracks per-user control state.
type UserSpecState struct {
	MaxRatePerMinute float64
	InPenalty        bool
	LastUpdated      time.Time
}

// SpecController applies the spec-error guardrail.
type SpecController struct {
	cfg Config
}

func NewSpecController(cfg Config) SpecController {
	return SpecController{cfg: cfg}
}

// Step computes the updated max rate for a user based on current metrics.
// This is a pure function over the provided state/metrics; the caller owns persistence.
func (c SpecController) Step(now time.Time, prev UserSpecState, m UserSpecMetrics) UserSpecState {
	state := prev

	// Initialize state when empty.
	if state.MaxRatePerMinute <= 0 {
		state.MaxRatePerMinute = m.CurrentEWMAStarts
		if state.MaxRatePerMinute <= 0 {
			state.MaxRatePerMinute = c.cfg.SpecMinRatePerMinute
		}
	}

	overThreshold := m.SpecErrorRate > c.cfg.SpecErrorThreshold
	if overThreshold {
		// First entry into penalty: set baseline to current EWMA.
		if !state.InPenalty {
			if m.CurrentEWMAStarts > 0 {
				state.MaxRatePerMinute = m.CurrentEWMAStarts
			}
		}
		state.InPenalty = true
		state.MaxRatePerMinute *= 0.5
	} else {
		// Healthy path: recover slowly after a delay.
		if state.InPenalty && now.Sub(m.LastHealthy) >= c.cfg.SpecRecoverDelay {
			state.MaxRatePerMinute += c.cfg.SpecRecoverStepPerMin
			if state.MaxRatePerMinute >= m.CurrentEWMAStarts && m.CurrentEWMAStarts > 0 {
				state.InPenalty = false
			}
		}
	}

	if state.MaxRatePerMinute < c.cfg.SpecMinRatePerMinute {
		state.MaxRatePerMinute = c.cfg.SpecMinRatePerMinute
	}

	state.LastUpdated = now
	return state
}

// PairKey identifies a (source,destination) capacity bucket.
type PairKey struct {
	Source      string
	Destination string
}

// PairMetrics carries the inputs to classify the pair state.
type PairMetrics struct {
	ErrorRate float64 // fraction over trailing error window
	CostPct   float64 // estimated percent of runtime spent in stage-in
	JobCostGB float64 // per-job cost for throughput translation
}

// PairState tracks the AIMD state for a pair.
type PairState struct {
	CapacityGBPerMin float64
	LastUpdated      time.Time
}

// PairController applies the AIMD policy for a (source,destination) pair.
type PairController struct {
	cfg Config
}

func NewPairController(cfg Config) PairController {
	return PairController{cfg: cfg}
}

// Step returns an updated capacity for the pair, applying green/yellow/red logic.
func (c PairController) Step(now time.Time, prev PairState, m PairMetrics) PairState {
	state := prev
	if state.CapacityGBPerMin <= 0 {
		state.CapacityGBPerMin = c.cfg.MinCapacityGBPerMin
	}

	bandErrors := classifyBand(m.ErrorRate, c.cfg.ErrorGreenThreshold, c.cfg.ErrorYellowThreshold)
	bandCost := classifyBand(m.CostPct/100.0, c.cfg.CostGreenThresholdPercent/100.0, c.cfg.CostYellowThresholdPercent/100.0)

	red := bandErrors == bandRed || bandCost == bandRed
	yellow := !red && (bandErrors == bandYellow || bandCost == bandYellow)

	switch {
	case red:
		state.CapacityGBPerMin *= c.cfg.MultiplicativeDecrease
	case yellow:
		// hold steady
	default: // green
		state.CapacityGBPerMin += c.cfg.AdditiveIncreaseGBPerMin
	}

	if state.CapacityGBPerMin < c.cfg.MinCapacityGBPerMin {
		state.CapacityGBPerMin = c.cfg.MinCapacityGBPerMin
	}

	state.LastUpdated = now
	return state
}

// classifyBand returns green/yellow/red given thresholds (fractions).
type band int

const (
	bandGreen band = iota
	bandYellow
	bandRed
)

func classifyBand(value float64, greenThresh, yellowThresh float64) band {
	switch {
	case value < greenThresh:
		return bandGreen
	case value < yellowThresh:
		return bandYellow
	default:
		return bandRed
	}
}
