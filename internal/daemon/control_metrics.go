package daemon

import (
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/control"
	"github.com/bbockelm/pelican-ap-manager/internal/state"
	"github.com/bbockelm/pelican-ap-manager/internal/stats"
)

// pairMetrics derives control inputs for a (source,destination) pair using recent stats and job runtimes.
func pairMetrics(cfg control.Config, st *state.State, tracker *stats.Tracker, source, destination string) control.PairMetrics {
	m := control.PairMetrics{JobCostGB: cfg.DefaultJobCostGB}

	if tracker != nil {
		m.ErrorRate = tracker.ErrorRate(source, destination)
	}

	if st != nil {
		samples, pct := st.PairStageInPercent(24*time.Hour, source, destination)
		if samples > 0 {
			m.CostPct = pct
		}
	}

	return m
}

// limitMetrics derives control inputs for a (user,site) pair using recent stats and job runtimes.
func limitMetrics(cfg control.Config, st *state.State, tracker *stats.Tracker, user, site string) control.PairMetrics {
	m := control.PairMetrics{JobCostGB: cfg.DefaultJobCostGB}

	if tracker != nil {
		m.ErrorRate = tracker.UserSiteErrorRate(user, site)
	}

	if st != nil {
		stats := st.UserSiteStageInPercent(24*time.Hour, user, site)
		if stats.Samples > 0 {
			m.CostPct = stats.Percent
		}
	}

	return m
}
