package cluster

import "math"

// Phi-accrual failure detection. Rather than evicting a peer after a fixed number
// of missed heartbeats, the detector learns the distribution of intervals between
// a peer's successful heartbeats and, on each check, computes a suspicion level
//
//	phi = -log10( P(a heartbeat arrives later than the time since the last one) )
//
// phi rises smoothly as a peer goes quiet, scaled by the observed jitter: on a
// steady link a real failure crosses the threshold within a few intervals, while
// on a jittery one a transient blip is tolerated — far fewer false evictions than
// a fixed miss count (which the comment in CheckLiveness anticipated). Until a
// peer has enough samples, the caller falls back to the simple miss count.
//
// See Hayashibara et al., "The phi Accrual Failure Detector" (2004); the normal
// CDF uses Akka's logistic approximation.
const (
	// phiWindow bounds how many recent heartbeat intervals the estimator keeps,
	// so the mean and variance track changing network conditions.
	phiWindow = 200
	// phiMinSamples is how many intervals must be seen before phi is trusted;
	// below it the caller uses the consecutive-miss count instead.
	phiMinSamples = 3
	// defaultPhiThreshold is the suspicion level at which a peer is declared dead.
	// 8.0 ≈ a 10^-8 chance the peer is still alive (Akka's default).
	defaultPhiThreshold = 8.0
	// phiAcceptablePauseFactor pads the mean interval before suspicion rises, as a
	// multiple of the mean, so a brief pause (a GC, a dropped tick) is tolerated
	// without a false eviction. It scales with the tick rate automatically.
	phiAcceptablePauseFactor = 1.0
	// phiMinStdDevFactor floors the interval std-dev, as a multiple of the mean,
	// so a very regular link still tolerates jitter and phi cannot spike to
	// infinity from a near-zero variance.
	phiMinStdDevFactor = 0.5
)

// phiHistory is a bounded window of the intervals between one peer's successful
// heartbeats, with O(1) running mean and variance. Every method takes the
// current time explicitly (unix nanoseconds), so the estimator is deterministic
// and unit-testable without a real clock.
type phiHistory struct {
	intervals []float64 // ring buffer of inter-arrival intervals, in nanoseconds
	next      int       // ring write index once full
	sum       float64   // running sum of the window's intervals
	sumSq     float64   // running sum of their squares
	last      int64     // unix nano of the last recorded heartbeat (0 = none yet)
}

func newPhiHistory() *phiHistory {
	return &phiHistory{intervals: make([]float64, 0, phiWindow)}
}

// record notes a successful heartbeat at now, adding the interval since the
// previous one to the window (evicting the oldest once full). A non-advancing or
// backwards clock is ignored for the interval but still updates last.
func (h *phiHistory) record(now int64) {
	if h.last != 0 && now > h.last {
		iv := float64(now - h.last)
		if len(h.intervals) < phiWindow {
			h.intervals = append(h.intervals, iv)
		} else {
			old := h.intervals[h.next]
			h.sum -= old
			h.sumSq -= old * old
			h.intervals[h.next] = iv
			h.next = (h.next + 1) % phiWindow
		}
		h.sum += iv
		h.sumSq += iv * iv
	}
	h.last = now
}

// warm reports whether enough intervals have been seen for phi to be trusted.
func (h *phiHistory) warm() bool { return len(h.intervals) >= phiMinSamples }

func (h *phiHistory) mean() float64 { return h.sum / float64(len(h.intervals)) }

// stdDev is the window's standard deviation, floored relative to the mean so a
// near-perfectly-regular link still tolerates a little jitter.
func (h *phiHistory) stdDev() float64 {
	n := float64(len(h.intervals))
	mean := h.sum / n
	variance := h.sumSq/n - mean*mean
	if variance < 0 {
		variance = 0 // float rounding can push a tiny variance just below zero
	}
	sd := math.Sqrt(variance)
	if floor := mean * phiMinStdDevFactor; sd < floor {
		sd = floor
	}
	return sd
}

// phi returns the current suspicion level for the peer at time now: larger means
// more likely dead. It returns 0 until the history is warm (the caller then uses
// the miss count). The mean is padded by the acceptable-pause margin so a brief
// silence does not raise suspicion.
func (h *phiHistory) phi(now int64) float64 {
	if !h.warm() || h.last == 0 {
		return 0
	}
	elapsed := float64(now - h.last)
	adjMean := h.mean() * (1 + phiAcceptablePauseFactor)
	sd := h.stdDev()
	y := (elapsed - adjMean) / sd
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	var pLater float64
	if elapsed > adjMean {
		pLater = e / (1.0 + e)
	} else {
		pLater = 1.0 - 1.0/(1.0+e)
	}
	if pLater < 1e-300 {
		pLater = 1e-300 // keep Log10 finite when the peer is overwhelmingly likely dead
	}
	return -math.Log10(pLater)
}
