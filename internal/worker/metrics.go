package worker

import (
	"sync/atomic"
)

type PoolMetrics struct {
	jobsProcessed atomic.Int64
	jobsSucceeded atomic.Int64
	jobsFailed    atomic.Int64
	jobRetried    atomic.Int64
	jobsDead      atomic.Int64
}

func NewPoolMetrics() *PoolMetrics { return &PoolMetrics{} }

func (m *PoolMetrics) IncProcessed() { m.jobsProcessed.Add(1) }
func (m *PoolMetrics) IncSucceeded() { m.jobsSucceeded.Add(1) }
func (m *PoolMetrics) IncFailed()    { m.jobsFailed.Add(1) }
func (m *PoolMetrics) IncRetried()   { m.jobRetried.Add(1) }
func (m *PoolMetrics) IncDead()      { m.jobsDead.Add(1) }

func (m *PoolMetrics) Processed() int64 { return m.jobsProcessed.Load() }
func (m *PoolMetrics) Succeeded() int64 { return m.jobsSucceeded.Load() }
func (m *PoolMetrics) Failed() int64    { return m.jobsFailed.Load() }
func (m *PoolMetrics) Retried() int64   { return m.jobRetried.Load() }
func (m *PoolMetrics) Dead() int64      { return m.jobsDead.Load() }

func (m *PoolMetrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"processed": m.Processed(),
		"succeeded": m.Succeeded(),
		"failed":    m.Failed(),
		"retried":   m.Retried(),
		"dead":      m.Dead(),
	}
}
