package dcache

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type metricSet struct {
	Hit     *prometheus.CounterVec
	Latency *prometheus.HistogramVec
	Error   *prometheus.CounterVec
}

type metricHitLabel string
type metricErrLabel string

var (
	hitLabels = []string{"hit"}
	// metrics hit labels
	hitLabelMemory metricHitLabel = "mem"
	hitLabelRedis  metricHitLabel = "redis"
	hitLabelDB     metricHitLabel = "db"
	// The unit is ms.
	latencyBucket = []float64{
		1, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

	errLabels = []string{"when"}
	// metrics error labels
	errLabelSetRedis              metricErrLabel = "set_redis"
	errLabelSetMemCache           metricErrLabel = "set_mem_cache"
	errLabelInvalidate            metricErrLabel = "invalidate_error"
	errLabelMemoryUnmarshalFailed metricErrLabel = "mem_unmarshal_failed"
	errLabelRedisUnmarshalFailed  metricErrLabel = "redis_unmarshal_failed"
)

func newMetricSet(appName string) *metricSet {
	return &metricSet{
		Hit: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fmt.Sprintf("%s_dcache_hit_total", appName),
				Help: "how many hits of 3 different operations: {mem, redis, db}.",
			}, hitLabels),
		Latency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    fmt.Sprintf("%s_dcache_latency_milliseconds", appName),
				Help:    "Cache read latency in milliseconds",
				Buckets: latencyBucket,
			}, hitLabels),
		Error: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fmt.Sprintf("%s_dcache_error_total", appName),
				Help: "how many internal errors happened",
			}, errLabels),
	}
}

func (m *metricSet) Register() {
	err := prometheus.Register(m.Hit)
	if err != nil {
		log.Err(err).Msgf("failed to register prometheus Hit counters")
	}
	err = prometheus.Register(m.Latency)
	if err != nil {
		log.Err(err).Msgf("failed to register prometheus Latency histogram")
	}
	err = prometheus.Register(m.Error)
	if err != nil {
		log.Err(err).Msgf("failed to register prometheus Error counter")
	}
}

func (m *metricSet) Unregister() {
	prometheus.Unregister(m.Hit)
	prometheus.Unregister(m.Error)
	prometheus.Unregister(m.Latency)
}
