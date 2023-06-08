package dcache

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type metricSet struct {
	AppName   string
	Hit       *prometheus.CounterVec
	Latency   *prometheus.HistogramVec
	Error     *prometheus.CounterVec
	RedisPool *prometheus.GaugeVec
}

type metricHitLabel string
type metricErrLabel string

var (
	hitLabels = []string{"app", "hit"}
	// metrics hit labels
	hitLabelMemory metricHitLabel = "mem"
	hitLabelRedis  metricHitLabel = "redis"
	hitLabelDB     metricHitLabel = "db"
	// The unit is ms.
	latencyBucket = []float64{
		1, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

	errLabels = []string{"app", "when"}
	// metrics error labels
	errLabelSetRedis              metricErrLabel = "set_redis"
	errLabelSetMemCache           metricErrLabel = "set_mem_cache"
	errLabelInvalidate            metricErrLabel = "invalidate_error"
	errLabelMemoryUnmarshalFailed metricErrLabel = "mem_unmarshal_failed"
	errLabelRedisUnmarshalFailed  metricErrLabel = "redis_unmarshal_failed"

	redisLabels = []string{"app", "name"}
)

func newMetricSet(appName string) *metricSet {
	return &metricSet{
		AppName: appName,
		Hit: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fmt.Sprintf("dcache_hit_total"),
				Help: "how many hits of 3 different operations: {mem, redis, db}.",
			}, hitLabels),
		Latency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    fmt.Sprintf("dcache_latency_milliseconds"),
				Help:    "Cache read latency in milliseconds",
				Buckets: latencyBucket,
			}, hitLabels),
		Error: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: fmt.Sprintf("dcache_error_total"),
				Help: "how many internal errors happened",
			}, errLabels),
		RedisPool: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: fmt.Sprintf("dcache_redis_pool"),
				Help: "redis pool status",
			}, redisLabels),
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
	err = prometheus.Register(m.RedisPool)
	if err != nil {
		log.Err(err).Msgf("failed to register prometheus RedisPool gauge")
	}
}

func (m *metricSet) Unregister() {
	prometheus.Unregister(m.Hit)
	prometheus.Unregister(m.Error)
	prometheus.Unregister(m.Latency)
	prometheus.Unregister(m.RedisPool)
}

// MakeHitObserver returns a function that can be used to observe hit by defer.
func (m *metricSet) MakeHitObserver(label metricHitLabel, startedAt time.Time) func() {
	// failing to observe hit is not a fatal error, including not successfully registered.
	return func() {
		if m.Hit != nil {
			m.Hit.WithLabelValues(m.AppName, string(label)).Inc()
		}
		if m.Latency != nil {
			m.Latency.WithLabelValues(m.AppName, string(label)).Observe(
				float64(getNow().UnixMilli() - startedAt.UnixMilli()))
		}
	}
}

// MakeErrorObserver returns a function that can be used to observe error by defer.
func (m *metricSet) ObserveError(label metricErrLabel) {
	if m.Error != nil {
		m.Error.WithLabelValues(m.AppName, string(label)).Inc()
	}
}

// UpdateConnPoolStatus updates the redis pool status.
func (m *metricSet) UpdateConnPoolStatus(totalConns, idelConns uint32) {
	if m.RedisPool != nil {
		m.RedisPool.WithLabelValues(m.AppName, "total_conns").Set(float64(totalConns))
		m.RedisPool.WithLabelValues(m.AppName, "idle_conns").Set(float64(idelConns))
	}
}
