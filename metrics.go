package dcache

import (
	"errors"
	"sync"
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

	registered bool
}

type metricHitLabel string
type metricErrLabel string

// labelApp is the label that carries the cache's app name on every metric.
const labelApp = "app"

var (
	hitLabels = []string{labelApp, "hit"}
	// metrics hit labels
	hitLabelMemory metricHitLabel = "mem"
	hitLabelRedis  metricHitLabel = "redis"
	hitLabelDB     metricHitLabel = "db"
	// The unit is ms.
	latencyBucket = []float64{
		1, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

	errLabels = []string{labelApp, "when"}
	// metrics error labels
	errLabelSetRedis              metricErrLabel = "set_redis"
	errLabelGetRedis              metricErrLabel = "get_redis"
	errLabelPublishInvalidate     metricErrLabel = "publish_invalidate"
	errLabelSetMemCache           metricErrLabel = "set_mem_cache"
	errLabelInvalidate            metricErrLabel = "invalidate_error"
	errLabelMemoryUnmarshalFailed metricErrLabel = "mem_unmarshal_failed"
	errLabelRedisUnmarshalFailed  metricErrLabel = "redis_unmarshal_failed"

	redisLabels = []string{labelApp, "name"}
)

// The default registry identifies collectors by metric name, so all DCache
// instances in a process share one set of collectors, labeled by app.
// The collectors are registered by the first instance and unregistered when
// the last instance closes.
var (
	sharedMu   sync.Mutex
	shared     *sharedCollectors
	sharedRefs int
)

type sharedCollectors struct {
	hit       *prometheus.CounterVec
	latency   *prometheus.HistogramVec
	err       *prometheus.CounterVec
	redisPool *prometheus.GaugeVec
	// owned are the collectors dcache registered itself. Collectors already
	// registered by other code under the same names are used but never
	// unregistered by dcache.
	owned []prometheus.Collector
}

// registerOrReuse registers c, or returns the collector already registered
// under the same name.
func registerOrReuse[T prometheus.Collector](c T, owned *[]prometheus.Collector) T {
	err := prometheus.Register(c)
	if err == nil {
		*owned = append(*owned, c)
		return c
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(T); ok {
			return existing
		}
	}
	log.Err(err).Msg("failed to register dcache metric; its observations are dropped")
	return c
}

func acquireCollectors() *sharedCollectors {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if shared == nil {
		s := &sharedCollectors{}
		s.hit = registerOrReuse(prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dcache_hit_total",
				Help: "how many hits of 3 different operations: {mem, redis, db}.",
			}, hitLabels), &s.owned)
		s.latency = registerOrReuse(prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "dcache_latency_milliseconds",
				Help:    "Cache read latency in milliseconds",
				Buckets: latencyBucket,
			}, hitLabels), &s.owned)
		s.err = registerOrReuse(prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dcache_error_total",
				Help: "how many internal errors happened",
			}, errLabels), &s.owned)
		s.redisPool = registerOrReuse(prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dcache_redis_pool",
				Help: "redis pool status",
			}, redisLabels), &s.owned)
		shared = s
	}
	sharedRefs++
	return shared
}

func releaseCollectors() {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedRefs == 0 {
		return
	}
	sharedRefs--
	if sharedRefs == 0 {
		for _, c := range shared.owned {
			prometheus.Unregister(c)
		}
		shared = nil
	}
}

func newMetricSet(appName string) *metricSet {
	return &metricSet{AppName: appName}
}

// Register attaches the metric set to the process-wide dcache collectors.
func (m *metricSet) Register() {
	if m.registered {
		return
	}
	s := acquireCollectors()
	m.Hit, m.Latency, m.Error, m.RedisPool = s.hit, s.latency, s.err, s.redisPool
	m.registered = true
}

// Unregister detaches the metric set. It drops this app's connection-pool
// gauges, which would otherwise go stale, and unregisters the collectors when
// no DCache uses them anymore. It is idempotent.
func (m *metricSet) Unregister() {
	if !m.registered {
		return
	}
	m.RedisPool.DeleteLabelValues(m.AppName, "total_conns")
	m.RedisPool.DeleteLabelValues(m.AppName, "idle_conns")
	m.registered = false
	releaseCollectors()
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
				float64(getNow().Sub(startedAt)) / float64(time.Millisecond))
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
