package dcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/coocood/freecache"
	"github.com/go-redis/redis/v8"
	"github.com/klauspost/compress/s2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	uuid "github.com/satori/go.uuid"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/singleflight"
)

var _ = log.Logger

const (
	lockSuffix = "_LOCK"
	delimiter  = "~|~"

	// Duration to sleep before try to get another distributed lock for single flight.
	lockSleep = 50 * time.Millisecond

	// invalidate can support up to ~ (100 * 100 ops / second)
	// without blocking.
	redisCacheInvalidateTopic = "CacheInvalidatePubSub"
	maxInvalidate             = 100
	invalidateChSize          = 100

	// the maximum read interval to warn about inappropriately large value.
	maxReadInterval = 3 * time.Second
)

// compression constants
const (
	compressionThreshold = 64
	timeLen              = 4
	noCompression        = 0x0
	s2Compression        = 0x1
)

var (
	getNow = time.Now
)

var (
	// ErrTimeout is timeout error
	ErrTimeout = errors.New("timeout")
	// ErrInternal should never happen
	ErrInternal = errors.New("internal")
	// ErrNotPointer value passed to get functions is not a pointer.
	ErrNotPointer = errors.New("value is not a pointer")
	// ErrTypeMismatch value passed to get functions is not a pointer.
	ErrTypeMismatch = errors.New("value type mismatches cached type")
)

// SetNowFunc is a helper function to replace time.Now(), usually used for testing.
func SetNowFunc(f func() time.Time) { getNow = f }

// ReadFunc is the actual call to underlying data source
type ReadFunc = func() (any, error)

// ReadWithTtlFunc is the actual call to underlying data source while
// returning a duration as expire timer
type ReadWithTtlFunc = func() (any, time.Duration, error)

// ValueBytesExpiredAt is how we store value and expiration time to Redis.
type ValueBytesExpiredAt struct {
	ValueBytes []byte `msgpack:"v,omitempty"`
	ExpiredAt  int64  `msgpack:"e,omitempty"` // UNIX timestamp in Milliseconds.
}

// Cache interface here is only for backward compatibility.
// It is not recommended to use this interface in your app, instead, use *DCache.
type Cache interface {
	// Get will read the value from cache if exists or call read() to retrieve the value and
	// cache it in both the memory and Redis by @p ttl.
	// Inputs:
	// @p key:     Key used in cache
	// @p value:   A pointer to the memory piece of the type of the value.
	//             For example, if we are caching string, then target must be of type *string.
	//             if we caching a null-able string, using *string to represent it, then the
	//             target must be of type **string, i.e., pointer to the pointer of string.
	// @p ttl:     Expiration of cache key
	// @p read:    Actual call that hits underlying data source.
	// @p noCache: The response value will be fetched through @p read(). The new value will be
	//             cached, unless @p noStore is specified.
	// @p noStore: The response value will not be saved into the cache.
	Get(
		ctx context.Context, key string, target any, ttl time.Duration,
		read ReadFunc, noCache bool, noStore bool) error

	// GetWithTtl will read the value from cache if exists or call @p read to retrieve the value and
	// cache it in both the memory and Redis by the ttl returned in @p read.
	// Inputs:
	// @p key:     Key used in cache
	// @p value:   A pointer to the memory piece of the type of the value.
	//             For example, if we are caching string, then target must be of type *string.
	//             if we caching a null-able string, using *string to represent it, then the
	//             target must be of type **string, i.e., pointer to the pointer of string.
	// @p read:    Actual call that hits underlying data source that also returns a ttl for cache.
	// @p noCache: The response value will be fetched through @p read(). The new value will be
	//             cached, unless @p noStore is specified.
	// @p noStore: The response value will not be saved into the cache.
	GetWithTtl(
		ctx context.Context, key string, target any,
		readWithTtl ReadWithTtlFunc, noCache bool, noStore bool) error

	// Set explicitly set a cache key to a val
	// Inputs:
	// key	  - key to set
	// val	  - val to set
	// ttl    - ttl of key
	Set(ctx context.Context, key string, val any, ttl time.Duration) error

	// Invalidate explicitly invalidates a cache key
	// Inputs:
	// key    - key to invalidate
	Invalidate(ctx context.Context, key string) error

	// Close closes resources used by cache
	Close()
}

type metricSet struct {
	Hit     *prometheus.CounterVec
	Latency *prometheus.HistogramVec
	Error   *prometheus.CounterVec
}

var (
	hitLabels      = []string{"hit"}
	hitLabelMemory = "mem"
	hitLabelRedis  = "redis"
	hitLabelDB     = "db"
	// The unit is ms.
	latencyBucket = []float64{
		1, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}
	// errors
	errLabels           = []string{"when"}
	errLabelSetRedis    = "set_redis"
	errLabelSetMemCache = "set_mem_cache"
	errLabelInvalidate  = "invalidate_error"
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

// DCache implements cache.
type DCache struct {
	conn         redis.UniversalClient
	readInterval time.Duration
	group        singleflight.Group
	stats        *metricSet
	tracer       *tracer

	// In memory cache related
	inMemCache     *freecache.Cache
	pubsub         *redis.PubSub
	id             string
	invalidateKeys map[string]struct{}
	invalidateMu   *sync.Mutex
	invalidateCh   chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

var _ Cache = &DCache{}

// NewDCache creates a new cache client with in-memory cache if not @p inMemCache not nil.
// It will also register several Prometheus metrics to the default register.
// @p readInterval specify the duration between each read per key.
func NewDCache(
	appName string,
	primaryClient redis.UniversalClient,
	inMemCache *freecache.Cache,
	readInterval time.Duration,
	enableStats bool,
	enableTracer bool,
) (*DCache, error) {
	var stats *metricSet = nil
	if enableStats {
		stats = newMetricSet(appName)
		stats.Register()
	}

	var tracer *tracer = nil
	if enableTracer {
		tracer = newTracer()
	}

	if readInterval > maxReadInterval {
		log.Warn().Msgf("read interval might be too large, suggest: %s, got: %s ",
			maxReadInterval.String(), readInterval.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &DCache{
		conn:           primaryClient,
		stats:          stats,
		tracer:         tracer,
		id:             uuid.NewV4().String(),
		invalidateKeys: make(map[string]struct{}),
		invalidateMu:   &sync.Mutex{},
		invalidateCh:   make(chan struct{}, invalidateChSize),
		inMemCache:     inMemCache,
		readInterval:   readInterval,
		ctx:            ctx,
		cancel:         cancel,
	}
	if inMemCache != nil {
		c.pubsub = c.conn.Subscribe(ctx, redisCacheInvalidateTopic)
		c.wg.Add(2)
		go c.aggregateSend()
		go c.listenKeyInvalidate()
	}
	return c, nil
}

// Ping checks if the underlying redis connection is alive
func (c *DCache) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx).Err()
}

// Close terminates redis pubsub gracefully
func (c *DCache) Close() {
	if c.pubsub != nil {
		err := c.pubsub.Unsubscribe(c.ctx)
		if err != nil {
			log.Err(err).Msgf("failed to pubsub.Unsubscribe()")
		}
		err = c.pubsub.Close()
		if err != nil {
			log.Err(err).Msgf("failed to close pubsub")
		}
	}
	c.cancel()  // should be no-op because pubsub has been closed.
	c.wg.Wait() // wait aggregateSend and listenKeyValidate close.

	// unregister after all	go routines are closed.
	if c.stats != nil {
		c.stats.Unregister()
	}
}

func (c *DCache) recordLatency(label string, startedAt time.Time) func() {
	return func() {
		if c.stats != nil {
			c.stats.Latency.WithLabelValues(label).Observe(
				float64(getNow().UnixMilli() - startedAt.UnixMilli()))
		}
	}
}

// readValue read through using f and cache to @p key if no error and not @p noStore.
// return the marshaled bytes if no error.
func (c *DCache) readValue(
	ctx context.Context, key string, f ReadWithTtlFunc, noStore bool) ([]byte, error) {
	if c.tracer != nil {
		c.tracer.TraceHitFrom(ctx, hitDB)
	}
	// valueTtl is an internal helper struct that bundles value and ttl.
	type valueTtl struct {
		Val any
		Ttl time.Duration
	}
	// per-pod single flight for calling @p f.
	// NOTE: This is mostly useful when user call cache layer with noCache flag, because
	// when cache is used, call to this function is protected by a distributed lock.
	rv, err, _ := c.group.Do(key, func() (any, error) {
		defer c.recordLatency(hitLabelDB, getNow())()
		if c.stats != nil {
			c.stats.Hit.WithLabelValues(hitLabelDB).Inc()
		}
		// c.stats.
		dbres, ttl, err := f()
		return &valueTtl{
			Val: dbres,
			Ttl: ttl,
		}, err
	})
	if err != nil {
		return nil, err
	}
	valTtl := rv.(*valueTtl)
	valueBytes, err := marshal(valTtl.Val)
	if err != nil {
		return nil, err
	}
	if !noStore {
		// If failed to set cache, we do not return error because value has been
		// successfully retrieved.
		err := c.setKey(ctx, key, valueBytes, valTtl.Ttl, false)
		if err != nil {
			log.Err(err).Msgf("Failed to set Redis cache for %s", key)
			if c.stats != nil {
				c.stats.Error.WithLabelValues(errLabelSetRedis).Inc()
			}
		}
	}
	return valueBytes, nil
}

// setKey set key in redis and inMemCache
func (c *DCache) setKey(ctx context.Context, key string, valueBytes []byte, ttl time.Duration, isExplicitSet bool) error {
	ve := &ValueBytesExpiredAt{
		ValueBytes: valueBytes,
		ExpiredAt:  getNow().Add(ttl).UnixMilli(),
	}
	veBytes, err := msgpack.Marshal(ve)
	if err != nil {
		return err
	}
	err = c.conn.Set(ctx, storeKey(key), veBytes, ttl).Err()
	if err != nil {
		return err
	}
	c.updateMemoryCache(key, ve, isExplicitSet)
	return nil
}

// isExplicitSet = true, calling from Set. Otherwise, value is backfilled from Redis.
func (c *DCache) updateMemoryCache(key string, ve *ValueBytesExpiredAt, isExplicitSet bool) {
	// update memory cache.
	// sub-second TTL will be ignored for memory cache.
	ttl := time.UnixMilli(ve.ExpiredAt).Unix() - getNow().Unix()
	if c.inMemCache != nil && ttl > 0 {
		memValue, err := c.inMemCache.Get([]byte(storeKey(key)))
		// Broadcast invalidation request only when value is explicitly set to new one,
		// by Set(), instead of backfilled from Redis, and if
		// (1) The value does not exist before
		//     so that we do not know if the new value will make any difference, or
		// (2) we have value cached before and they are different from new value.
		if isExplicitSet {
			if err == freecache.ErrNotFound ||
				(err == nil && !bytes.Equal(ve.ValueBytes, memValue)) {
				c.broadcastKeyInvalidate(storeKey(key))
			}
		}
		// ignore in memory cache error
		err = c.inMemCache.Set([]byte(storeKey(key)), ve.ValueBytes, int(ttl))
		if err != nil {
			log.Err(err).Msgf("Failed to set memory cache for key %s", storeKey(key))
			if c.stats != nil {
				c.stats.Error.WithLabelValues(errLabelSetMemCache).Inc()
			}
		}
	}
}

// deleteKey delete key in redis and inMemCache
func (c *DCache) deleteKey(ctx context.Context, key string) error {
	n, err := c.conn.Del(ctx, storeKey(key)).Result()
	if err != nil {
		return err
	}
	if n > 0 {
		if c.inMemCache != nil {
			c.inMemCache.Del([]byte(storeKey(key)))
			c.broadcastKeyInvalidate(key)
		}
	}
	return nil
}

// broadcastKeyInvalidate pushes key into a list and wait for broadcast
func (c *DCache) broadcastKeyInvalidate(key string) {
	c.invalidateMu.Lock()
	c.invalidateKeys[storeKey(key)] = struct{}{}
	l := len(c.invalidateKeys)
	c.invalidateMu.Unlock()
	if l == maxInvalidate {
		c.invalidateCh <- struct{}{}
	}
}

// aggregateSend waits for 1 seconds or list accumulating more than maxInvalidate
// to send to redis pubsub
func (c *DCache) aggregateSend() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-c.invalidateCh:
		case <-c.ctx.Done():
			return
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.invalidateMu.Lock()
			if len(c.invalidateKeys) == 0 {
				c.invalidateMu.Unlock()
				return
			}
			toSend := c.invalidateKeys
			c.invalidateKeys = make(map[string]struct{})
			c.invalidateMu.Unlock()
			keys := make([]string, 0)
			for key := range toSend {
				keys = append(keys, key)
			}
			msg := c.id + delimiter + strings.Join(keys, delimiter)
			c.conn.Publish(c.ctx, redisCacheInvalidateTopic, msg)
		}()
	}
}

// listenKeyInvalidate subscribe to invalidate key requests and invalidates memory cache.
func (c *DCache) listenKeyInvalidate() {
	defer c.wg.Done()
	ch := c.pubsub.Channel()
	for {
		msg, ok := <-ch
		if !ok {
			return
		}
		payload := msg.Payload
		c.wg.Add(1)
		go func(payload string) {
			defer c.wg.Done()
			l := strings.Split(payload, delimiter)
			if len(l) < 2 {
				// Invalid payload
				log.Warn().Msgf("Received invalidate payload %s", payload)

				if c.stats != nil {
					c.stats.Error.WithLabelValues(errLabelInvalidate).Inc()
				}
				return
			}
			if l[0] == c.id {
				// Receive message from self
				return
			}
			// Invalidate key
			for _, key := range l[1:] {
				c.inMemCache.Del([]byte(key))
			}
		}(payload)
	}
}

func storeKey(key string) string {
	return fmt.Sprintf(":{%s}", key)
}

func lockKey(key string) string {
	return fmt.Sprintf(":%s%s", storeKey(key), lockSuffix)
}

// Get will read the value from cache if exists or call read() to retrieve the value and
// cache it in both the memory and Redis by @p ttl.
// Inputs:
// @p key:     Key used in cache
// @p value:   A pointer to the memory piece of the type of the value.
//
//	For example, if we are caching string, then target must be of type *string.
//	if we caching a null-able string, using *string to represent it, then the
//	target must be of type **string, i.e., pointer to the pointer of string.
//
// @p ttl:     Expiration of cache key
// @p read:    Actual call that hits underlying data source.
// @p noCache: The response value will be fetched through @p read(). The new value will be
//
//	cached, unless @p noStore is specified.
//
// @p noStore: The response value will not be saved into the cache.
func (c *DCache) Get(ctx context.Context, key string, target any, expire time.Duration, read ReadFunc, noCache bool, noStore bool) error {
	readWithTtl := func() (any, time.Duration, error) {
		res, err := read()
		return res, expire, err
	}
	return c.GetWithTtl(ctx, key, target, readWithTtl, noCache, noStore)
}

// GetWithTtl will read the value from cache if exists or call @p read to retrieve the value and
// cache it in both the memory and Redis by the ttl returned in @p read.
// Inputs:
// @p key:     Key used in cache
// @p value:   A pointer to the memory piece of the type of the value.
//
//	For example, if we are caching string, then target must be of type *string.
//	if we caching a null-able string, using *string to represent it, then the
//	target must be of type **string, i.e., pointer to the pointer of string.
//
// @p read:    Actual call that hits underlying data source that also returns a ttl for cache.
// @p noCache: The response value will be fetched through @p read(). The new value will be
//
//	cached, unless @p noStore is specified.
//
// @p noStore: The response value will not be saved into the cache.
func (c *DCache) GetWithTtl(ctx context.Context, key string, target any, read ReadWithTtlFunc, noCache bool, noStore bool) (err error) {
	if c.tracer != nil {
		ctx = c.tracer.TraceStart(ctx,
			"GetWithTtl",
			[]string{
				fmt.Sprintf("key=%s", key),
				fmt.Sprintf("noCache=%v", noCache),
				fmt.Sprintf("noStore=%v", noStore),
			})
		defer c.tracer.TraceEnd(ctx, err)
	}

	if noCache {
		var targetBytes []byte
		targetBytes, err = c.readValue(ctx, key, read, noStore)
		if err != nil {
			return
		}
		err = unmarshal(targetBytes, target)
		return
	}
	// lookup in memory cache.
	if c.inMemCache != nil {
		var targetBytes []byte
		targetBytes, err = c.inMemCache.Get([]byte(storeKey(key)))
		if err == nil {
			if c.stats != nil {
				c.stats.Hit.WithLabelValues(hitLabelMemory).Inc()
			}
			if c.tracer != nil {
				c.tracer.TraceHitFrom(ctx, hitMem)
			}
			err = unmarshal(targetBytes, target)
			return
		}
	}

	var anyTypedBytes any
	anyTypedBytes, err, _ = c.group.Do(lockKey(key), func() (any, error) {
		// distributed single flight to query db for value.
		startedAt := getNow()
		for {
			ve := &ValueBytesExpiredAt{}
			veBytes, e := c.conn.Get(ctx, storeKey(key)).Bytes()
			if e == nil {
				e = msgpack.Unmarshal(veBytes, ve)
			}
			if e == nil {
				// Value was retrieved from Redis, backfill memory cache and return.
				if c.stats != nil {
					c.stats.Hit.WithLabelValues(hitLabelRedis).Inc()
				}
				if c.tracer != nil {
					c.tracer.TraceHitFrom(ctx, hitRedis)
				}
				c.recordLatency(hitLabelRedis, startedAt)
				if !noStore {
					c.updateMemoryCache(key, ve, false)
				}
				return ve.ValueBytes, nil
			}
			// If failed to retrieve value from Redis, try to get a lock and query DB.
			// To avoid spamming Redis with SetNX requests, only one request should try to get
			// the lock per-pod.
			// If timeout or not cache-able error, another thread will obtain lock after sleep.
			updated, err := c.conn.SetNX(ctx, lockKey(key), "", c.readInterval).Result()
			if err != nil {
				if c.stats != nil {
					c.stats.Error.WithLabelValues(errLabelSetRedis).Inc()
				}
			}
			if updated {
				return c.readValue(ctx, key, read, noStore)
			}
			// Did not obtain lock, sleep and retry to wait for update
			select {
			case <-ctx.Done():
				// NOTE: for requests grouped into one flight, if the earliest request
				// timeout, all of them will timeout.
				return nil, ErrTimeout
			case <-time.After(lockSleep):
				continue
			}
		}
	})
	if err != nil {
		return
	}
	err = unmarshal(anyTypedBytes.([]byte), target)
	return
}

// Invalidate explicitly invalidates a cache key
// Inputs:
// key    - key to invalidate
func (c *DCache) Invalidate(ctx context.Context, key string) (err error) {
	if c.tracer != nil {
		ctx = c.tracer.TraceStart(ctx, "Invalidate", []string{fmt.Sprintf("key=%s", key)})
		defer c.tracer.TraceEnd(ctx, nil)
	}
	err = c.deleteKey(ctx, key)
	return
}

// Set explicitly set a cache key to a val
// Inputs:
// key	  - key to set
// val	  - val to set
// ttl    - ttl of key
func (c *DCache) Set(ctx context.Context, key string, val any, ttl time.Duration) (err error) {
	if c.tracer != nil {
		ctx = c.tracer.TraceStart(ctx, "Set",
			[]string{
				fmt.Sprintf("key=%s", key),
				fmt.Sprintf("ttl=%s", ttl),
			})
		defer c.tracer.TraceEnd(ctx, err)
	}
	bs, err := marshal(val)
	if err != nil {
		return
	}
	err = c.setKey(ctx, key, bs, ttl, true)
	return
}

// compress data with s2. Add 1 suffix byte to indicate if it is cached.
func compress(data []byte) []byte {
	if len(data) < compressionThreshold {
		n := len(data) + 1
		b := make([]byte, n, n+timeLen)
		copy(b, data)
		b[len(b)-1] = noCompression
		return b
	}

	n := s2.MaxEncodedLen(len(data)) + 1
	b := make([]byte, n, n+timeLen)
	b = s2.Encode(b, data)
	b = append(b, s2Compression)
	return b
}

// marshal @p value into returned bytes, with compression.
// copy from https://github.com/go-redis/cache/blob/v8/cache.go
func marshal(value interface{}) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return nil, err
	}
	return compress(b), nil
}

// unmarshal @p b into @p value.
// copy from https://github.com/go-redis/cache/blob/v8/cache.go
func unmarshal(b []byte, value interface{}) error {
	if reflect.ValueOf(value).Kind() != reflect.Ptr {
		return ErrNotPointer
	}
	if len(b) == 0 {
		// if we cache nil or any zero value, must set *value to
		// the same zero value.
		v := reflect.ValueOf(value)
		if v.Elem().CanSet() {
			v.Elem().Set(reflect.Zero(v.Elem().Type()))
		} else {
			return ErrTypeMismatch
		}
		return nil
	}
	switch value := value.(type) {
	case nil:
		return nil
	case *[]byte:
		clone := make([]byte, len(b))
		copy(clone, b)
		*value = clone
		return nil
	case *string:
		*value = string(b)
		return nil
	}

	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]

		var err error
		b, err = s2.Decode(nil, b)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown compression method: %x", c)
	}

	return msgpack.Unmarshal(b, value)
}
