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
	"github.com/klauspost/compress/s2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	uuid "github.com/satori/go.uuid"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/singleflight"
)

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

	// update redis connection pool status.
	connPoolUpdateInterval = 1 * time.Second
)

// Hardcap for memory cache TTL, changeable for testing.
var defaultMemCacheMaxTTLSeconds int64 = 5
var memCacheMaxTTLHardCapSeconds int64 = 120

// compression constants
const (
	compressionThreshold = 64
	timeLen              = 4
	noCompression        = 0x0
	s2Compression        = 0x1
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

var (
	getNow = time.Now
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

// DCache implements cache.
type DCache struct {
	conn         redis.UniversalClient
	readInterval time.Duration
	group        singleflight.Group
	stats        *metricSet
	tracer       *tracer

	// enableRedisSingleFlight is a flag to enable single flight for Redis.
	// It is disabled by default since v0.3.0.
	enableRedisSingleFlight bool

	// In memory cache related
	inMemCache            *freecache.Cache
	memCacheMaxTTLSeconds int64
	pubsub                *redis.PubSub
	id                    string
	invalidateKeys        map[string]struct{}
	invalidateMu          *sync.Mutex
	invalidateCh          chan struct{}
	ctx                   context.Context
	cancel                context.CancelFunc
	wg                    sync.WaitGroup
}

type DacheOption func(*DCache)

// NewDCache creates a new cache client with in-memory cache if not @p inMemCache not nil.
// Cache MUST be explicitly closed by calling Close().
// It will also register several Prometheus metrics to the default register.
// @p readInterval specify the duration between each read per key.
func NewDCache(
	appName string,
	primaryClient redis.UniversalClient,
	inMemCache *freecache.Cache,
	readInterval time.Duration,
	enableStats bool,
	enableTracer bool,
	options ...DacheOption,
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
		conn:                    primaryClient,
		stats:                   stats,
		tracer:                  tracer,
		enableRedisSingleFlight: false,
		id:                      uuid.NewV4().String(),
		invalidateKeys:          make(map[string]struct{}),
		invalidateMu:            &sync.Mutex{},
		invalidateCh:            make(chan struct{}, invalidateChSize),
		inMemCache:              inMemCache,
		memCacheMaxTTLSeconds:   defaultMemCacheMaxTTLSeconds,
		readInterval:            readInterval,
		ctx:                     ctx,
		cancel:                  cancel,
	}
	// apply options
	for _, option := range options {
		option(c)
	}
	// start pubsub if inMemCache is enabled.
	if inMemCache != nil {
		c.pubsub = c.conn.Subscribe(ctx, redisCacheInvalidateTopic)
		c.wg.Add(2)
		go c.aggregateSend()
		go c.listenKeyInvalidate()
	}
	if enableStats {
		c.wg.Add(1)
		go c.updateMetrics()
	}
	return c, nil
}

// WithRedisSingleFlight enables single flight for Redis.
func EnableRedisSingleFlightOption(c *DCache) {
	c.enableRedisSingleFlight = true
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
	c.wg.Wait() // wait aggregateSend, listenKeyValidate and updateMetrics close.

	// unregister after all	go routines are closed.
	if c.stats != nil {
		c.stats.Unregister()
	}
}

func (c *DCache) SetMemCacheMaxTTLSeconds(ttl int64) error {
	if (ttl <= 0) || (ttl > memCacheMaxTTLHardCapSeconds) {
		return fmt.Errorf(
			"invalid ttl: %d, should be in range (0, %d]", ttl, memCacheMaxTTLHardCapSeconds)
	}
	c.memCacheMaxTTLSeconds = ttl
	return nil
}

func (c *DCache) makeHitRecorder(label metricHitLabel, startedAt time.Time) func() {
	if c.stats != nil {
		return c.stats.MakeHitObserver(label, startedAt)
	}
	return func() {}
}

func (c *DCache) recordError(label metricErrLabel) {
	if c.stats != nil {
		c.stats.ObserveError(label)
	}
}

func (c *DCache) traceHit(ctx context.Context, hit hitFrom) {
	if c.tracer != nil {
		c.tracer.TraceHitFrom(ctx, hit)
	}
}

// readValue read through using f and cache to @p key if no error and not @p noStore.
// return the marshaled bytes if no error.
func (c *DCache) readValue(
	ctx context.Context, key string, f ReadWithTtlFunc, noStore bool) ([]byte, error) {
	c.traceHit(ctx, hitDB)
	// valueTtl is an internal helper struct that bundles value and ttl.
	type valueTtl struct {
		Val any
		Ttl time.Duration
	}
	// per-pod single flight for calling @p f.
	// NOTE: This is mostly useful when user call cache layer with noCache flag, because
	// when cache is used, call to this function is protected by a distributed lock.
	rv, err, _ := c.group.Do(key, func() (any, error) {
		defer c.makeHitRecorder(hitLabelDB, getNow())()
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
			log.Ctx(ctx).Err(err).Msgf("Failed to set Redis cache for %s", key)
			c.recordError(errLabelSetRedis)
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
	c.updateMemoryCache(ctx, key, ve, isExplicitSet)
	return nil
}

// tryReadFromRedis try to read value from Redis.
func (c *DCache) tryReadFromRedis(ctx context.Context, key string) (*ValueBytesExpiredAt, error) {
	veBytes, err := c.conn.Get(ctx, storeKey(key)).Bytes()
	if err != nil {
		return nil, err
	}
	ve := &ValueBytesExpiredAt{}
	err = msgpack.Unmarshal(veBytes, ve)
	return ve, err
}

// isExplicitSet = true, calling from Set. Otherwise, value is backfilled from Redis.
func (c *DCache) updateMemoryCache(
	ctx context.Context, key string, ve *ValueBytesExpiredAt, isExplicitSet bool) {
	// update memory cache.
	// sub-second TTL will be ignored for memory cache.
	ttl := time.UnixMilli(ve.ExpiredAt).Unix() - getNow().Unix()
	if ttl > c.memCacheMaxTTLSeconds {
		ttl = c.memCacheMaxTTLSeconds
	}
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
			log.Ctx(ctx).Err(err).Msgf("Failed to set memory cache for key %s", storeKey(key))
			c.recordError(errLabelSetMemCache)
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
				log.Error().Msgf("Received invalidate payload %s", payload)
				c.recordError(errLabelInvalidate)
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

func (c *DCache) updateMetrics() {
	defer c.wg.Done()
	if c.stats == nil {
		return
	}
	ticker := time.NewTicker(connPoolUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			return
		}
		stats := c.conn.PoolStats()
		c.stats.UpdateConnPoolStatus(stats.TotalConns, stats.IdleConns)
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
	startedAt := getNow()
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
	// lookup in memory cache, return only when unmarshal succeeded.
	if c.inMemCache != nil {
		var targetBytes []byte
		targetBytes, err = c.inMemCache.Get([]byte(storeKey(key)))
		if err == nil {
			err = unmarshal(targetBytes, target)
			if err == nil {
				c.makeHitRecorder(hitLabelMemory, startedAt)()
				c.traceHit(ctx, hitMem)
				return
			} else {
				log.Ctx(ctx).Err(err).Msgf("Failed to unmarshal from memory cache for %s", key)
				c.recordError(errLabelMemoryUnmarshalFailed)
			}
		}
	}

	var anyTypedBytes any
	var targetHasUnmarshalled bool
	anyTypedBytes, err, _ = c.group.Do(lockKey(key), func() (any, error) {
		// distributed single flight to query db for value.
		for {
			ve, e := c.tryReadFromRedis(ctx, key)
			if e == nil {
				// NOTE: must check if bytes stored in Redis can be correctly
				// unmarshalled into target, because it may not when data structure changes.
				// When that happens, we will still fetch from DB.
				e = unmarshal(ve.ValueBytes, target)
				if e == nil {
					targetHasUnmarshalled = true
					// Value was retrieved from Redis, backfill memory cache and return.
					defer c.makeHitRecorder(hitLabelRedis, startedAt)()
					c.traceHit(ctx, hitRedis)
					if !noStore {
						c.updateMemoryCache(ctx, key, ve, false)
					}
					return ve.ValueBytes, nil
				} else {
					log.Ctx(ctx).Err(err).Msgf("Failed to unmarshal from Redis for %s", key)
					c.recordError(errLabelRedisUnmarshalFailed)
				}
			}
			if !c.enableRedisSingleFlight {
				// when redis single flight is disabled, we will just try to get value from DB, without
				// acquiring lock. Note that if redis is down, we will not make any attempt to get value.
				// We enforce this behavior by check if error is the redis.ErrNil.
				if e != redis.Nil {
					log.Ctx(ctx).Err(e).Msgf("Redis failed to response GET for %s", key)
					c.recordError(errLabelSetRedis)
					return nil, e
				} else {
					// Redis is okay, but value is not found, try to get value from DB.
					return c.readValue(ctx, key, read, noStore)
				}
			}
			// Redis single flight is enabled, we will try to get value from Redis, and if failed, we will
			// If failed to retrieve value from Redis, try to get a lock and query DB.
			// To avoid spamming Redis with SetNX requests, only one request should try to get
			// the lock per-pod.
			// If timeout or not cache-able error, another thread will obtain lock after sleep.
			updated, err := c.conn.SetNX(ctx, lockKey(key), "", c.readInterval).Result()
			if err != nil {
				log.Ctx(ctx).Err(err).Msgf("Failed to get lock by SetNX for %s", key)
				c.recordError(errLabelSetRedis)
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
				// TODO(yumin): we can further optimize this part by
				// check TTL of lockKey(key), and sleep wisely.
				continue
			}
		}
	})
	if err != nil {
		return
	}
	if !targetHasUnmarshalled {
		err = unmarshal(anyTypedBytes.([]byte), target)
	}
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
