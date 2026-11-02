package dcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coocood/freecache"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	dbResponseTime = 100 * time.Millisecond
	waitTime       = 10 * time.Millisecond
)

type testSuite struct {
	suite.Suite
	redisConn   redis.UniversalClient
	inMemCache  *freecache.Cache
	cacheRepo   *DCache
	inMemCache2 *freecache.Cache
	cacheRepo2  *DCache
	// redis single flight enabled cache
	inMemCacheSf1 *freecache.Cache
	cacheRepoSf1  *DCache
	inMemCacheSf2 *freecache.Cache
	cacheRepoSf2  *DCache
	mockRepo      dummyMock
}

type dummyMock struct {
	mock.Mock
}

// ReadThrough
func (_m *dummyMock) ReadThrough() (interface{}, error) {
	ret := _m.Called()
	// Emulate db response time
	time.Sleep(dbResponseTime)

	r0 := ret.Get(0)

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(error)
		}
	}

	return r0, r1
}

// ReadThroughWithExpire
func (_m *dummyMock) ReadThroughWithExpire() (interface{}, time.Duration, error) {
	ret := _m.Called()
	// Emulate db response time
	time.Sleep(dbResponseTime)

	r0 := ret.Get(0)

	var r1 time.Duration
	if rf, ok := ret.Get(1).(func() time.Duration); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(time.Duration)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		if ret.Get(2) != nil {
			r2 = ret.Get(2).(error)
		}
	}

	return r0, r1, r2
}

// WriteThrough
func (_m *dummyMock) WriteThrough() (interface{}, error) {
	ret := _m.Called()
	// Emulate db response time
	time.Sleep(dbResponseTime)

	r0 := ret.Get(0)

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(error)
		}
	}

	return r0, r1
}

func newTestSuite() *testSuite {
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("127.0.0.1:6379"),
		DB:   10,
	})
	appName := "test"
	// max value size is: 100MB / 1024 = 100KB
	inMemCache := freecache.NewCache(100 * 1024 * 1024)
	cacheRepo, e := NewDCache(appName, redisClient, inMemCache, time.Second, true, true)
	if e != nil {
		panic(e)
	}
	inMemCache2 := freecache.NewCache(1024 * 1024)
	cacheRepo2, e := NewDCache(appName, redisClient, inMemCache2, time.Second, false, true)
	if e != nil {
		panic(e)
	}
	// max value size is: 100MB / 1024 = 100KB
	inMemCacheSf1 := freecache.NewCache(1024 * 1024)
	cacheRepoSf1, e := NewDCache(appName, redisClient, inMemCacheSf1, time.Second, true, true, EnableRedisSingleFlightOption)
	if e != nil {
		panic(e)
	}
	inMemCacheSf2 := freecache.NewCache(1024 * 1024)
	cacheRepoSf2, e := NewDCache(appName, redisClient, inMemCacheSf2, time.Second, true, true, EnableRedisSingleFlightOption)
	if e != nil {
		panic(e)
	}
	return &testSuite{
		redisConn:     redisClient,
		cacheRepo:     cacheRepo,
		inMemCache:    inMemCache,
		cacheRepo2:    cacheRepo2,
		inMemCache2:   inMemCache2,
		inMemCacheSf1: inMemCacheSf1,
		cacheRepoSf1:  cacheRepoSf1,
		inMemCacheSf2: inMemCacheSf2,
		cacheRepoSf2:  cacheRepoSf2,
	}
}

func TestRepoTestSuite(t *testing.T) {
	suite.Run(t, newTestSuite())
}

func (suite *testSuite) BeforeTest(_, _ string) {
	suite.inMemCache.Clear()
	suite.inMemCache2.Clear()
	suite.inMemCacheSf1.Clear()
	suite.inMemCacheSf2.Clear()
	suite.Require().NoError(suite.redisConn.FlushAll(context.Background()).Err())
}

func (suite *testSuite) AfterTest(_, _ string) {
	suite.mockRepo.AssertExpectations(suite.T())
}

func (suite *testSuite) TearDownSuite() {
	suite.cacheRepo.Close()
	suite.cacheRepo2.Close()
	suite.cacheRepoSf1.Close()
	suite.cacheRepoSf2.Close()
}

func (suite *testSuite) encodeByte(value interface{}) []byte {
	switch value := value.(type) {
	case nil:
		return nil
	case []byte:
		return value
	case string:
		return []byte(value)
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return nil
	}

	return b
}

func (suite *testSuite) TestCloseCache() {
	cache1, e := NewDCache("test", suite.redisConn, nil, time.Second, false, false)
	suite.Require().Nil(e)
	cache1.Close()

	cache1, e = NewDCache("test", suite.redisConn, nil, time.Second, true, false)
	suite.Require().Nil(e)
	cache1.Close()

	cache1, e = NewDCache("test", suite.redisConn, nil, time.Second, false, true)
	suite.Require().Nil(e)
	cache1.Close()

	cache1, e = NewDCache("test", suite.redisConn, nil, time.Second, true, true)
	suite.Require().Nil(e)
	cache1.Close()
}

func (suite *testSuite) TestPopulateCache() {
	ctx := context.Background()
	queryKey := "test"
	v := "testvalue"
	ev := suite.encodeByte(v)
	var vget string
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	err := suite.cacheRepo.Get(
		context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget)

	// Second call should not hit db
	err = suite.cacheRepo.Get(
		context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget)

	redisBytes, err := suite.redisConn.Get(ctx, suite.cacheRepo.storeKey(queryKey)).Bytes()
	suite.Require().NoError(err)
	vredis := &ValueBytesExpiredAt{}
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev, vredis.ValueBytes)

	vinmem, e := suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey)))
	suite.Require().NoError(e)
	suite.Equal(ev, vinmem)

	// Second pod should not hit db either
	var vget2 string
	err = suite.cacheRepo2.Get(context.Background(), queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget2)

	vinmem2, e := suite.inMemCache2.Get([]byte(suite.cacheRepo.storeKey(queryKey)))
	suite.NoError(e)
	suite.Equal(ev, vinmem2)
}

type data struct {
	S string
	I int
}

func (suite *testSuite) TestCachePenetrationNil() {
	ctx := context.Background()
	queryKey := "test"
	var vget *data
	suite.mockRepo.On("ReadThrough").Return(nil, nil).Once()
	err := suite.cacheRepo.Get(
		ctx, queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal((*data)(nil), vget)

	// second time should not penetrate cache.
	err = suite.cacheRepo.Get(
		ctx, queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal((*data)(nil), vget)
}

func (suite *testSuite) TestCachePenetrationStringZeroValue() {
	ctx := context.Background()
	queryKey := "test"
	var vget = "abc"
	suite.mockRepo.On("ReadThrough").Return("", nil).Once()
	err := suite.cacheRepo.Get(
		ctx, queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal("", vget)

	// second time should not penetrate cache.
	err = suite.cacheRepo.Get(
		ctx, queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal("", vget)
}

func (suite *testSuite) TestCacheCompressionRate() {
	var expected []data
	for i := 0; i < 10000; i++ {
		expected = append(expected, data{
			fmt.Sprintf("long string worth compression: %d", i),
			i,
		})
	}
	compressed, err := marshal(expected)
	suite.Require().Nil(err)
	original, err := msgpack.Marshal(expected)
	suite.Require().Nil(err)
	suite.LessOrEqual(len(compressed), len(original))
	fmt.Printf("compression space saving rate: %.2f%s\n",
		100-100*float64(len(compressed))/float64(len(original)), "%")
}

func (suite *testSuite) TestCacheCompressionValue() {
	ctx := context.Background()
	queryKey := "test"
	var vget []data
	var expected []data
	for i := 0; i < 10000; i++ {
		expected = append(expected, data{
			fmt.Sprintf("long string worth compression: %d", i),
			i,
		})
	}
	suite.mockRepo.On("ReadThrough").Return(expected, nil).Once()
	err := suite.cacheRepo.Get(
		ctx, queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal(expected, vget)
}

func (suite *testSuite) TestCachedNilMustOverwritePointerTarget() {
	ctx := context.Background()
	queryKey := "test"
	vget := new(string)
	*vget = "old_value"
	suite.mockRepo.On("ReadThrough").Return(nil, nil).Once()
	err := suite.cacheRepo.Get(
		ctx, queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)

	// This is a known issue for now...
	suite.Equal((*string)(nil), vget)
}

func (suite *testSuite) TestPopulateCacheWithExpire() {
	ctx := context.Background()
	queryKey1 := "test1"
	queryKey2 := "test2"
	v1 := "testvalue1s"
	v2 := "testvalue2s"
	// XXX(yumin): using 1 second will make this test interestingly flaky due
	// to a precision issue. If you change v1ct back to 1 second and do
	// `$ while go test -v ; do :; done`
	// after some time, you will get a test failure when wall time is something like
	// xx:yy:22.950. The reason is because of the precision of freecache is of seconds,
	// and when you set value with TTL of 1 second,
	// it will record the expiration time to be xx::yy::23, which means that the actual
	// TTL is 50ms, instead of 1 second.
	v1ct := time.Second * 2
	v2ct := time.Second * 3
	ev1 := suite.encodeByte(v1)
	ev2 := suite.encodeByte(v2)

	var vget1, vget2 string
	suite.mockRepo.On("ReadThroughWithExpire").Return(v1, v1ct, nil).Once()
	err := suite.cacheRepo.GetWithTtl(context.Background(), queryKey1, &vget1, func() (interface{}, time.Duration, error) {
		return suite.mockRepo.ReadThroughWithExpire()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v1, vget1)

	suite.mockRepo.On("ReadThroughWithExpire").Return(v2, v2ct, nil).Once()
	err = suite.cacheRepo.GetWithTtl(context.Background(), queryKey2, &vget2, func() (interface{}, time.Duration, error) {
		return suite.mockRepo.ReadThroughWithExpire()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v2, vget2)

	// get v1
	redisBytes, err := suite.redisConn.Get(ctx, suite.cacheRepo.storeKey(queryKey1)).Bytes()
	suite.Require().NoError(err)
	vredis := &ValueBytesExpiredAt{}
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev1, vredis.ValueBytes)

	vinmem, e := suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey1)))
	suite.NoError(e)
	suite.Equal(ev1, vinmem)

	// get v2
	redisBytes, err = suite.redisConn.Get(ctx, suite.cacheRepo.storeKey(queryKey2)).Bytes()
	suite.Require().NoError(err)
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev2, vredis.ValueBytes)

	vinmem, e = suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey2)))
	suite.NoError(e)
	suite.Equal(ev2, vinmem)

	time.Sleep(time.Second * 2)

	// get v1, not exist
	redisExist := suite.redisConn.Exists(ctx, suite.cacheRepo.storeKey(queryKey1)).Val()
	suite.EqualValues(redisExist, 0)

	_, e = suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey1)))
	suite.Error(e)

	// get v2
	redisBytes, err = suite.redisConn.Get(ctx, suite.cacheRepo.storeKey(queryKey2)).Bytes()
	suite.Require().NoError(err)
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev2, vredis.ValueBytes)

	vinmem, e = suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey2)))
	suite.NoError(e)
	suite.Equal(ev2, vinmem)
}

func (suite *testSuite) TestNotCachingError() {
	queryKey := "test"
	v := ""
	// Not cacheable error
	e := errors.New("newerror")
	// Should hit db twice
	suite.mockRepo.On("ReadThrough").Return(v, e).Twice()
	var vget string
	err := suite.cacheRepo.Get(
		context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.Equal(e, err)
	suite.Equal(v, vget)

	// The second time should return err from cache not db
	err = suite.cacheRepo.Get(
		context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.Equal(e, err)
	suite.Equal(v, vget)
}

func (suite *testSuite) TestConcurrentReadWaitSingleFlight() {
	queryKey := "test"
	v := "testvalue"
	// Only one pod should hit db
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var vget string
		err := suite.cacheRepoSf1.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
		suite.NoError(err)
		suite.Equal(v, vget)
	}()
	var vget2 string
	err := suite.cacheRepoSf2.Get(context.Background(), queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget2)
	wg.Wait()
}

func (suite *testSuite) TestConcurrentReadWaitTimeout() {
	queryKey := "test"
	v := "testvalue"
	// Only one pod should hit db
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var vget string
		err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
		suite.NoError(err)
		suite.Equal(v, vget)
	}()
	// Make sure cache2 is called later and timeout is within db response time
	time.Sleep(dbResponseTime / 10)
	ctx, cancel := context.WithCancel(context.Background())
	// cancel the context
	cancel()
	var vget2 string
	err := suite.cacheRepo2.Get(ctx, queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	// Should get timeout error
	suite.Error(err)
	wg.Wait()
}

type Dummy struct {
	A int
	B int
}

func (suite *testSuite) TestCacheDifferentType() {

	v1 := int32(10)
	var v1get int32
	queryKey := "test1"
	suite.mockRepo.On("ReadThrough").Return(v1, nil).Once()

	err := suite.cacheRepo.Get(context.Background(), queryKey, &v1get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v1, v1get)
	time.Sleep(waitTime)
	// Second call should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &v1get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v1, v1get)

	v2 := true
	var v2get bool
	queryKey = "test2"
	suite.mockRepo.On("ReadThrough").Return(v2, nil).Once()

	err = suite.cacheRepo.Get(context.Background(), queryKey, &v2get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v2, v2get)
	time.Sleep(waitTime)
	// Second call should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &v2get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v2, v2get)

	v3 := Dummy{
		A: 1,
		B: 3,
	}
	var v3get Dummy
	queryKey = "test3"
	suite.mockRepo.On("ReadThrough").Return(v3, nil).Once()

	err = suite.cacheRepo.Get(context.Background(), queryKey, &v3get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v3, v3get)
	time.Sleep(waitTime)
	// Second call should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &v3get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v3, v3get)

	v4 := &Dummy{
		A: 1,
		B: 3,
	}
	var v4get Dummy
	queryKey = "test4"
	suite.mockRepo.On("ReadThrough").Return(v4, nil).Once()

	err = suite.cacheRepo.Get(context.Background(), queryKey, &v4get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v4, &v4get)
	time.Sleep(waitTime)
	// Second call should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &v4get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v4, &v4get)

	v5 := []*Dummy{{A: 13}, {B: 2332}, {A: 13, B: 8921384}}
	var v5get []*Dummy
	queryKey = "test5"
	suite.mockRepo.On("ReadThrough").Return(v5, nil).Once()

	err = suite.cacheRepo.Get(context.Background(), queryKey, &v5get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v5, v5get)
	time.Sleep(waitTime)
	// Second call should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &v5get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v5, v5get)

	v6 := &[]*Dummy{{A: 13}, {B: 2332}, {A: 13, B: 8921384}}
	var v6get []*Dummy
	queryKey = "test6"
	suite.mockRepo.On("ReadThrough").Return(v6, nil).Once()

	err = suite.cacheRepo.Get(context.Background(), queryKey, &v6get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v6, &v6get)
	time.Sleep(waitTime)
	// Second call should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &v6get, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v6, &v6get)

}

func (suite *testSuite) TestDecodeToNil() {
	v := &Dummy{A: 4}
	queryKey := "tonil"
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	vget := (*Dummy)(nil)
	err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v, vget)

	time.Sleep(waitTime)
	// Second call should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.EqualValues(v, vget)
}

func (suite *testSuite) TestConcurrentReadAfterExpireSingleFlight() {
	queryKey := "test"
	v := "testvalueold"
	// Only one pod should hit db
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	var vget string
	err := suite.cacheRepoSf1.Get(context.Background(), queryKey, &vget, time.Second, func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget)

	var wg sync.WaitGroup

	// Wait for expire
	time.Sleep(time.Second * 2)
	// Change return
	newv := "testvaluenew"
	suite.mockRepo.On("ReadThrough").Return(newv, nil).Once()
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Use a separate target: the main goroutine reads concurrently.
		var vgetAsync string
		err := suite.cacheRepoSf1.Get(context.Background(), queryKey, &vgetAsync, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
		suite.NoError(err)
		suite.Equal(newv, vgetAsync)
	}()
	// Make sure cache2 is called later and timeout is within db response time
	time.Sleep(dbResponseTime / 2)
	err = suite.cacheRepoSf2.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	// The slower thread should return old cache value without wait
	suite.Equal(newv, vget)

	time.Sleep(dbResponseTime)
	// Should get newv afterwards
	err = suite.cacheRepoSf1.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(newv, vget)
	wg.Wait()
}

func (suite *testSuite) TestInvalidate() {
	queryKey := "test"
	v := "testvalueold"
	// Only one pod should hit db
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	var vget string
	err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget)

	// Wait for key to be created
	time.Sleep(waitTime)
	err = suite.cacheRepo.Invalidate(context.Background(), queryKey)
	suite.NoError(err)

	// Wait for key to be deleted
	time.Sleep(waitTime)
	exist, e := suite.redisConn.Exists(context.Background(), suite.cacheRepo.storeKey(queryKey)).Result()
	suite.NoError(e)
	suite.EqualValues(0, exist)

	_, e = suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e)
}

func (suite *testSuite) TestSet() {
	queryKey := "test"
	v := "testvalueold"
	// Only one pod should hit db
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	var vget string
	err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget)

	newv := "testvaluenew"
	newve := suite.encodeByte(newv)
	err = suite.cacheRepo.Set(context.Background(), queryKey, newv, Normal.ToDuration())
	suite.NoError(err)

	redisBytes, err := suite.redisConn.Get(context.Background(), suite.cacheRepo.storeKey(queryKey)).Bytes()
	suite.Require().NoError(err)
	vredis := &ValueBytesExpiredAt{}
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(newve, vredis.ValueBytes)

	vinmem, e := suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey)))
	suite.Require().NoError(e)
	suite.Equal(newve, vinmem)

	// Should not hit db
	err = suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(newv, vget)
}

func (suite *testSuite) TestInvalidateKeyAcrossPods() {
	queryKey := "test"
	v := "testvalueold"
	ve := suite.encodeByte(v)
	// Only one pod should hit db
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	var vget string
	err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget)

	time.Sleep(waitTime)
	var vget2 string
	err = suite.cacheRepo2.Get(context.Background(), queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget2)

	vinmem, e := suite.inMemCache2.Get([]byte(suite.cacheRepo2.storeKey(queryKey)))
	suite.NoError(e)
	suite.Equal(ve, vinmem)

	vinmem, e = suite.inMemCache2.Get([]byte(suite.cacheRepo2.storeKey(queryKey)))
	suite.NoError(e)
	suite.Equal(ve, vinmem)

	time.Sleep(waitTime)
	err = suite.cacheRepo.Invalidate(context.Background(), queryKey)
	suite.NoError(err)

	// Wait for key to be broadcasted
	time.Sleep(time.Second)
	exist, e := suite.redisConn.Exists(context.Background(), suite.cacheRepo.storeKey(queryKey)).Result()
	suite.NoError(e)
	suite.EqualValues(0, exist)

	_, e = suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e)

	// check inmemcache of second pod is invalidated too
	_, e = suite.inMemCache2.Get([]byte(suite.cacheRepo2.storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e)
}

func (suite *testSuite) TestSetMemCacheMaxTTlSeconds() {
	suite.Require().Nil(suite.cacheRepo.SetMemCacheMaxTTLSeconds(1))
	suite.Require().Error(suite.cacheRepo.SetMemCacheMaxTTLSeconds(0))
	suite.Require().Error(suite.cacheRepo.SetMemCacheMaxTTLSeconds(1000000))
}

func (suite *testSuite) TestStoreKey() {
	suite.Equal("test:{test}", suite.cacheRepo.storeKey("test"))
}

func (suite *testSuite) TestLockKey() {
	suite.Equal("test:{test}_LOCK", suite.cacheRepo.lockKey("test"))
}

// Set on one pod must invalidate the memory cache of other pods.
func (suite *testSuite) TestSetInvalidatesKeyAcrossPods() {
	queryKey := "test"
	v := "testvalueold"
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	var vget string
	err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.Require().NoError(err)

	// second pod backfills its memory cache from Redis.
	var vget2 string
	err = suite.cacheRepo2.Get(context.Background(), queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.Require().NoError(err)
	_, e := suite.inMemCache2.Get([]byte(suite.cacheRepo2.storeKey(queryKey)))
	suite.Require().NoError(e)

	err = suite.cacheRepo.Set(context.Background(), queryKey, "testvaluenew", Normal.ToDuration())
	suite.Require().NoError(err)

	// Wait for the invalidation to be broadcast.
	time.Sleep(1500 * time.Millisecond)
	_, e = suite.inMemCache2.Get([]byte(suite.cacheRepo2.storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e, "second pod must drop its stale value after Set")
}

// Invalidate must clear memory caches even when the key is already gone from Redis.
func (suite *testSuite) TestInvalidateWhenRedisKeyMissing() {
	queryKey := "test"
	v := "testvalue"
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	var vget string
	err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.Require().NoError(err)
	var vget2 string
	err = suite.cacheRepo2.Get(context.Background(), queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.Require().NoError(err)

	// The Redis copy disappears (expiry, eviction, manual delete) while memory caches still hold it.
	suite.Require().NoError(suite.redisConn.Del(context.Background(), suite.cacheRepo.storeKey(queryKey)).Err())

	suite.Require().NoError(suite.cacheRepo.Invalidate(context.Background(), queryKey))
	_, e := suite.inMemCache.Get([]byte(suite.cacheRepo.storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e, "local memory cache must be cleared")

	time.Sleep(1500 * time.Millisecond)
	_, e = suite.inMemCache2.Get([]byte(suite.cacheRepo2.storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e, "other pods must be told to drop the key")
}

// newUnreachableClient returns a Redis client that fails fast on every command.
func newUnreachableClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  -1,
	})
}

// Invalidate must clear the local memory cache even when Redis is unavailable.
func (suite *testSuite) TestInvalidateClearsLocalMemoryWhenRedisFails() {
	client := newUnreachableClient()
	defer client.Close()
	mem := freecache.NewCache(1024 * 1024)
	c, err := NewDCache("invalidate-redis-down", client, mem, time.Second, false, false)
	suite.Require().NoError(err)
	defer c.Close()

	key := c.storeKey("k")
	suite.Require().NoError(mem.Set([]byte(key), []byte("stale"), 60))

	err = c.Invalidate(context.Background(), "k")
	suite.Error(err, "the Redis failure is still reported")
	_, e := mem.Get([]byte(key))
	suite.Equal(freecache.ErrNotFound, e, "local memory cache must be cleared")
}

// metricValue returns the summed value of a counter, or the sample sum of a
// histogram, across series whose labels include all of want.
func metricValue(t *testing.T, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	found := false
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metrics:
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			for k, v := range want {
				if labels[k] != v {
					continue metrics
				}
			}
			found = true
			switch {
			case m.GetCounter() != nil:
				total += m.GetCounter().GetValue()
			case m.GetHistogram() != nil:
				total += m.GetHistogram().GetSampleSum()
			case m.GetGauge() != nil:
				total += m.GetGauge().GetValue()
			}
		}
	}
	return total, found
}

// Several caches in one process must all export metrics, and closing one must
// not stop the others from exporting.
func (suite *testSuite) TestMetricsSharedAcrossCaches() {
	t := suite.T()
	a, err := NewDCache("metrics-a", suite.redisConn, nil, time.Second, true, false)
	suite.Require().NoError(err)
	defer a.Close()
	b, err := NewDCache("metrics-b", suite.redisConn, nil, time.Second, true, false)
	suite.Require().NoError(err)

	read := func() (any, error) { return "v", nil }
	var got string
	suite.Require().NoError(a.Get(context.Background(), "ka", &got, Normal.ToDuration(), read, false, false))
	suite.Require().NoError(b.Get(context.Background(), "kb", &got, Normal.ToDuration(), read, false, false))

	_, found := metricValue(t, "dcache_hit_total", map[string]string{"app": "metrics-a"})
	suite.True(found, "metrics-a hits must be exported")
	_, found = metricValue(t, "dcache_hit_total", map[string]string{"app": "metrics-b"})
	suite.True(found, "metrics-b hits must be exported")

	b.Close()
	suite.Require().NoError(a.Get(context.Background(), "ka2", &got, Normal.ToDuration(), read, false, false))
	_, found = metricValue(t, "dcache_hit_total", map[string]string{"app": "metrics-a"})
	suite.True(found, "closing one cache must not unregister another cache's metrics")
}

// A failed Redis GET is counted as get_redis, not set_redis.
func (suite *testSuite) TestRedisGetFailureLabel() {
	t := suite.T()
	client := newUnreachableClient()
	defer client.Close()
	c, err := NewDCache("get-failure-label", client, nil, time.Second, true, false)
	suite.Require().NoError(err)
	defer c.Close()

	var got string
	err = c.Get(context.Background(), "k", &got, Normal.ToDuration(), func() (any, error) { return "v", nil }, false, false)
	suite.Error(err)
	v, _ := metricValue(t, "dcache_error_total", map[string]string{"app": "get-failure-label", "when": "get_redis"})
	suite.Equal(1.0, v)
	v, _ = metricValue(t, "dcache_error_total", map[string]string{"app": "get-failure-label", "when": "set_redis"})
	suite.Equal(0.0, v)
}

// A failed Publish of invalidated keys is counted instead of silently dropped.
func (suite *testSuite) TestPublishFailureCounted() {
	t := suite.T()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 10})
	mem := freecache.NewCache(1024 * 1024)
	c, err := NewDCache("publish-failure", client, mem, time.Second, true, false)
	suite.Require().NoError(err)
	defer c.Close()

	c.broadcastKeyInvalidate("k")
	suite.Require().NoError(client.Close())
	time.Sleep(1500 * time.Millisecond)

	v, _ := metricValue(t, "dcache_error_total", map[string]string{"app": "publish-failure", "when": "publish_invalidate"})
	suite.GreaterOrEqual(v, 1.0)
}

// Spans record the error returned to the caller.
func TestTraceRecordsError(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 10})
	defer client.Close()
	c, err := NewDCache("trace-error", client, nil, time.Second, false, true)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, parent := tp.Tracer("test").Start(context.Background(), "parent")
	readErr := errors.New("db down")
	var got string
	err = c.GetWithTtl(ctx, "k", &got, func() (any, time.Duration, error) { return nil, 0, readErr }, true, false)
	parent.End()
	if !errors.Is(err, readErr) {
		t.Fatalf("expected read error, got %v", err)
	}

	var span sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == "GetWithTtl" {
			span = s
		}
	}
	if span == nil {
		t.Fatal("GetWithTtl span not recorded")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want Error", span.Status().Code)
	}
}

// Latency is recorded with sub-millisecond precision.
func TestLatencySubMillisecond(t *testing.T) {
	m := &metricSet{
		AppName: "latency",
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_latency", Buckets: latencyBucket}, hitLabels),
	}
	start := time.Unix(1000, 0)
	SetNowFunc(func() time.Time { return start.Add(300 * time.Microsecond) })
	defer SetNowFunc(time.Now)

	m.MakeHitObserver(hitLabelMemory, start)()

	var out dto.Metric
	h := m.Latency.WithLabelValues("latency", string(hitLabelMemory)).(prometheus.Histogram)
	if err := h.Write(&out); err != nil {
		t.Fatal(err)
	}
	if got := out.GetHistogram().GetSampleSum(); got < 0.29 || got > 0.31 {
		t.Fatalf("latency sum = %v ms, want 0.3", got)
	}
}
