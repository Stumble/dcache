package dcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coocood/freecache"
	"github.com/go-redis/redis/v8"
	// "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	dbResponseTime = 100 * time.Millisecond
	waitTime       = 10 * time.Millisecond
)

type testSuite struct {
	suite.Suite
	redisConn   redis.UniversalClient
	inMemCache  *freecache.Cache
	cacheRepo   Cache
	inMemCache2 *freecache.Cache
	cacheRepo2  Cache
	mockRepo    dummyMock
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
	inMemCache := freecache.NewCache(1024 * 1024)
	cacheRepo, e := NewCache("test", redisClient, inMemCache, time.Second, true)
	if e != nil {
		panic(e)
	}
	inMemCache2 := freecache.NewCache(1024 * 1024)
	cacheRepo2, e := NewCache("test", redisClient, inMemCache2, time.Second, false)
	if e != nil {
		panic(e)
	}
	return &testSuite{
		redisConn:   redisClient,
		cacheRepo:   cacheRepo,
		inMemCache:  inMemCache,
		cacheRepo2:  cacheRepo2,
		inMemCache2: inMemCache2,
	}
}

func TestRepoTestSuite(t *testing.T) {
	suite.Run(t, newTestSuite())
}

func (suite *testSuite) BeforeTest(_, _ string) {
	suite.inMemCache.Clear()
	suite.inMemCache2.Clear()
	suite.Require().NoError(suite.redisConn.FlushAll(context.Background()).Err())
}

func (suite *testSuite) AfterTest(_, _ string) {
	suite.mockRepo.AssertExpectations(suite.T())
}

func (suite *testSuite) TearDownSuite() {
	suite.cacheRepo.Close()
	suite.cacheRepo2.Close()
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

	redisBytes, err := suite.redisConn.Get(ctx, storeKey(queryKey)).Bytes()
	suite.Require().NoError(err)
	vredis := &ValueBytesExpiredAt{}
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev, vredis.ValueBytes)

	vinmem, e := suite.inMemCache.Get([]byte(storeKey(queryKey)))
	suite.Require().NoError(e)
	suite.Equal(ev, vinmem)

	// Second pod should not hit db either
	var vget2 string
	err = suite.cacheRepo2.Get(context.Background(), queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	suite.Equal(v, vget2)

	vinmem2, e := suite.inMemCache2.Get([]byte(storeKey(queryKey)))
	suite.NoError(e)
	suite.Equal(ev, vinmem2)
}

type data struct {
	s string
	i int
}

func (suite *testSuite) TestCachePenetration() {
	ctx := context.Background()
	queryKey := "test"
	var vget *data = new(data)
	suite.mockRepo.On("ReadThrough").Return(nil, nil).Once()
	err := suite.cacheRepo.Get(
		ctx, queryKey, vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal((*data)(nil), vget)

	// second time should not penetrate cache.
	err = suite.cacheRepo.Get(
		ctx, queryKey, vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)
	suite.Equal((*data)(nil), vget)
}

func (suite *testSuite) TestCachedNilNotOverwriteTarget() {
	ctx := context.Background()
	queryKey := "test"
	vget := "previous_value"
	suite.mockRepo.On("ReadThrough").Return(nil, nil).Once()
	err := suite.cacheRepo.Get(
		ctx, queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
	suite.NoError(err)

	// This is a known issue for now...
	suite.NotEqual((*string)(nil), vget)
	suite.Equal("previous_value", vget)
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
	redisBytes, err := suite.redisConn.Get(ctx, storeKey(queryKey1)).Bytes()
	suite.Require().NoError(err)
	vredis := &ValueBytesExpiredAt{}
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev1, vredis.ValueBytes)

	vinmem, e := suite.inMemCache.Get([]byte(storeKey(queryKey1)))
	suite.NoError(e)
	suite.Equal(ev1, vinmem)

	// get v2
	redisBytes, err = suite.redisConn.Get(ctx, storeKey(queryKey2)).Bytes()
	suite.Require().NoError(err)
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev2, vredis.ValueBytes)

	vinmem, e = suite.inMemCache.Get([]byte(storeKey(queryKey2)))
	suite.NoError(e)
	suite.Equal(ev2, vinmem)

	time.Sleep(time.Second * 2)

	// get v1, not exist
	redisExist := suite.redisConn.Exists(ctx, storeKey(queryKey1)).Val()
	suite.EqualValues(redisExist, 0)

	_, e = suite.inMemCache.Get([]byte(storeKey(queryKey1)))
	suite.Error(e)

	// get v2
	redisBytes, err = suite.redisConn.Get(ctx, storeKey(queryKey2)).Bytes()
	suite.Require().NoError(err)
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(ev2, vredis.ValueBytes)

	vinmem, e = suite.inMemCache.Get([]byte(storeKey(queryKey2)))
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

func (suite *testSuite) TestConcurrentReadWait() {
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
	var vget2 string
	err := suite.cacheRepo2.Get(context.Background(), queryKey, &vget2, Normal.ToDuration(), func() (interface{}, error) {
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

func (suite *testSuite) TestConcurrentReadAfterExpire() {
	queryKey := "test"
	v := "testvalueold"
	// Only one pod should hit db
	suite.mockRepo.On("ReadThrough").Return(v, nil).Once()
	var vget string
	err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, time.Second, func() (interface{}, error) {
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
		wg.Done()
		err := suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
			return suite.mockRepo.ReadThrough()
		}, false, false)
		suite.NoError(err)
		suite.Equal(newv, vget)
	}()
	// Make sure cache2 is called later and timeout is within db response time
	time.Sleep(dbResponseTime / 2)
	err = suite.cacheRepo2.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
		return suite.mockRepo.ReadThrough()
	}, false, false)
	suite.NoError(err)
	// The slower thread should return old cache value without wait
	suite.Equal(newv, vget)

	time.Sleep(dbResponseTime)
	// Should get newv afterwards
	err = suite.cacheRepo.Get(context.Background(), queryKey, &vget, Normal.ToDuration(), func() (interface{}, error) {
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
	exist, e := suite.redisConn.Exists(context.Background(), storeKey(queryKey)).Result()
	suite.NoError(e)
	suite.EqualValues(0, exist)

	_, e = suite.inMemCache.Get([]byte(storeKey(queryKey)))
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

	redisBytes, err := suite.redisConn.Get(context.Background(), storeKey(queryKey)).Bytes()
	suite.Require().NoError(err)
	vredis := &ValueBytesExpiredAt{}
	suite.Require().NoError(msgpack.Unmarshal(redisBytes, vredis))
	suite.Equal(newve, vredis.ValueBytes)

	vinmem, e := suite.inMemCache.Get([]byte(storeKey(queryKey)))
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

	vinmem, e := suite.inMemCache2.Get([]byte(storeKey(queryKey)))
	suite.NoError(e)
	suite.Equal(ve, vinmem)

	vinmem, e = suite.inMemCache2.Get([]byte(storeKey(queryKey)))
	suite.NoError(e)
	suite.Equal(ve, vinmem)

	time.Sleep(waitTime)
	err = suite.cacheRepo.Invalidate(context.Background(), queryKey)
	suite.NoError(err)

	// Wait for key to be broadcasted
	time.Sleep(time.Second)
	exist, e := suite.redisConn.Exists(context.Background(), storeKey(queryKey)).Result()
	suite.NoError(e)
	suite.EqualValues(0, exist)

	_, e = suite.inMemCache.Get([]byte(storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e)

	// check inmemcache of second pod is invalidated too
	_, e = suite.inMemCache2.Get([]byte(storeKey(queryKey)))
	suite.Equal(freecache.ErrNotFound, e)
}
