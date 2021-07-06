package etcd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/borodyadka/jobs-coordinator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/client/v3"
)

func getStorage(client *clientv3.Client) *storage {
	return NewWithClient(client, Options{
		JobPrefix: "/jobs/",
		LockPrefix: "/locks/",
	})
}

type EtcdTestSuite struct {
	suite.Suite
	etcd *clientv3.Client
}

func (suite *EtcdTestSuite) SetupSuite() {
	var err error
	suite.etcd, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{os.Getenv("ETCD_HOST")},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		suite.T().Fatal(err)
	}
}

func (suite *EtcdTestSuite) TearDownSuite() {
	if err := suite.etcd.Close(); err != nil {
		suite.T().Fatal(err)
	}
}

func (suite *EtcdTestSuite) TearDownTest() {
	if _, err := suite.etcd.Delete(context.Background(), "/", clientv3.WithPrefix()); err != nil {
		suite.T().Fatal(err)
	}
}

func (suite *EtcdTestSuite) TestCreateJob() {
	storage := getStorage(suite.etcd)
	err := storage.CreateJob(context.Background(), "TestCreateJob")
	assert.NoError(suite.T(), err)

	resp, err := suite.etcd.Get(context.Background(), "/jobs/TestCreateJob")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "/jobs/TestCreateJob", string(resp.Kvs[0].Key))
	assert.Less(suite.T(), int64(0), resp.Kvs[0].CreateRevision)
}

func TestCreateJob(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestDestroyJob() {
	storage := getStorage(suite.etcd)
	err := storage.CreateJob(context.Background(), "TestDestroyJob")
	assert.NoError(suite.T(), err)

	err = storage.DestroyJob(context.Background(), "TestDestroyJob")
	assert.NoError(suite.T(), err)

	resp, err := suite.etcd.Get(context.Background(), "/jobs/TestDestroyJob")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 0, len(resp.Kvs))
}

func TestDestroyJob(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestListJobs() {
	storage := getStorage(suite.etcd)
	err := storage.CreateJob(context.Background(), "TestListJobs")
	assert.NoError(suite.T(), err)

	jobs, err := storage.ListJobs(context.Background())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), []string{"TestListJobs"}, jobs)
}

func TestListJobs(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestTryAcquireNoJobs() {
	storage := getStorage(suite.etcd)
	job, err := storage.TryAcquire(context.Background())
	assert.NoError(suite.T(), err)
	assert.Nil(suite.T(), job)
}

func TestTryAcquireNoJobs(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestTryAcquire() {
	storage := getStorage(suite.etcd)
	err := storage.CreateJob(context.Background(), "TestTryAcquire")
	assert.NoError(suite.T(), err)

	job, err := storage.TryAcquire(context.Background())
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "TestTryAcquire", job.Name())
}

func TestTryAcquire(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestAcquireByNameNoJob() {
	storage := getStorage(suite.etcd)
	job, err := storage.AcquireByName(context.Background(), "TestAcquireByNameNoJob")
	assert.Error(suite.T(), coordinator.ErrJobNotExists, err)
	assert.Nil(suite.T(), job)
}

func TestAcquireByNameNoJob(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestAcquireByName() {
	storage := getStorage(suite.etcd)
	err := storage.CreateJob(context.Background(), "TestAcquireByName")
	assert.NoError(suite.T(), err)

	job, err := storage.AcquireByName(context.Background(), "TestAcquireByName")
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "TestAcquireByName", job.Name())
}

func TestAcquireByName(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestAcquireByNameAlreadyLocked() {
	storage := getStorage(suite.etcd)
	err := storage.CreateJob(context.Background(), "TestAcquireByNameAlreadyLocked")
	assert.NoError(suite.T(), err)

	_, err = storage.AcquireByName(context.Background(), "TestAcquireByNameAlreadyLocked")
	assert.NoError(suite.T(), err)

	job, err := storage.TryAcquireByName(context.Background(), "TestAcquireByNameAlreadyLocked")
	assert.Nil(suite.T(), job)
	assert.Error(suite.T(), coordinator.ErrJobTaken, err)
}

func TestAcquireByNameAlreadyLocked(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestJobRelease() {
	storage := getStorage(suite.etcd)
	err := storage.CreateJob(context.Background(), "TestJobRelease")
	assert.NoError(suite.T(), err)

	job, err := storage.AcquireByName(context.Background(), "TestJobRelease")
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), job)

	_, err = storage.TryAcquireByName(context.Background(), "TestJobRelease")
	assert.Error(suite.T(), coordinator.ErrJobTaken, err)

	err = job.Release(context.Background())
	assert.NoError(suite.T(), err)

	_, err = storage.TryAcquireByName(context.Background(), "TestJobRelease")
	assert.NoError(suite.T(), err)
}

func TestJobRelease(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestWatchJobs() {
	ch := make(chan struct{})
	storage := getStorage(suite.etcd)
	go func() {
		jch, err := storage.WatchJobs(context.Background())
		assert.NoError(suite.T(), err)
		ch <- struct{}{}
		ev1 := <-jch
		assert.Equal(suite.T(), coordinator.JobEventTypeAdded, ev1.Type)
		assert.Equal(suite.T(), "TestWatchJobs", ev1.Key)

		ev2 := <-jch
		assert.Equal(suite.T(), coordinator.JobEventTypeRemoved, ev2.Type)
		assert.Equal(suite.T(), "TestWatchJobs", ev2.Key)
		ch <- struct{}{}
	}()
	<-ch
	err := storage.CreateJob(context.Background(), "TestWatchJobs")
	assert.NoError(suite.T(), err)
	err = storage.DestroyJob(context.Background(), "TestWatchJobs")
	assert.NoError(suite.T(), err)
	<-ch
}

func TestWatchJobs(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestWatchLocks() {
	ch := make(chan struct{})
	storage := getStorage(suite.etcd)
	go func() {
		jch, err := storage.WatchJobs(context.Background())
		assert.NoError(suite.T(), err)
		ch <- struct{}{}

		ev1 := <-jch
		assert.Equal(suite.T(), coordinator.JobEventTypeAdded, ev1.Type)
		assert.Equal(suite.T(), "TestWatchLocks", ev1.Key)

		ev2 := <-jch
		assert.Equal(suite.T(), coordinator.JobEventTypeLocked, ev2.Type)
		assert.Equal(suite.T(), "TestWatchLocks", ev2.Key)

		ev3 := <-jch
		assert.Equal(suite.T(), coordinator.JobEventTypeUnlocked, ev3.Type)
		assert.Equal(suite.T(), "TestWatchLocks", ev3.Key)
		ch <- struct{}{}
	}()
	<-ch
	err := storage.CreateJob(context.Background(), "TestWatchLocks")
	assert.NoError(suite.T(), err)

	job, err := storage.AcquireByName(context.Background(), "TestWatchLocks")
	assert.NoError(suite.T(), err)

	err = job.Release(context.Background())
	assert.NoError(suite.T(), err)
	<-ch
}

func TestWatchLocks(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}

func (suite *EtcdTestSuite) TestWatchCancel() {
	ch := make(chan struct{})
	storage := getStorage(suite.etcd)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c, err := storage.WatchJobs(ctx)
		assert.NoError(suite.T(), err)
		<-c
		ch <- struct{}{}
	}()
	cancel()
	<-ch
}

func TestWatchCancel(t *testing.T) {
	suite.Run(t, new(EtcdTestSuite))
}
