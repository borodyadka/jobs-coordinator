package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCoordinator_CreateJob(t *testing.T) {
	stor := new(storageMock)
	stor.On("CreateJob", "test-job").Return(nil)
	crd := New(stor, Options{})
	err := crd.CreateJob(context.Background(), "test-job")
	assert.NoError(t, err)
	stor.AssertExpectations(t)
}

func TestCoordinator_CreateJobFail(t *testing.T) {
	stor := new(storageMock)
	stor.On("CreateJob", "test-job").Return(errors.New("fail"))
	crd := New(stor, Options{})
	err := crd.CreateJob(context.Background(), "test-job")
	assert.EqualError(t, err, "fail")
	stor.AssertExpectations(t)
}

func TestCoordinator_DestroyJob(t *testing.T) {
	stor := new(storageMock)
	stor.On("DestroyJob", "test-job").Return(nil)
	crd := New(stor, Options{})
	err := crd.DestroyJob(context.Background(), "test-job")
	assert.NoError(t, err)
	stor.AssertExpectations(t)
}

func TestCoordinator_DestroyJobFail(t *testing.T) {
	stor := new(storageMock)
	stor.On("DestroyJob", "test-job").Return(errors.New("fail"))
	crd := New(stor, Options{})
	err := crd.DestroyJob(context.Background(), "test-job")
	assert.EqualError(t, err, "fail")
	stor.AssertExpectations(t)
}

func TestCoordinator_ListJobNames(t *testing.T) {
	stor := new(storageMock)
	stor.On("ListJobs").Return([]string{"test-job"}, nil)
	crd := New(stor, Options{})
	jobs, err := crd.ListJobNames(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []string{"test-job"}, jobs)
	stor.AssertExpectations(t)
}

func TestCoordinator_ListJobNamesFail(t *testing.T) {
	stor := new(storageMock)
	stor.On("ListJobs").Return([]string{}, errors.New("fail"))
	crd := New(stor, Options{})
	_, err := crd.ListJobNames(context.Background())
	assert.EqualError(t, err, "fail")
	stor.AssertExpectations(t)
}

func TestCoordinator_TryAcquireJob(t *testing.T) {
	stor := new(storageMock)
	stor.On("TryAcquire").Return(newJobMock("test-job"), nil)
	stor.On("WatchJobs").Return(make(chan JobEvent), nil)
	crd := New(stor, Options{})
	job, err := crd.TryAcquireJob(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "test-job", job.Name())
	stor.AssertExpectations(t)
}

func TestCoordinator_TryAcquireJobFail(t *testing.T) {
	stor := new(storageMock)
	stor.On("TryAcquire").Return(newJobMock("test-job"), errors.New("fail"))
	crd := New(stor, Options{})
	_, err := crd.TryAcquireJob(context.Background())
	assert.EqualError(t, err, "fail")
	stor.AssertExpectations(t)
}

func TestCoordinator_AcquireJobByName(t *testing.T) {
	stor := new(storageMock)
	stor.On("AcquireByName", "test-job").Return(newJobMock("test-job"), nil)
	stor.On("WatchJobs").Return(make(chan JobEvent), nil)
	crd := New(stor, Options{})
	job, err := crd.AcquireJobByName(context.Background(), "test-job")
	assert.NoError(t, err)
	assert.Equal(t, "test-job", job.Name())
	stor.AssertExpectations(t)
}

func TestCoordinator_AcquireJobByNameFail(t *testing.T) {
	stor := new(storageMock)
	stor.On("AcquireByName", "test-job").Return(newJobMock("test-job"), errors.New("fail"))
	crd := New(stor, Options{})
	_, err := crd.AcquireJobByName(context.Background(), "test-job")
	assert.EqualError(t, err, "fail")
	stor.AssertExpectations(t)
}

func TestCoordinator_TryAcquireJobByName(t *testing.T) {
	stor := new(storageMock)
	stor.On("TryAcquireByName", "test-job").Return(newJobMock("test-job"), nil)
	stor.On("WatchJobs").Return(make(chan JobEvent), nil)
	crd := New(stor, Options{})
	job, err := crd.TryAcquireJobByName(context.Background(), "test-job")
	assert.NoError(t, err)
	assert.Equal(t, "test-job", job.Name())
	stor.AssertExpectations(t)
}

func TestCoordinator_TryAcquireJobByNameFail(t *testing.T) {
	stor := new(storageMock)
	stor.On("TryAcquireByName", "test-job").Return(newJobMock("test-job"), errors.New("fail"))
	crd := New(stor, Options{})
	_, err := crd.TryAcquireJobByName(context.Background(), "test-job")
	assert.EqualError(t, err, "fail")
	stor.AssertExpectations(t)
}

func TestCoordinator_WatchJobs(t *testing.T) {
	stor := new(storageMock)
	ch := make(chan JobEvent)
	stor.On("WatchJobs").Return(ch, nil)
	crd := New(stor, Options{})
	wch, err := crd.WatchJobs(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, wch)
	stor.AssertExpectations(t)
}

func TestCoordinator_WatchJobsFail(t *testing.T) {
	stor := new(storageMock)
	ch := make(chan JobEvent)
	stor.On("WatchJobs").Return(ch, errors.New("fail"))
	crd := New(stor, Options{})
	_, err := crd.WatchJobs(context.Background())
	assert.EqualError(t, err, "fail")
	stor.AssertExpectations(t)
}

func TestCoordinator_Shutdown(t *testing.T) {
	stor := new(storageMock)
	crd := New(stor, Options{})
	job := newJobMock("test-job")
	job.On("Release").Return(nil)
	stor.On("TryAcquire").Return(job, nil)
	stor.On("WatchJobs").Return(make(chan JobEvent), nil)
	_, _ = crd.TryAcquireJob(context.Background())
	assert.NotEqual(t, true, job.released)
	_ = crd.Shutdown(context.Background())
	assert.Equal(t, true, job.released)
}

func TestCoordinator_AutoRelease(t *testing.T) {
	stor := new(storageMock)
	crd := New(stor, Options{})
	job := newJobMock("test-job")
	job.On("Release").Return(nil)
	stor.On("TryAcquire").Return(job, nil)
	ch := make(chan JobEvent)
	stor.On("WatchJobs").Return(ch, nil)
	_, _ = crd.TryAcquireJob(context.Background())

	assert.NotEqual(t, true, job.released)
	ch <- JobEvent{Key: "test-job", Type: JobEventTypeRemoved}
	assert.Equal(t, true, job.released)
}
