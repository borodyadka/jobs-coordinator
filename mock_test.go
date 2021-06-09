package coordinator

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type storageMock struct {
	mock.Mock
}

func (s *storageMock) CreateJob(_ context.Context, name string) error {
	args := s.Called(name)
	return args.Error(0)
}

func (s *storageMock) DestroyJob(_ context.Context, name string) error {
	args := s.Called(name)
	return args.Error(0)
}

func (s *storageMock) ListJobs(_ context.Context) ([]string, error) {
	args := s.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (s *storageMock) TryAcquire(_ context.Context) (Job, error) {
	args := s.Called()
	return args.Get(0).(Job), args.Error(1)
}

func (s *storageMock) AcquireByName(_ context.Context, name string) (Job, error) {
	args := s.Called(name)
	return args.Get(0).(Job), args.Error(1)
}

func (s *storageMock) TryAcquireByName(_ context.Context, name string) (Job, error) {
	args := s.Called(name)
	return args.Get(0).(Job), args.Error(1)
}

func (s *storageMock) WatchJobs(_ context.Context) (<-chan JobEvent, error) {
	args := s.Called()
	return args.Get(0).(chan JobEvent), args.Error(1)
}

type jobMock struct {
	mock.Mock
	name     string
	released bool
}

func (j *jobMock) Name() string {
	return j.name
}

func (j *jobMock) Release(ctx context.Context) error {
	j.released = true
	args := j.Called()
	return args.Error(0)
}

func (j *jobMock) Done() <-chan struct{} {
	args := j.Called()
	return args.Get(0).(<-chan struct{})
}

func newJobMock(name string) *jobMock {
	return &jobMock{name: name}
}
