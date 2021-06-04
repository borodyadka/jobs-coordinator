package coordinator

import (
	"context"
)

type JobEventType int8

const (
	JobEventTypeAdded JobEventType = iota
	JobEventTypeRemoved
)

type JobEvent struct {
	Key  string
	Type JobEventType
}

type Job interface {
	Name() string
	Release(ctx context.Context) error
	Done() <-chan struct{}
}

type Options struct {
}

type Coordinator struct {
	storage Storage
	options Options
}

func (c *Coordinator) CreateJob(ctx context.Context, name string) error {
	return c.storage.CreateJob(ctx, name)
}

func (c *Coordinator) DestroyJob(ctx context.Context, name string) error {
	return c.storage.DestroyJob(ctx, name)
}

func (c *Coordinator) ListJobNames(ctx context.Context) ([]string, error) {
	return c.storage.ListJobs(ctx)
}

func (c *Coordinator) TryAcquireJob(ctx context.Context) (Job, error) {
	return c.storage.TryAcquire(ctx)
}

func (c *Coordinator) AcquireJobByName(ctx context.Context, name string) (Job, error) {
	return c.storage.AcquireByName(ctx, name)
}

func (c *Coordinator) TryAcquireJobByName(ctx context.Context, name string) (Job, error) {
	return c.storage.TryAcquireByName(ctx, name)
}

func (c *Coordinator) WatchJobs(ctx context.Context) (<-chan JobEvent, error) {
	return c.storage.WatchJobs(ctx)
}

func New(storage Storage, options Options) *Coordinator {
	return &Coordinator{
		storage: storage,
		options: options,
	}
}
