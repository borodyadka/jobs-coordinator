package coordinator

import (
	"context"
	"sync"
)

type JobEventType int8

const (
	JobEventTypeAdded JobEventType = iota
	JobEventTypeRemoved
	JobEventTypeLocked
	JobEventTypeUnlocked
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

type Storage interface {
	CreateJob(ctx context.Context, name string) error
	DestroyJob(ctx context.Context, name string) error
	ListJobs(ctx context.Context) ([]string, error)
	TryAcquire(ctx context.Context) (Job, error)
	AcquireByName(ctx context.Context, name string) (Job, error)
	TryAcquireByName(ctx context.Context, name string) (Job, error)
	WatchJobs(ctx context.Context) (<-chan JobEvent, error)
}

type Options struct {
	// do not release destroyed jobs
	NoAutoRelease bool
}

type Coordinator interface {
	CreateJob(ctx context.Context, name string) error
	DestroyJob(ctx context.Context, name string) error
	ListJobNames(ctx context.Context) ([]string, error)
	TryAcquireJob(ctx context.Context) (Job, error)
	AcquireJobByName(ctx context.Context, name string) (Job, error)
	TryAcquireJobByName(ctx context.Context, name string) (Job, error)
	WatchJobs(ctx context.Context) (<-chan JobEvent, error)
	Shutdown(ctx context.Context) error
}

type coordinator struct {
	storage Storage
	options Options
	lock    sync.Mutex
	jobs    sync.Map
	ctx     context.Context
	stop    context.CancelFunc
	watcher <-chan JobEvent
}

func (c *coordinator) CreateJob(ctx context.Context, name string) error {
	return c.storage.CreateJob(ctx, name)
}

func (c *coordinator) DestroyJob(ctx context.Context, name string) error {
	return c.storage.DestroyJob(ctx, name)
}

func (c *coordinator) ListJobNames(ctx context.Context) ([]string, error) {
	return c.storage.ListJobs(ctx)
}

func (c *coordinator) TryAcquireJob(ctx context.Context) (Job, error) {
	job, err := c.storage.TryAcquire(ctx)
	if err != nil {
		return nil, err
	}
	c.jobs.Store(job.Name(), job)
	c.ensureReleaseWatcher()
	return job, nil
}

func (c *coordinator) AcquireJobByName(ctx context.Context, name string) (Job, error) {
	job, err := c.storage.AcquireByName(ctx, name)
	if err != nil {
		return nil, err
	}
	c.jobs.Store(job.Name(), job)
	c.ensureReleaseWatcher()
	return job, nil
}

func (c *coordinator) TryAcquireJobByName(ctx context.Context, name string) (Job, error) {
	job, err := c.storage.TryAcquireByName(ctx, name)
	if err != nil {
		return nil, err
	}
	c.jobs.Store(job.Name(), job)
	c.ensureReleaseWatcher()
	return job, nil
}

func (c *coordinator) WatchJobs(ctx context.Context) (<-chan JobEvent, error) {
	return c.storage.WatchJobs(ctx)
}

func (c *coordinator) ensureReleaseWatcher() {
	if c.options.NoAutoRelease {
		// we don't need to release destroyed jobs
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.watcher == nil {
		var err error
		c.watcher, err = c.storage.WatchJobs(c.ctx)
		if err != nil {
			// TODO: handle error
			return
		}
		go c.watchEvents()
	}
}

func (c *coordinator) watchEvents() {
	for {
		select {
		case <-c.ctx.Done():
			c.watcher = nil
			break
		case ev := <-c.watcher:
			if ev.Type == JobEventTypeRemoved || ev.Type == JobEventTypeUnlocked {
				if j, ok := c.jobs.LoadAndDelete(ev.Key); ok {
					_ = j.(Job).Release(context.Background()) // TODO: handle error
				}
			}
		}
	}
}

func (c *coordinator) Shutdown(ctx context.Context) error {
	c.stop()
	c.lock.Lock()
	c.jobs.Range(func(key, value interface{}) bool {
		_ = value.(Job).Release(ctx)
		return true
	})
	defer c.lock.Unlock()
	return nil
}

func New(storage Storage, options Options) Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		storage: storage,
		options: options,
		ctx:     ctx,
		stop:    cancel,
		// TODO: logger
	}
}
