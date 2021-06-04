package coordinator

import (
	"context"
)

type Storage interface {
	CreateJob(ctx context.Context, name string) error
	DestroyJob(ctx context.Context, name string) error
	ListJobs(ctx context.Context) ([]string, error)
	TryAcquire(ctx context.Context) (Job, error)
	AcquireByName(ctx context.Context, name string) (Job, error)
	TryAcquireByName(ctx context.Context, name string) (Job, error)
	WatchJobs(ctx context.Context) (<-chan JobEvent, error)
}
