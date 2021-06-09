package etcd

import (
	"context"
	"strings"

	coordinator "github.com/borodyadka/jobs-coordinator"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	jobsSuffix  = "/jobs/"
	locksSuffix = "/locks/"
)

type job struct {
	name     string
	mutex    *concurrency.Mutex
	session  *concurrency.Session
	released bool
	donec    chan struct{}
}

func (j *job) Name() string {
	return j.name
}

func (j *job) Release(ctx context.Context) error {
	if j.released {
		return coordinator.ErrJobReleased
	}
	close(j.donec)
	j.released = true
	if err := j.session.Close(); err != nil {
		return err
	}
	if err := j.mutex.Unlock(ctx); err != nil {
		return err
	}
	return nil
}

func (j *job) Done() <-chan struct{} {
	return j.donec
}

func newJob(name string, mutex *concurrency.Mutex, sess *concurrency.Session) *job {
	return &job{
		name:    name,
		mutex:   mutex,
		session: sess,
		donec:   make(chan struct{}),
	}
}

type Options struct {
	Prefix string
}

type storage struct {
	etcd *clientv3.Client
	opts Options
}

func (s *storage) CreateJob(ctx context.Context, name string) error {
	key := s.opts.Prefix + jobsSuffix + name

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	resp, err := s.etcd.Txn(ctx).If(cmp).Then(clientv3.OpPut(key, "")).Else(clientv3.OpGet(key)).Commit()
	if err != nil {
		return err
	}
	if resp.Responses[0].GetResponseRange() != nil {
		return coordinator.ErrJobAlreadyExists
	}
	return nil
}

func (s *storage) DestroyJob(ctx context.Context, name string) error {
	key := s.opts.Prefix + jobsSuffix + name
	_, err := s.etcd.Delete(ctx, key)
	if err != nil {
		return err
	}
	return nil
}

func (s *storage) ListJobs(ctx context.Context) ([]string, error) {
	resp, err := s.etcd.Get(
		ctx,
		s.opts.Prefix+jobsSuffix,
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(resp.Kvs))
	for _, ev := range resp.Kvs {
		result = append(result, strings.TrimPrefix(string(ev.Key), s.opts.Prefix+jobsSuffix))
	}
	return result, nil
}

func (s *storage) listLocks(ctx context.Context) ([]string, error) {
	resp, err := s.etcd.Get(
		ctx,
		s.opts.Prefix+locksSuffix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(resp.Kvs))
	for _, ev := range resp.Kvs {
		result = append(result, strings.TrimPrefix(string(ev.Key), s.opts.Prefix+locksSuffix))
	}
	return result, nil
}

func (s *storage) listAvailable(ctx context.Context) ([]string, error) {
	all, err := s.ListJobs(ctx)
	if err != nil {
		return nil, err
	}
	locked, err := s.listLocks(ctx)
	if err != nil {
		return nil, err
	}
	available := make([]string, 0, abs(len(all)-len(locked)))

outer:
	for _, present := range all {
		for _, omit := range locked {
			if present == omit {
				continue outer
			}
		}
		available = append(available, present)
	}
	return available, nil
}

func (s *storage) TryAcquire(ctx context.Context) (coordinator.Job, error) {
	available, err := s.listAvailable(ctx)
	if err != nil {
		return nil, err
	}

	for _, name := range available {
		j, err := s.TryAcquireByName(ctx, name)
		if err != nil {
			if err == coordinator.ErrJobTaken {
				continue
			}
			return nil, err
		}
		return j, nil
	}
	return nil, nil
}

func (s *storage) checkJobExists(ctx context.Context, name string) error {
	resp, err := s.etcd.Get(ctx, s.opts.Prefix+jobsSuffix+name, clientv3.WithKeysOnly())
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return coordinator.ErrJobNotExists
	}
	return nil
}

func (s *storage) AcquireByName(ctx context.Context, name string) (coordinator.Job, error) {
	if err := s.checkJobExists(ctx, name); err != nil {
		return nil, err
	}

	key := s.opts.Prefix + locksSuffix + name
	sess, err := concurrency.NewSession(s.etcd)
	if err != nil {
		return nil, err
	}
	mutex := concurrency.NewMutex(sess, key)
	if err := mutex.Lock(ctx); err != nil {
		_ = sess.Close()
		return nil, err
	}

	return newJob(name, mutex, sess), nil
}

func (s *storage) TryAcquireByName(ctx context.Context, name string) (coordinator.Job, error) {
	if err := s.checkJobExists(ctx, name); err != nil {
		return nil, err
	}

	key := s.opts.Prefix + locksSuffix + name
	sess, err := concurrency.NewSession(s.etcd)
	if err != nil {
		return nil, err
	}
	mutex := concurrency.NewMutex(sess, key)
	if err := mutex.TryLock(ctx); err != nil {
		_ = sess.Close()
		if err == concurrency.ErrLocked {
			return nil, coordinator.ErrJobTaken
		}
		return nil, err
	}
	return newJob(name, mutex, sess), nil
}

func (s *storage) WatchJobs(ctx context.Context) (<-chan coordinator.JobEvent, error) {
	var changes clientv3.WatchChan
	events := make(chan coordinator.JobEvent)
	jobsPrefix := s.opts.Prefix + jobsSuffix
	locksPrefix := s.opts.Prefix + locksSuffix
	go func() {
		defer close(events)

		for {
			if changes == nil {
				changes = s.etcd.Watch(ctx, s.opts.Prefix, clientv3.WithPrefix())
			}

			select {
			case res := <-changes:
				for i := range res.Events {
					key := string(res.Events[i].Kv.Key)
					var typ coordinator.JobEventType

					if key[0:len(jobsPrefix)] == jobsPrefix {
						if res.Events[i].Type == clientv3.EventTypePut {
							typ = coordinator.JobEventTypeAdded
						} else {
							typ = coordinator.JobEventTypeRemoved
						}

						events <- coordinator.JobEvent{
							Key:  strings.TrimPrefix(key, jobsPrefix),
							Type: typ,
						}
					} else if key[0:len(locksPrefix)] == locksPrefix {
						if res.Events[i].Type == clientv3.EventTypePut {
							typ = coordinator.JobEventTypeLocked
						} else {
							typ = coordinator.JobEventTypeUnlocked
						}
						// we need to cut etcd-specific mutex suffix
						key := strings.TrimPrefix(key, locksPrefix)
						idx := strings.LastIndex(key, "/")

						events <- coordinator.JobEvent{
							Key:  key[0:idx],
							Type: typ,
						}
					}
				}
				if res.Canceled {
					if res.Err() == context.Canceled {
						break
					} else {
						// TODO: maybe better errors handling?
						changes = nil
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return events, nil
}

func NewWithClient(etcd *clientv3.Client, opts Options) *storage {
	return &storage{
		etcd: etcd,
		opts: opts,
	}
}
