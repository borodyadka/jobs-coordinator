# Jobs coordinator

Simple way to handle lots of continuous jobs between distributed workers.

Supported storages:

* etcd;
* redis (coming soon).

## Usage

```go
package main

import (
	"context"
	"fmt"

	"github.com/borodyadka/jobs-coordinator"
	"github.com/borodyadka/jobs-coordinator/storages/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	etcdClient, _ := clientv3.New(clientv3.Config{})
	storage := etcd.NewWithClient(etcdClient, etcd.Options{
		Prefix: "/jobs",
	})
	crd := coordinator.New(storage, coordinator.Options{})

	err := crd.CreateJob(context.Background(), "foobar")
	if err != nil {
		panic(err)
	}

	go func() {
		job, err := crd.AcquireJobByName(context.Background(), "foobar")
		if err != nil {
			panic(err)
		}
		for {
			select {
			case <-job.Done():
				return
			default:
				fmt.Println("handle foobar job")
				job.Release(context.Background())
			}
		}
	}()
}
```

## License

[MIT](LICENSE)
