.PHONY: test

ETCD_HOST = localhost:2379
export

test:
	docker-compose up -d --force-recreate --remove-orphans
	go test -mod vendor -cover ./...
	docker-compose down
