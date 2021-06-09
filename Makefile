.PHONY: test
test:
	go test -mod vendor -cover .

.PHONY: test-storages
test-storages:
	$(MAKE) -C storages/etcd test
