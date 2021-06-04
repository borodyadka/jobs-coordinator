.PHONY: test test_etcd

test: test_etcd

test_etcd:
	$(MAKE) -C storages/etcd test
