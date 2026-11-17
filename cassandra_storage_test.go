package cassandra_storage

import (
	"context"
	"os"
	"strings"
	"testing"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
)

func TestCassandraStorage(t *testing.T) {
	envName := "STORAGE_LOCK_CASSANDRA_HOSTS"
	hosts := os.Getenv(envName)
	if hosts == "" {
		t.Skip("STORAGE_LOCK_CASSANDRA_HOSTS not set, skipping Cassandra test")
	}

	keyspace := os.Getenv("STORAGE_LOCK_CASSANDRA_KEYSPACE")
	if keyspace == "" {
		keyspace = "storage_lock_test"
	}

	ctx := context.Background()
	hostList := strings.Split(hosts, ",")
	cm := NewCassandraConnectionManagerFromConfig(hostList, keyspace)
	s, err := NewCassandraStorage(ctx, &CassandraStorageOptions{
		ConnectionManager: cm,
		Keyspace:          keyspace,
		TableName:         storage_test_helper.TestTableName,
		TimeProvider:      &LocalTimeProvider{},
	})
	// 构造失败必须立刻中止：TestStorage 不检查入参，nil 存储会在 Init 里空指针崩溃，
	// 把真正的连接错误盖掉。
	if err != nil {
		t.Fatalf("NewCassandraStorage: %v", err)
	}
	storage_test_helper.TestStorage(t, s)
}
