package cassandra_storage

import (
	"context"
	"os"
	"strings"
	"testing"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
