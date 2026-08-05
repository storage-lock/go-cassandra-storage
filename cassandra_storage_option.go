package cassandra_storage

import (
	"github.com/gocql/gocql"
	"github.com/storage-lock/go-storage"
)

// CassandraStorageOptions Cassandra 存储的选项
type CassandraStorageOptions struct {
	ConnectionManager storage.ConnectionManager[*gocql.Session]
	Keyspace          string
	TableName         string
	TimeProvider      storage.TimeProvider
}

func NewCassandraStorageOptions() *CassandraStorageOptions {
	return &CassandraStorageOptions{
		TableName: storage.DefaultStorageTableName,
	}
}

func (x *CassandraStorageOptions) SetConnectionManager(cm storage.ConnectionManager[*gocql.Session]) *CassandraStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *CassandraStorageOptions) SetKeyspace(keyspace string) *CassandraStorageOptions {
	x.Keyspace = keyspace
	return x
}

func (x *CassandraStorageOptions) SetTableName(tableName string) *CassandraStorageOptions {
	x.TableName = tableName
	return x
}

func (x *CassandraStorageOptions) SetTimeProvider(tp storage.TimeProvider) *CassandraStorageOptions {
	x.TimeProvider = tp
	return x
}

func (x *CassandraStorageOptions) Check() error {
	if x.Keyspace == "" {
		return ErrKeyspaceEmpty
	}
	if x.TableName == "" {
		return ErrTableNameEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}