package cassandra_storage

import "fmt"

var (
	ErrKeyspaceEmpty       = fmt.Errorf("Keyspace can not empty")
	ErrTableNameEmpty      = fmt.Errorf("TableName can not empty")
	ErrConnectionManagerNil = fmt.Errorf("ConnectionManager can not nil")
)