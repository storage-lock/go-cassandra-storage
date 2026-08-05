package cassandra_storage

import (
	"context"
	"sync"

	"github.com/gocql/gocql"
	"github.com/storage-lock/go-storage"
)

// CassandraConnectionManager 管理 Cassandra 会话连接
type CassandraConnectionManager struct {
	ClusterHosts []string
	Keyspace     string

	presetSession *gocql.Session

	initMu      sync.Mutex
	initialized bool
	session     *gocql.Session
}

var _ storage.ConnectionManager[*gocql.Session] = &CassandraConnectionManager{}

// NewCassandraConnectionManagerFromConfig 从配置创建
func NewCassandraConnectionManagerFromConfig(hosts []string, keyspace string) *CassandraConnectionManager {
	return &CassandraConnectionManager{
		ClusterHosts: hosts,
		Keyspace:     keyspace,
	}
}

// NewCassandraConnectionManagerFromSession 从已有会话创建
func NewCassandraConnectionManagerFromSession(session *gocql.Session) *CassandraConnectionManager {
	return &CassandraConnectionManager{
		presetSession: session,
		initialized:   true,
		session:       session,
	}
}

func (x *CassandraConnectionManager) Name() string {
	return "cassandra-connection-manager"
}

func (x *CassandraConnectionManager) Take(ctx context.Context) (*gocql.Session, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.session, nil
	}

	cluster := gocql.NewCluster(x.ClusterHosts...)
	cluster.Keyspace = x.Keyspace
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	x.session = session
	x.initialized = true
	return x.session, nil
}

func (x *CassandraConnectionManager) Return(ctx context.Context, connection *gocql.Session) error {
	return nil
}

func (x *CassandraConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.session != nil {
		x.session.Close()
	}
	x.session = nil
	x.initialized = false
	return nil
}