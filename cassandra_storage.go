package cassandra_storage

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// CassandraStorage 基于 Apache Cassandra 的存储引擎实现
// Cassandra 支持轻量级事务（LWT），可实现真正的 CAS
type CassandraStorage struct {
	options      *CassandraStorageOptions
	session      *gocql.Session
	timeProvider storage.TimeProvider
}

var _ storage.Storage = &CassandraStorage{}

// 查询默认走真实会话。Session 没有可替换的查询接口，测试通过替换这些函数覆盖分支。
var (
	cassandraExec = func(session *gocql.Session, query string) error {
		return session.Query(query).Exec()
	}
	cassandraScanCAS = func(session *gocql.Session, ctx context.Context, query string, args ...any) (bool, error) {
		return session.Query(query, args...).WithContext(ctx).ScanCAS()
	}
	cassandraScan = func(session *gocql.Session, ctx context.Context, query string, args []any, dest ...any) error {
		return session.Query(query, args...).WithContext(ctx).Scan(dest...)
	}
	cassandraIter = func(session *gocql.Session, ctx context.Context, query string) cassandraIterator {
		return session.Query(query).WithContext(ctx).Iter()
	}
)

type cassandraIterator interface {
	Scan(dest ...any) bool
	Close() error
}

func NewCassandraStorage(ctx context.Context, options *CassandraStorageOptions) (*CassandraStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &CassandraStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const CassandraStorageName = "cassandra-storage"

func (x *CassandraStorage) GetName() string {
	return CassandraStorageName
}

func (x *CassandraStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
		storage.CapabilityWatch,
	}
}

func (x *CassandraStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

func (x *CassandraStorage) Init(ctx context.Context) error {
	session, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.session = session

	// 创建表（如果不存在）
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		lock_id text PRIMARY KEY,
		version bigint,
		json_data text
	)`, x.options.Keyspace, x.options.TableName)
	if err := cassandraExec(x.session, query); err != nil {
		return err
	}

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// CreateWithVersion 使用 LWT 原子创建
func (x *CassandraStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	query := fmt.Sprintf(`INSERT INTO %s.%s (lock_id, version, json_data) VALUES (?, ?, ?) IF NOT EXISTS`,
		x.options.Keyspace, x.options.TableName)
	applied, err := cassandraScanCAS(x.session, ctx, query, lockId, int64(version), lockInformation.ToJsonString())
	if err != nil {
		return err
	}
	if !applied {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// UpdateWithVersion 使用 LWT 原子更新
func (x *CassandraStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	query := fmt.Sprintf(`UPDATE %s.%s SET version = ?, json_data = ? WHERE lock_id = ? IF version = ?`,
		x.options.Keyspace, x.options.TableName)
	applied, err := cassandraScanCAS(x.session, ctx, query, int64(newVersion), lockInformation.ToJsonString(), lockId, int64(expectedVersion))
	if err != nil {
		return err
	}
	if !applied {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// DeleteWithVersion 使用 LWT 原子删除
func (x *CassandraStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	query := fmt.Sprintf(`DELETE FROM %s.%s WHERE lock_id = ? IF version = ?`,
		x.options.Keyspace, x.options.TableName)
	applied, err := cassandraScanCAS(x.session, ctx, query, lockId, int64(expectedVersion))
	if err != nil {
		return err
	}
	if !applied {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

func (x *CassandraStorage) Get(ctx context.Context, lockId string) (string, error) {
	query := fmt.Sprintf(`SELECT version, json_data FROM %s.%s WHERE lock_id = ?`,
		x.options.Keyspace, x.options.TableName)
	var version int64
	var jsonData string
	if err := cassandraScan(x.session, ctx, query, []any{lockId}, &version, &jsonData); err != nil {
		if err == gocql.ErrNotFound {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	return jsonData, nil
}

func (x *CassandraStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

func (x *CassandraStorage) Close(ctx context.Context) error {
	x.session = nil
	return nil
}

func (x *CassandraStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	query := fmt.Sprintf(`SELECT json_data FROM %s.%s`, x.options.Keyspace, x.options.TableName)
	iter := cassandraIter(x.session, ctx, query)

	slice := make([]*storage.LockInformation, 0)
	var jsonData string
	for iter.Scan(&jsonData) {
		lockInfo, err := storage.LockInformationFromJsonString(jsonData)
		if err != nil {
			continue
		}
		slice = append(slice, lockInfo)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return go_iterator.FromSlice(slice), nil
}
