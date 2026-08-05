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
	}
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
	if err := x.session.Query(query).Exec(); err != nil {
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
	applied, err := x.session.Query(query, lockId, int64(version), lockInformation.ToJsonString()).WithContext(ctx).ScanCAS()
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
	applied, err := x.session.Query(query, int64(newVersion), lockInformation.ToJsonString(), lockId, int64(expectedVersion)).WithContext(ctx).ScanCAS()
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
	applied, err := x.session.Query(query, lockId, int64(expectedVersion)).WithContext(ctx).ScanCAS()
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
	if err := x.session.Query(query, lockId).WithContext(ctx).Scan(&version, &jsonData); err != nil {
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
	iter := x.session.Query(query).WithContext(ctx).Iter()

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