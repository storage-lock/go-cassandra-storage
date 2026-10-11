package cassandra_storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

type takeErrorManager struct{}

func (takeErrorManager) Name() string { return "take-error" }
func (takeErrorManager) Take(context.Context) (*gocql.Session, error) {
	return nil, errors.New("take failed")
}
func (takeErrorManager) Return(context.Context, *gocql.Session) error { return nil }
func (takeErrorManager) Shutdown(context.Context) error               { return nil }

type fakeIter struct {
	values []string
	index  int
	err    error
}

func (i *fakeIter) Scan(dest ...any) bool {
	if i.index >= len(i.values) {
		return false
	}
	*(dest[0].(*string)) = i.values[i.index]
	i.index++
	return true
}
func (i *fakeIter) Close() error { return i.err }

func newTestCassandra(t *testing.T) *CassandraStorage {
	t.Helper()
	s, err := NewCassandraStorage(context.Background(), &CassandraStorageOptions{
		ConnectionManager: NewCassandraConnectionManagerFromSession(&gocql.Session{}),
		Keyspace:          "ks",
		TableName:         "locks",
		TimeProvider:      &LocalTimeProvider{},
	})
	assert.NoError(t, err)
	return s
}

func restoreCassandra(t *testing.T) {
	t.Helper()
	exec, cas, scan, iter, create, closeFn := cassandraExec, cassandraScanCAS, cassandraScan, cassandraIter, cassandraCreateSession, cassandraCloseSession
	t.Cleanup(func() {
		cassandraExec, cassandraScanCAS, cassandraScan, cassandraIter, cassandraCreateSession, cassandraCloseSession = exec, cas, scan, iter, create, closeFn
	})
}

func TestCassandraConnectionManager(t *testing.T) {
	preset := &gocql.Session{}
	fromSession := NewCassandraConnectionManagerFromSession(preset)
	assert.Equal(t, "cassandra-connection-manager", fromSession.Name())
	got, err := fromSession.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, preset, got)
	assert.NoError(t, fromSession.Return(context.Background(), got))
	assert.NoError(t, fromSession.Shutdown(context.Background()))

	empty := &CassandraConnectionManager{}
	assert.NoError(t, empty.Shutdown(context.Background()))

	restoreCassandra(t)
	cassandraCreateSession = func(*gocql.ClusterConfig) (*gocql.Session, error) {
		return nil, errors.New("dial failed")
	}
	fromConfig := NewCassandraConnectionManagerFromConfig([]string{"127.0.0.1"}, "ks")
	_, err = fromConfig.Take(context.Background())
	assert.EqualError(t, err, "dial failed")
	assert.False(t, fromConfig.initialized)

	opened := &gocql.Session{}
	closed := false
	cassandraCloseSession = func(*gocql.Session) { closed = true }
	cassandraCreateSession = func(*gocql.ClusterConfig) (*gocql.Session, error) { return opened, nil }
	got, err = fromConfig.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, opened, got)
	again, err := fromConfig.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, got, again)
	assert.NoError(t, fromConfig.Shutdown(context.Background()))
	assert.True(t, closed)
	assert.False(t, fromConfig.initialized)
}

func TestCassandraStorage_Lifecycle(t *testing.T) {
	assert.Panics(t, func() { _, _ = NewCassandraStorage(context.Background(), nil) })
	opts0 := NewCassandraStorageOptions().SetConnectionManager(takeErrorManager{}).SetTimeProvider(&LocalTimeProvider{})
	assert.Equal(t, takeErrorManager{}, opts0.ConnectionManager)
	assert.NotNil(t, opts0.TimeProvider)
	_, err := NewCassandraStorage(context.Background(), NewCassandraStorageOptions())
	assert.ErrorIs(t, err, ErrKeyspaceEmpty)
	opts := NewCassandraStorageOptions().SetKeyspace("ks").SetTableName("")
	assert.ErrorIs(t, opts.Check(), ErrTableNameEmpty)
	opts.SetTableName("locks")
	assert.ErrorIs(t, opts.Check(), ErrConnectionManagerNil)

	restoreCassandra(t)
	cassandraExec = func(*gocql.Session, string) error { return errors.New("create failed") }
	_, err = NewCassandraStorage(context.Background(), &CassandraStorageOptions{
		ConnectionManager: NewCassandraConnectionManagerFromSession(&gocql.Session{}),
		Keyspace:          "ks",
		TableName:         "locks",
	})
	assert.EqualError(t, err, "create failed")

	_, err = NewCassandraStorage(context.Background(), &CassandraStorageOptions{
		ConnectionManager: takeErrorManager{},
		Keyspace:          "ks",
		TableName:         "locks",
	})
	assert.EqualError(t, err, "take failed")

	cassandraExec = func(*gocql.Session, string) error { return nil }
	s := newTestCassandra(t)
	assert.Equal(t, CassandraStorageName, s.GetName())
	assert.Len(t, s.Capabilities(), 4)
	now, err := s.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	plain, err := NewCassandraStorage(context.Background(), &CassandraStorageOptions{
		ConnectionManager: NewCassandraConnectionManagerFromSession(&gocql.Session{}),
		Keyspace:          "ks",
		TableName:         "locks",
	})
	assert.NoError(t, err)
	now, err = plain.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	assert.NoError(t, s.Close(context.Background()))
	_, err = (&CassandraStorage{}).Watch(context.Background(), "")
	assert.Error(t, err)
	local, err := (&LocalTimeProvider{}).GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, local.IsZero())
}

func TestCassandraStorage_Mutations(t *testing.T) {
	restoreCassandra(t)
	cassandraExec = func(*gocql.Session, string) error { return nil }
	s := newTestCassandra(t)
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	cassandraScanCAS = func(*gocql.Session, context.Context, string, ...any) (bool, error) { return true, nil }
	assert.NoError(t, s.CreateWithVersion(ctx, "id", 1, info))
	assert.NoError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))
	assert.NoError(t, s.DeleteWithVersion(ctx, "id", 1, info))

	cassandraScanCAS = func(*gocql.Session, context.Context, string, ...any) (bool, error) { return false, nil }
	assert.ErrorIs(t, s.CreateWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	cassandraScanCAS = func(*gocql.Session, context.Context, string, ...any) (bool, error) {
		return false, errors.New("cas failed")
	}
	assert.EqualError(t, s.CreateWithVersion(ctx, "id", 1, info), "cas failed")
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "cas failed")
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "cas failed")
}

func TestCassandraStorage_GetAndList(t *testing.T) {
	restoreCassandra(t)
	cassandraExec = func(*gocql.Session, string) error { return nil }
	s := newTestCassandra(t)
	ctx := context.Background()

	cassandraScan = func(_ *gocql.Session, _ context.Context, _ string, _ []any, dest ...any) error {
		return gocql.ErrNotFound
	}
	_, err := s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	cassandraScan = func(_ *gocql.Session, _ context.Context, _ string, _ []any, dest ...any) error {
		return errors.New("scan failed")
	}
	_, err = s.Get(ctx, "id")
	assert.EqualError(t, err, "scan failed")

	cassandraScan = func(_ *gocql.Session, _ context.Context, _ string, _ []any, dest ...any) error {
		*(dest[0].(*int64)) = 1
		*(dest[1].(*string)) = `{"owner_id":"owner"}`
		return nil
	}
	got, err := s.Get(ctx, "id")
	assert.NoError(t, err)
	assert.Contains(t, got, "owner")

	jsonText := (&storage.LockInformation{OwnerId: "owner"}).ToJsonString()
	cassandraIter = func(*gocql.Session, context.Context, string) cassandraIterator {
		return &fakeIter{err: errors.New("iter failed")}
	}
	_, err = s.List(ctx)
	assert.EqualError(t, err, "iter failed")

	cassandraIter = func(*gocql.Session, context.Context, string) cassandraIterator {
		return &fakeIter{values: []string{"not-json", jsonText}}
	}
	listed, err := s.List(ctx)
	assert.NoError(t, err)
	count := 0
	for listed.Next() {
		count++
		assert.Equal(t, "owner", listed.Value().OwnerId)
	}
	assert.Equal(t, 1, count)
}

// cqlFrame 按协议 v4 写一帧响应：版本字节带响应方向位。
func cqlFrame(stream uint16, op byte, body []byte) []byte {
	buf := make([]byte, 9+len(body))
	buf[0] = 0x84
	binary.BigEndian.PutUint16(buf[2:4], uint16(stream))
	buf[4] = op
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(body)))
	copy(buf[9:], body)
	return buf
}

func cqlShortString(s string) []byte {
	buf := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(s)))
	copy(buf[2:], s)
	return buf
}

func cqlInt(n int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(n))
	return buf
}

func cqlBytes(b []byte) []byte {
	return append(cqlInt(int32(len(b))), b...)
}

// cqlRows 返回一行 varchar 结果。system.local 需要至少一行，否则建连失败。
func cqlRows(column, value string) []byte {
	body := append([]byte{}, cqlInt(2)...)           // result kind: rows
	body = append(body, cqlInt(0x01)...)             // global_tables_spec
	body = append(body, cqlInt(1)...)                // column count
	body = append(body, cqlShortString("system")...) // keyspace
	body = append(body, cqlShortString("local")...)  // table
	body = append(body, cqlShortString(column)...)   // name
	body = append(body, []byte{0x00, 0x0D}...)       // varchar
	body = append(body, cqlInt(1)...)                // row count
	body = append(body, cqlBytes([]byte(value))...)
	return body
}

func cqlError(msg string) []byte {
	// ErrCodeServer = 0x0000，只带一段错误文本。
	return append(cqlInt(0), cqlShortString(msg)...)
}

func readCQLQuery(header []byte, body []byte) string {
	if header[4] != 0x07 || len(body) < 4 {
		return ""
	}
	n := int(binary.BigEndian.Uint32(body[:4]))
	if n < 0 || 4+n > len(body) {
		return ""
	}
	return string(body[4 : 4+n])
}

func serveCQL(conn net.Conn) {
	defer conn.Close()
	header := make([]byte, 9)
	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		bodyLen := int(binary.BigEndian.Uint32(header[5:9]))
		body := make([]byte, bodyLen)
		if bodyLen > 0 {
			if _, err := io.ReadFull(conn, body); err != nil {
				return
			}
		}
		stream := binary.BigEndian.Uint16(header[2:4])
		switch header[4] {
		case 0x05: // OPTIONS
			_, _ = conn.Write(cqlFrame(stream, 0x06, []byte{0, 0}))
		case 0x01: // STARTUP
			_, _ = conn.Write(cqlFrame(stream, 0x02, nil))
		case 0x07, 0x09, 0x0A: // QUERY / PREPARE / EXECUTE
			query := readCQLQuery(header, body)
			if strings.Contains(query, "system.local") {
				frame := cqlFrame(stream, 0x08, cqlRows("cluster_name", "test"))
				_, _ = conn.Write(frame)
			} else if strings.Contains(query, "system.peers") {
				_, _ = conn.Write(cqlFrame(stream, 0x08, append(append(append(cqlInt(2), cqlInt(0)...), cqlInt(0)...), cqlInt(0)...)))
			} else {
				_, _ = conn.Write(cqlFrame(stream, 0x00, cqlError("offline")))
			}
		default:
			_, _ = conn.Write(cqlFrame(stream, 0x02, nil))
		}
	}
}

func TestCassandraDefaultOperations_Delegate(t *testing.T) {
	_, err := cassandraCreateSession(gocql.NewCluster())
	assert.ErrorIs(t, err, gocql.ErrNoHosts)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveCQL(conn)
		}
	}()

	addr := ln.Addr().(*net.TCPAddr)
	cluster := gocql.NewCluster(addr.IP.String())
	cluster.Port = addr.Port
	cluster.ProtoVersion = 4
	cluster.DisableInitialHostLookup = true
	cluster.ConnectTimeout = 2 * time.Second
	cluster.Timeout = 2 * time.Second
	cluster.NumConns = 1
	session, err := cassandraCreateSession(cluster)
	if !assert.NoError(t, err) || session == nil {
		return
	}
	defer cassandraCloseSession(session)

	assert.Error(t, cassandraExec(session, "SELECT 1"))
	_, err = cassandraScanCAS(session, context.Background(), "SELECT 1")
	assert.Error(t, err)
	assert.Error(t, cassandraScan(session, context.Background(), "SELECT 1", nil, new(string)))
	iter := cassandraIter(session, context.Background(), "SELECT 1")
	assert.False(t, iter.Scan(new(string)))
	assert.Error(t, iter.Close())
}
