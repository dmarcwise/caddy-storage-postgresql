package caddypostgresql

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(PostgresStorage{})
}

type PostgresStorage struct {
	logger *zap.Logger
	pool   *pgxpool.Pool
	locks  sync.Map // name -> *pgxpool.Conn

	ConnectionString string `json:"connection_string,omitempty"`
}

func (s *PostgresStorage) CertMagicStorage() (certmagic.Storage, error) {
	return s, nil
}

func (PostgresStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.postgresql",
		New: func() caddy.Module {
			return NewPostgresStorage()
		},
	}
}

func NewPostgresStorage() *PostgresStorage {
	return &PostgresStorage{}
}

func (s *PostgresStorage) Provision(ctx caddy.Context) error {
	s.logger = ctx.Logger(s)

	pool, err := pgxpool.New(ctx.Context, s.ConnectionString)
	if err != nil {
		s.logger.Error("could not create connection pool", zap.Error(err))
		return err
	}

	s.pool = pool

	if err := s.ensureTableExists(ctx.Context); err != nil {
		s.logger.Error("could not ensure certmagic_data table exists", zap.Error(err))
		return err
	}

	return nil
}

func (s *PostgresStorage) ensureTableExists(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS certmagic_data (
			key TEXT PRIMARY KEY,
			value BYTEA,
			modified TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		)
	`)

	return err
}

func (s *PostgresStorage) Lock(ctx context.Context, name string) error {
	// Hash the name to a 64-bit integer key for the advisory lock
	// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
	key := hash64(name)

	// Acquire a dedicated connection, which will stay pinned until Unlock
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not acquire connection from pool: %w", err)
	}

	// Block until the advisory lock is obtained or ctx is canceled
	// TODO: timeout
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		conn.Release()
		return err
	}

	s.locks.Store(name, conn)

	return nil
}

func (s *PostgresStorage) Unlock(_ context.Context, name string) error {
	// Retrieve the connection associated with this lock
	v, ok := s.locks.Load(name)
	if !ok {
		return fmt.Errorf("unlock without prior lock: %q", name)
	}

	conn := v.(*pgxpool.Conn)

	// Always return the connection to the pool and forget about it
	defer func() {
		conn.Release()
		s.locks.Delete(name)
	}()

	key := hash64(name)

	// Use a non-cancelable context so that unlock is not interrupted
	ctx := context.Background()

	_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", key)

	// The unlock can still fail. We treat the connection as poisoned and
	// close the underlying session so that advisory locks are dropped automatically
	if err != nil {
		if raw := conn.Conn(); raw != nil {
			_ = raw.Close(ctx)
		}

		return err
	}

	return nil
}

func (s *PostgresStorage) Store(ctx context.Context, key string, value []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO certmagic_data (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE
		SET value = EXCLUDED.value, modified = CURRENT_TIMESTAMP
	`, key, value)

	return err
}

func (s *PostgresStorage) Load(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := s.pool.QueryRow(ctx, `
		SELECT value FROM certmagic_data WHERE key = $1
	`, key).Scan(&value)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fs.ErrNotExist
	}

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *PostgresStorage) Delete(ctx context.Context, key string) error {
	// TODO: should delete all keys in directory
	cmd, err := s.pool.Exec(ctx, `
		DELETE FROM certmagic_data WHERE key = $1
	`, key)

	if err != nil {
		return err
	}

	if cmd.RowsAffected() == 0 {
		return fs.ErrNotExist
	}

	return nil
}

func (s *PostgresStorage) Exists(ctx context.Context, key string) bool {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM certmagic_data WHERE key = $1)
	`, key).Scan(&exists)

	return err == nil && exists
}

func (s *PostgresStorage) List(ctx context.Context, path string, recursive bool) ([]string, error) {
	return []string{}, nil

	// TODO: implement me
}

func (s *PostgresStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	var info certmagic.KeyInfo

	err := s.pool.QueryRow(ctx, `
		SELECT octet_length(value), modified
		FROM certmagic_data
		WHERE key = $1
	`, key).Scan(&info.Size, &info.Modified)

	if errors.Is(err, pgx.ErrNoRows) {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	info.Key = key
	info.IsTerminal = true

	return info, nil
}

func (s *PostgresStorage) Cleanup() error {
	// Release all acquired connections and clean locks mapping
	s.locks.Range(func(key, value any) bool {
		if conn, ok := value.(*pgxpool.Conn); ok {
			conn.Release()
		}
		s.locks.Delete(key)
		return true
	})

	// Close all pool connections
	if s.pool != nil {
		s.pool.Close()
	}

	return nil
}

// Interface guards
var (
	_ caddy.StorageConverter = (*PostgresStorage)(nil)
	_ certmagic.Storage      = (*PostgresStorage)(nil)
	_ caddy.Provisioner      = (*PostgresStorage)(nil)
	_ caddyfile.Unmarshaler  = (*PostgresStorage)(nil)
	_ caddy.CleanerUpper     = (*PostgresStorage)(nil)
)
