package caddypostgresql

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"cirello.io/pglock"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(PostgresStorage{})
}

type PostgresStorage struct {
	logger *zap.Logger
	pool   *pgxpool.Pool
	locks  sync.Map // name -> *pglock.Lock
	pglock *pglock.Client

	ConnectionString string `json:"connection_string,omitempty"`
	ObjectsTable     string `json:"objects_table,omitempty"`
	LocksTable       string `json:"locks_table,omitempty"`
	InstanceId       string `json:"instance_id,omitempty"`
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

	if s.ConnectionString == "" {
		return fmt.Errorf("connection_string is required")
	}

	// Create PostgreSQL connection pool
	pool, err := pgxpool.New(ctx.Context, s.ConnectionString)
	if err != nil {
		s.logger.Error("could not create connection pool", zap.Error(err))
		return err
	}

	s.pool = pool

	// Initialize pglock

	if s.LocksTable == "" {
		s.LocksTable = "caddy_locks"
	}

	if s.InstanceId == "" {
		instanceId, err := caddy.InstanceID()
		if err != nil {
			s.logger.Error("could not get caddy instance id", zap.Error(err))
			return err
		}

		s.InstanceId = instanceId.String()
	}

	s.pglock, err = pglock.UnsafeNew(
		stdlib.OpenDBFromPool(s.pool),
		pglock.WithCustomTable(s.LocksTable),
		pglock.WithOwner(s.InstanceId),
		// TODO: add the other pglock options
	)

	if err != nil {
		s.logger.Error("could not create pglock client", zap.Error(err))
		return err
	}

	if err = s.pglock.TryCreateTable(); err != nil {
		s.logger.Error("could not create pglock table", zap.Error(err))
		return err
	}

	// Ensure storage objects table exists
	if s.ObjectsTable == "" {
		s.ObjectsTable = "caddy_certmagic_objects"
	}

	if err := s.ensureTableExists(ctx.Context); err != nil {
		s.logger.Error("could not ensure certmagic_data table exists", zap.Error(err))
		return err
	}

	return nil
}

func (s *PostgresStorage) ensureTableExists(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key TEXT PRIMARY KEY,
			value BYTEA,
			modified TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		)
	`, s.ObjectsTable))

	return err
}

func (s *PostgresStorage) Lock(ctx context.Context, name string) error {
	lock, err := s.pglock.AcquireContext(ctx, name)
	if err != nil {
		return fmt.Errorf("could not acquire pglock: %w", err)
	}

	s.locks.Store(name, lock)

	return nil
}

func (s *PostgresStorage) Unlock(ctx context.Context, name string) error {
	v, ok := s.locks.LoadAndDelete(name)
	if !ok {
		return fmt.Errorf("unlock without prior lock: %q", name)
	}

	lock := v.(*pglock.Lock)

	if err := s.pglock.ReleaseContext(ctx, lock); err != nil {
		return fmt.Errorf("could not release pglock: %w", err)
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
	// Release locks and clean locks mapping
	s.locks.Range(func(key, value any) bool {
		if lock, ok := value.(*pglock.Lock); ok {
			_ = lock.Close()
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
