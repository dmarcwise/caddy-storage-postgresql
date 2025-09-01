package caddypostgresql

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"time"

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
		pglock.WithCustomTable("caddy_locks"),
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
	if err := s.ensureTableExists(ctx.Context); err != nil {
		s.logger.Error("could not ensure certmagic_data table exists", zap.Error(err))
		return err
	}

	return nil
}

func (s *PostgresStorage) ensureTableExists(ctx context.Context) error {
	createQuery := `
		CREATE TABLE IF NOT EXISTS caddy_certmagic_objects (
			parent    text    NOT NULL,
			name      text    NOT NULL,
			is_file   boolean NOT NULL,
			value     bytea,
			modified  timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (parent, name),
		    CONSTRAINT caddy_certmagic_objects_chk CHECK (
				(is_file = true AND value IS NOT NULL) OR
				(is_file = false AND value IS NULL)
			)
		)
	`

	_, err := s.pool.Exec(ctx, createQuery)
	if err != nil {
		return err
	}

	createIndexQuery := `
		CREATE INDEX IF NOT EXISTS caddy_certmagic_objects_parent_like_idx
		ON caddy_certmagic_objects (parent text_pattern_ops)
	`

	_, err = s.pool.Exec(ctx, createIndexQuery)
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
	parent, name := splitKey(key)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback(ctx) }()

	// TODO: disallow converting a directory to a file and vice versa?

	// Ensure records for the directories exist.
	// For example, a parent of "a/b/c" should create:
	// | parent | name | is_file | value | modified |
	// |--------|------|---------|-------|----------|
	// | ""     | "a"  | false   | NULL  | now()    |
	// | "a"    | "b"  | false   | NULL  | now()    |
	// | "a/b"  | "c"  | false   | NULL  | now()    |
	// if they do not already exist.
	if parent != "" {
		parts := strings.Split(parent, "/")
		curParent := ""
		for _, part := range parts {
			var insertQuery = `
				INSERT INTO caddy_certmagic_objects (parent, name, is_file, value)
				VALUES ($1, $2, false, NULL)
				ON CONFLICT (parent, name) DO NOTHING
			`

			if _, err := tx.Exec(ctx, insertQuery, curParent, part); err != nil {
				return err
			}

			curParent = join(curParent, part)
		}
	}

	// Upsert the file row.
	// For example, for a key of "a/b/c/d.txt", this will create or update:
	// | parent | name   | is_file | value  | modified |
	// |--------|--------|---------|--------|----------|
	// | "a/b/c"| "d.txt"| true    | <data> | now()    |
	var upsertQuery = `
		INSERT INTO caddy_certmagic_objects (parent, name, is_file, value)
		VALUES ($1, $2, true, $3)
		ON CONFLICT (parent, name)
		DO UPDATE SET is_file = EXCLUDED.is_file,
		              value = EXCLUDED.value,
		              modified = now()
	`

	if _, err := tx.Exec(ctx, upsertQuery, parent, name, value); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *PostgresStorage) Load(ctx context.Context, key string) ([]byte, error) {
	parent, name := splitKey(key)

	query := `
		SELECT value FROM caddy_certmagic_objects
		WHERE parent = $1 AND name = $2 AND is_file = true
	`

	var value []byte
	if err := s.pool.QueryRow(ctx, query, parent, name).Scan(&value); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}

	return value, nil
}

func (s *PostgresStorage) Delete(ctx context.Context, key string) error {
	parent, name := splitKey(key)

	// Delete the key and all the descendants
	query := `DELETE FROM caddy_certmagic_objects
		WHERE (parent = $1 AND name = $2)
		OR parent = $3
		OR parent LIKE $3 || '/%'
	`

	_, err := s.pool.Exec(ctx, query, parent, name, key)
	if err != nil {
		return err
	}

	return nil
}

func (s *PostgresStorage) Exists(ctx context.Context, key string) bool {
	parent, name := splitKey(key)

	query := "SELECT EXISTS (SELECT 1 FROM caddy_certmagic_objects WHERE parent = $1 AND name = $2)"

	var ok bool
	if err := s.pool.QueryRow(ctx, query, parent, name).Scan(&ok); err != nil {
		return false
	}

	return ok
}

func (s *PostgresStorage) List(ctx context.Context, path string, recursive bool) ([]string, error) {
	var rows pgx.Rows

	if recursive {
		if path == "" {
			// Query for all entries
			query := "SELECT parent, name FROM caddy_certmagic_objects ORDER BY parent, name"

			var err error
			rows, err = s.pool.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
		} else {
			// Query for all entries where parent equals path OR parent starts with path + "/"
			query := `
				SELECT parent, name FROM caddy_certmagic_objects
				WHERE parent = $1 OR parent LIKE $2
				ORDER BY parent, name
			`

			var err error
			rows, err = s.pool.Query(ctx, query, path, path+"/%")
			if err != nil {
				return nil, err
			}
			defer rows.Close()
		}
	} else {
		query := "SELECT parent, name FROM caddy_certmagic_objects WHERE parent = $1 ORDER BY parent, name"

		var err error
		rows, err = s.pool.Query(ctx, query, path)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
	}

	var results []string
	for rows.Next() {
		var parent, name string
		if err := rows.Scan(&parent, &name); err != nil {
			return nil, err
		}

		fullPath := join(parent, name)
		results = append(results, fullPath)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *PostgresStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	parent, name := splitKey(key)

	query := `
		SELECT CASE WHEN is_file THEN octet_length(value) ELSE 0 END, modified, is_file
		FROM caddy_certmagic_objects
		WHERE parent = $1 AND name = $2
	`

	var size int64
	var mod time.Time
	var isFile bool
	if err := s.pool.QueryRow(ctx, query, parent, name).Scan(&size, &mod, &isFile); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return certmagic.KeyInfo{}, fs.ErrNotExist
		}

		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   mod,
		Size:       size,
		IsTerminal: isFile,
	}, nil
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
