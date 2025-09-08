package caddypostgresql

import (
	"context"
	"database/sql"
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
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(PostgresStorage{})
}

type PostgresStorage struct {
	logger *zap.Logger
	db     *sql.DB
	pglock *pglock.Client

	locks  map[string]*pglock.Lock
	mutex  sync.Mutex // protects locks map
	closed bool

	Dsn        string `json:"dsn,omitempty"`
	DebugLocks bool   `json:"debug_locks,omitempty"`
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
	return &PostgresStorage{
		locks: make(map[string]*pglock.Lock),
	}
}

func (s *PostgresStorage) Provision(ctx caddy.Context) error {
	// Initialize locks map if not already initialized
	s.mutex.Lock()
	if s.locks == nil {
		s.locks = make(map[string]*pglock.Lock)
	}
	s.mutex.Unlock()

	s.logger = ctx.Logger(s)

	if s.Dsn == "" {
		return fmt.Errorf("connection_string is required")
	}

	// Replace placeholders in DSN
	s.Dsn = caddy.NewReplacer().ReplaceAll(s.Dsn, "")

	// Create PostgreSQL connection pool
	db, err := sql.Open("postgres", s.Dsn)
	if err != nil {
		s.logger.Error("could not create connection pool", zap.Error(err))
		return err
	}

	s.db = db

	// Initialize pglock
	s.logger.Debug("initializing pglock...")

	instanceId, err := caddy.InstanceID()
	if err != nil {
		s.logger.Error("could not get caddy instance id", zap.Error(err))
		return err
	}

	s.pglock, err = pglock.UnsafeNew(
		s.db,
		pglock.WithCustomTable("caddy_locks"),
		pglock.WithOwner(instanceId.String()),
		pglock.WithLevelLogger(pglockLogger{isEnabled: s.DebugLocks, logger: s.logger}),
	)

	if err != nil {
		s.logger.Error("could not create pglock client", zap.Error(err))
		return err
	}

	if err = s.pglock.TryCreateTable(); err != nil {
		s.logger.Error("could not create pglock table", zap.Error(err))
		return err
	}

	s.logger.Debug("pglock initialized")

	s.logger.Debug("ensuring caddy_certmagic_objects table exists...")

	// Ensure storage objects table exists
	if err := s.ensureTableExists(ctx.Context); err != nil {
		s.logger.Error("could not ensure caddy_certmagic_objects table exists", zap.Error(err))
		return err
	}

	s.logger.Debug("ensured caddy_certmagic_objects table exists")

	return nil
}

type pglockLogger struct {
	isEnabled bool
	logger    *zap.Logger
}

func (l pglockLogger) Debug(msg string, args ...any) {
	if l.isEnabled {
		l.logger.Debug("pglock: " + fmt.Sprintf(msg, args...))
	}
}

func (l pglockLogger) Error(msg string, args ...any) {
	if l.isEnabled {
		l.logger.Error("pglock: " + fmt.Sprintf(msg, args...))
	}
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

	_, err := s.db.ExecContext(ctx, createQuery)
	if err != nil {
		return err
	}

	createIndexQuery := `
		CREATE INDEX IF NOT EXISTS caddy_certmagic_objects_parent_like_idx
		ON caddy_certmagic_objects (parent text_pattern_ops)
	`

	_, err = s.db.ExecContext(ctx, createIndexQuery)
	return err
}

func (s *PostgresStorage) Lock(ctx context.Context, name string) error {
	s.logger.Debug("acquiring lock", zap.String("name", name))

	s.mutex.Lock()

	// Check if we are closed/closing
	if s.closed {
		s.logger.Debug("storage is closed, cannot acquire lock", zap.String("name", name))
		s.mutex.Unlock()
		return fmt.Errorf("storage is being cleaned up/has been cleaned up, cannot acquire new locks")
	}

	s.mutex.Unlock()

	lock, err := s.pglock.AcquireContext(ctx, name)

	if err != nil {
		return fmt.Errorf("could not acquire pglock: %w", err)
	}

	s.mutex.Lock()

	// Double-check we haven't been closed while acquiring the lock
	if s.closed {
		s.mutex.Unlock()

		s.logger.Debug("acquired lock, but storage is closed, releasing lock", zap.String("name", name))

		// Release the lock we just acquired since we're shutting down
		_ = s.pglock.ReleaseContext(ctx, lock)
		return fmt.Errorf("storage is being cleaned up/has been cleaned up, cannot acquire new locks")
	}

	s.locks[name] = lock
	s.mutex.Unlock()

	return nil
}

func (s *PostgresStorage) Unlock(ctx context.Context, name string) error {
	s.logger.Debug("releasing lock", zap.String("name", name))

	s.mutex.Lock()

	if s.closed {
		s.mutex.Unlock()
		// Already unlocked in cleanup
		return nil
	}

	lock, ok := s.locks[name]
	delete(s.locks, name)
	s.mutex.Unlock()

	if !ok {
		return fmt.Errorf("unlock without prior lock: %q", name)
	}

	err := s.pglock.ReleaseContext(ctx, lock)

	if err != nil && !errors.Is(err, pglock.ErrLockAlreadyReleased) {
		return fmt.Errorf("could not release pglock: %w", err)
	}

	return nil
}

func (s *PostgresStorage) Store(ctx context.Context, key string, value []byte) error {
	s.logger.Debug("storing key", zap.String("key", key))

	parent, name := splitKey(key)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

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
			s.logger.Debug("ensuring directory exists", zap.String("directory", join(curParent, part)))

			var insertQuery = `
				INSERT INTO caddy_certmagic_objects (parent, name, is_file, value)
				VALUES ($1, $2, false, NULL)
				ON CONFLICT (parent, name) DO NOTHING
			`

			if _, err := tx.ExecContext(ctx, insertQuery, curParent, part); err != nil {
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
	s.logger.Debug("upserting file", zap.String("file", key))

	var upsertQuery = `
		INSERT INTO caddy_certmagic_objects (parent, name, is_file, value)
		VALUES ($1, $2, true, $3)
		ON CONFLICT (parent, name)
		DO UPDATE SET is_file = EXCLUDED.is_file,
		              value = EXCLUDED.value,
		              modified = now()
	`

	if _, err := tx.ExecContext(ctx, upsertQuery, parent, name, value); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *PostgresStorage) Load(ctx context.Context, key string) ([]byte, error) {
	s.logger.Debug("loading key", zap.String("key", key))

	parent, name := splitKey(key)

	query := `
		SELECT value FROM caddy_certmagic_objects
		WHERE parent = $1 AND name = $2 AND is_file = true
	`

	var value []byte
	if err := s.db.QueryRowContext(ctx, query, parent, name).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}

	return value, nil
}

func (s *PostgresStorage) Delete(ctx context.Context, key string) error {
	s.logger.Debug("deleting key", zap.String("key", key))

	parent, name := splitKey(key)

	// Delete the key and all the descendants
	query := `DELETE FROM caddy_certmagic_objects
		WHERE (parent = $1 AND name = $2)
		OR parent = $3
		OR parent LIKE $3 || '/%'
	`

	_, err := s.db.ExecContext(ctx, query, parent, name, key)
	if err != nil {
		return err
	}

	return nil
}

func (s *PostgresStorage) Exists(ctx context.Context, key string) bool {
	s.logger.Debug("checking existence of key", zap.String("key", key))

	parent, name := splitKey(key)

	query := "SELECT EXISTS (SELECT 1 FROM caddy_certmagic_objects WHERE parent = $1 AND name = $2)"

	var ok bool
	if err := s.db.QueryRowContext(ctx, query, parent, name).Scan(&ok); err != nil {
		return false
	}

	return ok
}

func (s *PostgresStorage) List(ctx context.Context, path string, recursive bool) ([]string, error) {
	s.logger.Debug("listing keys", zap.String("path", path), zap.Bool("recursive", recursive))

	var rows *sql.Rows

	if recursive {
		if path == "" {
			// Query for all entries
			query := "SELECT parent, name FROM caddy_certmagic_objects ORDER BY parent, name"

			var err error
			rows, err = s.db.QueryContext(ctx, query)
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
			rows, err = s.db.QueryContext(ctx, query, path, path+"/%")
			if err != nil {
				return nil, err
			}
			defer rows.Close()
		}
	} else {
		query := "SELECT parent, name FROM caddy_certmagic_objects WHERE parent = $1 ORDER BY parent, name"

		var err error
		rows, err = s.db.QueryContext(ctx, query, path)
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
	s.logger.Debug("gathering stats for key", zap.String("key", key))

	parent, name := splitKey(key)

	query := `
		SELECT CASE WHEN is_file THEN octet_length(value) ELSE 0 END, modified, is_file
		FROM caddy_certmagic_objects
		WHERE parent = $1 AND name = $2
	`

	var size int64
	var mod time.Time
	var isFile bool
	if err := s.db.QueryRowContext(ctx, query, parent, name).Scan(&size, &mod, &isFile); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
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
	s.logger.Debug("cleanup requested, releasing all locks and closing connections")

	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		s.logger.Debug("cleanup already in progress or completed")
		return nil
	}

	s.closed = true

	// Clean locks mapping
	locks := s.locks
	s.locks = nil

	s.mutex.Unlock()

	// Release all held locks
	for _, l := range locks {
		_ = l.Close()
	}

	// Close all pool connections
	if s.db != nil {
		s.db.Close()
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
