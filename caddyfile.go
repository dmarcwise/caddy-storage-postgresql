package caddypostgresql

import (
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func (s *PostgresStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "dsn":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.Dsn = d.Val()
				if d.NextArg() {
					return d.ArgErr()
				} // no extra args
			case "debug_locks":
				if d.NextArg() {
					return d.ArgErr()
				} // must be bare
				s.DebugLocks = true
			default:
				return d.Errf("unrecognized subdirective %q", d.Val())
			}
		}
	}

	return nil
}
