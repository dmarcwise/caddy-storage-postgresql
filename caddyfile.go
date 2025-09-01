package caddypostgresql

import (
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func (s *PostgresStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "connection_string":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.ConnectionString = d.Val()
				if d.NextArg() {
					return d.ArgErr()
				} // no extra args
			case "instance_id":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.InstanceId = d.Val()
				if d.NextArg() {
					return d.ArgErr()
				}
			case "debug":
				if d.NextArg() {
					return d.ArgErr()
				} // must be bare
				s.Debug = true
			default:
				return d.Errf("unrecognized subdirective %q", d.Val())
			}
		}
	}

	return nil
}
