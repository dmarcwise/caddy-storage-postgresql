package caddypostgresql

import (
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func (s *PostgresStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string

		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "connection_string":
			s.ConnectionString = value
		case "instance_id":
			s.InstanceId = value
		case "debug":
			if d.NextArg() {
				return d.ArgErr()
			}
			s.Debug = true
		default:
			return d.ArgErr()
		}
	}

	return nil
}
