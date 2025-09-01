package caddypostgresql

import "strings"

func splitKey(k string) (parent, name string) {
	if k == "" {
		return "", ""
	}
	i := strings.LastIndexByte(k, '/')
	if i < 0 {
		return "", k
	}
	return k[:i], k[i+1:]
}

func join(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
