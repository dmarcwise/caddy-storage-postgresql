# Caddy Storage PostgresQL

> [!WARNING]
> This project is still a work in progress. Do not use in production.

A storage implementation for [Caddy](https://caddyserver.com/) that uses PostgreSQL as the backend.

- ✅ Supports all Caddy storage operations
- ✅ Directories semantics fully implemented (e.g. listing objects in a directory)
- ✅ Supports [storage export](https://caddyserver.com/docs/command-line#caddy-storage) (recursive listing of all
  files)
- ✅ Works in distributed Caddy setups thanks to distributed locking with fencing tokens ([pglock](https://github.com/cirello-io/pglock))
- ✅ Fully tested ([integration tests](https://github.com/dmarcwise/caddy-storage-postgresql/blob/main/storage_test.go) + [smoke tests and real export test](https://github.com/dmarcwise/caddy-storage-postgresql/blob/main/.github/workflows/test.yml))

## How to use

Build the module into your Caddy binary:

```shell
xcaddy build --with github.com/dmarcwise/caddy-storage-postgresql
```

Configure Caddy to use the PostgreSQL storage module. Example `Caddyfile`:

```caddy
{
    storage postgresql {
        dsn "postgres://user:password@localhost:5432/caddy?sslmode=disable"
    }
}
```

The `dsn` parameter is a [PostgreSQL connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING), either as a key/value string or as a connection URI.
