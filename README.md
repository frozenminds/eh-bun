# Event Horizon with Bun

An **EventStore** implementation for [looplab/eventhorizon](https://pkg.go.dev/github.com/looplab/eventhorizon) powered by [BUN](https://bun.uptrace.dev/), a lightweight Go ORM for PostgreSQL, MySQL, ~~MSSQL~~, and SQLite.


## Tests

**All DSNs, except SQLite, must begin with a scheme!**

The reason is to be able to distinguish between Postgres, MySQL and MSSQL.
Unfortunately, [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql#dsn-data-source-name) does not support schemes in the DSN, but it will be stripped automatically inside the tests.

#### Postgres

```shell
DSN="postgres://{user}:{password}@{hostname}:{port}/{dbname}?search_path=public&sslmode=disable" go test

# example

DSN="postgres://postgres:postgres@postgres:5432/postgres?search_path=public&sslmode=disable" go test
```

#### MySQL

```shell
DSN="postgres://{user}:{password}@{hostname}:{port}/{dbname}" go test

# example

DSN="mysql://mysql:mysql@tcp(mysql:3306)/mysql" go test
```

For more options see https://github.com/go-sql-driver/mysql#dsn-data-source-name

#### MSSQL

MSSQL is not supported yet.

#### SQLite

```shell
DSN="file::memory:?cache=shared" go test
```