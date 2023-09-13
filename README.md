# Event Horizon with Bun

An **EventStore** implementation for [looplab/eventhorizon](https://pkg.go.dev/github.com/looplab/eventhorizon) powered by [BUN](https://bun.uptrace.dev/), a lightweight Go ORM for PostgreSQL, MySQL, ~~MSSQL~~, and SQLite.

## Notice

I'm fairly new to Go, so do not expect the code to be brilliant or very reliable. Please shoot constructive feedback, PRs and ideas.

## Todo

- [high] Implement snapshots
- [low] Make it work with MSSQL (it's my least priority b/c I never use it)

## Usage

Both the event store and the repo need first a BUN DB handle, see docs on how to connect to a DB:

- https://bun.uptrace.dev/guide/golang-orm.html#connecting-to-a-database
- https://bun.uptrace.dev/guide/drivers.html

See `test_helpers.go` and individual test files for more examples.

### Event Store

```go
config, err := pgx.ParseConfig("{schema}://{user}:{password}@{hostname}:{port}/{dbname}")
if err != nil {
  panic("could not parse dsn")
}
config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

sqldb := stdlib.OpenDB(*config)

db = bun.NewDB(sqldb, pgdialect.New())
store, err := ehbun.NewEventStore(*db)

...
```

### Repo

```go
config, err := pgx.ParseConfig("{schema}://{user}:{password}@{hostname}:{port}/{dbname}")
if err != nil {
  panic("could not parse dsn")
}
config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

sqldb := stdlib.OpenDB(*config)

db = bun.NewDB(sqldb, pgdialect.New())
repo, err := ehbun.NewRepo(*db)

...
```

## Tests

Provided is a Docker Compose environment to test the event store against PostgreSQL, MySQL and MSSQL.

An environment variable `DSN` is required to specify the connection details.

**All DSNs, except SQLite, must begin with a scheme!**

The reason is to be able to distinguish between PostgreSQL, MySQL and MSSQL.
Unfortunately, [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql#dsn-data-source-name) does not support schemes in the DSN, but it will be stripped automatically inside the tests.

The tests are a little bit slow because the tables are always reset (drop+create) in order to run a clean and updated database. 

```shell
DSN="{schema}://{user}:{password}@{hostname}:{port}/{dbname}" go test
```

#### PostgreSQL

```shell
DSN="postgres://postgres:postgres@postgres:5432/postgres?search_path=public&sslmode=disable" go test
```

For more options see https://bun.uptrace.dev/postgres/#pgx and https://github.com/jackc/pgx

#### MySQL

```shell
DSN="mysql://mysql:mysql@tcp(mysql:3306)/mysql" go test
```

For more options see https://github.com/go-sql-driver/mysql#dsn-data-source-name and https://bun.uptrace.dev/guide/drivers.html#mysql

#### MSSQL

MSSQL is not supported yet.

#### SQLite

```shell
DSN="file::memory:?cache=shared" go test
```