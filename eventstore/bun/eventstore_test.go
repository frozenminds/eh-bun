package bun_test

import (
	"context"
	"database/sql"
	"fmt"
	ehbun "github.com/frozenminds/eh-bun/eventstore/bun"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/looplab/eventhorizon/eventstore"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/mysqldialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
	"os"
	"strings"
	"testing"
)

func newDB() (*bun.DB, error) {

	var db *bun.DB
	var err error

	// A DSN must always begin with a scheme for tests
	dsnRaw, ok := os.LookupEnv("DSN")
	if !ok {
		return nil, fmt.Errorf("you must provide a dsn")
	}

	switch {
	case strings.HasPrefix(dsnRaw, "postgres://"):
		fmt.Println("Connecting to Postgres")
		// Postgres via pgx supports a full DSN including scheme
		config, err := pgx.ParseConfig(dsnRaw)
		if err != nil {
			return nil, err
		}
		config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

		sqldb := stdlib.OpenDB(*config)

		db = bun.NewDB(sqldb, pgdialect.New())

	case strings.HasPrefix(dsnRaw, "mysql://"):
		fmt.Println("Connecting to MySQL")
		// The DSN for MySQL does not work with a scheme
		dsnMysql, ok := strings.CutPrefix(dsnRaw, "mysql://")
		if !ok {
			return nil, fmt.Errorf("could not remove scheme from mysql dsn")
		}
		sqldb, err := sql.Open("mysql", dsnMysql)
		if err != nil {
			return nil, err
		}

		db = bun.NewDB(sqldb, mysqldialect.New())
	case strings.HasPrefix(dsnRaw, "sqlserver://"):
		return nil, fmt.Errorf("mssql is not supported")

	case strings.HasPrefix(dsnRaw, "file::memory"):
		fmt.Println("Connecting to SQLite")
		sqldb, err := sql.Open(sqliteshim.ShimName, dsnRaw)
		if err != nil {
			return nil, err
		}

		db = bun.NewDB(sqldb, sqlitedialect.New())
	}

	return db, err
}
func TestEventStore(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := newDB()
	if err != nil {
		t.Fatal("could not open db connection", err)
	}
	if db == nil {
		t.Fatal("could not open db connection")
	}

	store, err := ehbun.NewEventStore(*db)
	if err != nil {
		t.Fatal("could not create a new evt store:", err)
	}
	if store == nil {
		t.Fatal("there should be a evt store")
	}

	eventstore.AcceptanceTest(t, store, context.Background())

	if err := store.Close(); err != nil {
		t.Error("could not close evt store:", err)
	}
}

func BenchmarkEventStore(t *testing.B) {
	config, err := pgx.ParseConfig(os.Getenv("DSN_POSTGRES"))
	if err != nil {
		t.Fatalf("could not parse Postgres config: %v", err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	sqldb := stdlib.OpenDB(*config)
	db := bun.NewDB(sqldb, pgdialect.New())
	if db == nil {
		t.Fatal("could not open db connection")
	}

	store, err := ehbun.NewEventStore(*db)
	if err != nil {
		t.Fatal("could not create a new evt store:", err)
	}
	if store == nil {
		t.Fatal("there should be a evt store")
	}
	defer store.Close()

	eventstore.Benchmark(t, store)
}
