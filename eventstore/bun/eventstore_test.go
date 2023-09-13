package bun_test

import (
	"context"
	ehbun "github.com/frozenminds/eh-bun"
	ehbunes "github.com/frozenminds/eh-bun/eventstore/bun"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/looplab/eventhorizon/eventstore"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"os"
	"testing"
)

func TestEventStore(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := ehbun.NewDB()
	if err != nil {
		t.Fatal("could not open db connection", err)
	}
	if db == nil {
		t.Fatal("could not open db connection")
	}

	store, err := ehbunes.NewEventStore(*db, ehbunes.WithTableReset())
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

	store, err := ehbunes.NewEventStore(*db, ehbunes.WithTableReset())
	if err != nil {
		t.Fatal("could not create a new event store:", err)
	}
	if store == nil {
		t.Fatal("there should be a event store")
	}
	defer store.Close()

	eventstore.Benchmark(t, store)
}
