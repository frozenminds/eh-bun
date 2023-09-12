package bun

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/mysqldialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
	"os"
	"strings"
)

func NewDB() (*bun.DB, error) {

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
