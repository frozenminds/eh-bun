package bun

import (
	"encoding/json"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	"github.com/uptrace/bun"
	"time"
)

// EventStream - is a stream of events, contains the events for an aggregate.
type EventStream struct {
	bun.BaseModel `bun:"event_stream,alias:s"`

	ID            uuid.UUID        `bun:"id,type:uuid,pk"`
	AggregateType eh.AggregateType `bun:"aggregate_type,notnull"`
	Position      int64            `bun:"position,type:integer,notnull"`
	Version       int              `bun:"version,type:integer,notnull"`
	UpdatedAt     time.Time        `bun:""`
}

// EventStore - is the event store model that is persisted to the DB.
type EventStore struct {
	bun.BaseModel `bun:"table:event_store,alias:e"`

	Position      int64                  `bun:"position,pk,autoincrement"`
	AggregateID   uuid.UUID              `bun:"aggregate_id,type:uuid,notnull,unique:idx_aggregateid-version"`
	Version       int                    `bun:"version,notnull,unique:idx_aggregateid-version"`
	EventType     eh.EventType           `bun:"event_type,notnull"`
	RawData       json.RawMessage        `bun:"data"`
	AggregateType eh.AggregateType       `bun:"aggregate_type,notnull"`
	Timestamp     time.Time              `bun:"timestamp,notnull"`
	Metadata      map[string]interface{} `bun:"metadata"`
	// RawData mapped as eventhorizon.EventData
	Data eh.EventData `bun:"-"`
}
