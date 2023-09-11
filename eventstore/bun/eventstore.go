// Copyright (c) 2023 - Constantin Bejenaru <boby@frozenminds.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bun

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	"github.com/uptrace/bun"
	"time"
)

// stream - is a stream of events, contains the events for an aggregate
type stream struct {
	bun.BaseModel `bun:"event_stream,alias:s"`

	ID            uuid.UUID        `bun:"id,type:uuid,pk"`
	AggregateType eh.AggregateType `bun:"aggregate_type,notnull"`
	Position      int64            `bun:"position,type:integer,notnull"`
	Version       int              `bun:"version,type:integer,notnull"`
	UpdatedAt     time.Time        `bun:""`
}

// evt - is the event store model that is persisted to the DB
type evt struct {
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
	data eh.EventData `bun:"-"`
}

// EventStore is an eventhorizon.EventStore for Bun
type EventStore struct {
	db                    bun.DB
	eventHandlerAfterSave eh.EventHandler
	eventHandlerInTX      eh.EventHandler
}

// NewEventStore creates a new EventStore with a bun.DB handle.
func NewEventStore(db bun.DB, options ...Option) (*EventStore, error) {
	return newEventStoreWithHandle(db, options...)
}

func newEventStoreWithHandle(db bun.DB, options ...Option) (*EventStore, error) {
	eventStore := &EventStore{
		db: db,
	}

	for _, option := range options {
		if err := option(eventStore); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	if err := eventStore.db.DB.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping to DB: %w", err)
	}

	db.RegisterModel((*stream)(nil))
	db.RegisterModel((*evt)(nil))

	ctx := context.Background()

	err := eventStore.CreateTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not create evt store table: %w", err)
	}

	return eventStore, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

// WithEventHandler adds an event handler that will be called after saving events.
// An example would be to add an event bus to publish events.
func WithEventHandler(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		if s.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}

		if s.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}

		s.eventHandlerAfterSave = h

		return nil
	}
}

// WithEventHandlerInTX adds an event handler that will be called during saving of
// events. An example would be to add an outbox to further process events.
// For an outbox to be atomic it needs to use the same transaction as the save
// operation, which is passed down using the context.
func WithEventHandlerInTX(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		if s.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}

		if s.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}

		s.eventHandlerInTX = h

		return nil
	}
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (eventStore *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	lenEvents := len(events)

	if lenEvents == 0 {
		return &eh.EventStoreError{
			Err: eh.ErrMissingEvents,
			Op:  eh.EventStoreOpSave,
		}
	}

	dbEvents := make([]evt, lenEvents)
	aggregateId := events[0].AggregateID()
	aggregateType := events[0].AggregateType()

	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != aggregateId {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateIDs,
				Op:               eh.EventStoreOpSave,
				AggregateType:    aggregateType,
				AggregateID:      aggregateId,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Only accept events that apply to the correct aggregate type.
		if event.AggregateType() != aggregateType {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateTypes,
				Op:               eh.EventStoreOpSave,
				AggregateType:    aggregateType,
				AggregateID:      aggregateId,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return &eh.EventStoreError{
				Err:              eh.ErrIncorrectEventVersion,
				Op:               eh.EventStoreOpSave,
				AggregateType:    aggregateType,
				AggregateID:      aggregateId,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Create the evt record for the DB.
		dbEvent, err := eventStore.newDBEvent(event)
		if err != nil {
			return &eh.EventStoreError{
				Err:              fmt.Errorf("could not map to DB event: %w", err),
				Op:               eh.EventStoreOpSave,
				AggregateType:    aggregateType,
				AggregateID:      aggregateId,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		dbEvents[i] = *dbEvent
	}

	if err := eventStore.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {

		if _, err := tx.
			NewInsert().
			Model(&dbEvents).
			Returning("position").
			Exec(ctx); err != nil {
			return fmt.Errorf("could not insert events: %w", err)
		}

		// Grab the last event to use for position, returned from the insert above.
		lastEvent := dbEvents[len(dbEvents)-1]

		// Store the stream, based on the last event for position etc.
		strm := &stream{
			ID:            lastEvent.AggregateID,
			Position:      lastEvent.Position,
			AggregateType: lastEvent.AggregateType,
			Version:       lastEvent.Version,
			UpdatedAt:     lastEvent.Timestamp,
		}
		if _, err := tx.NewInsert().
			Model(strm).
			On("CONFLICT (id) DO UPDATE").
			Set("position = EXCLUDED.position").
			Set("version = EXCLUDED.version").
			Set("updated_at = EXCLUDED.updated_at").
			Exec(ctx); err != nil {
			return fmt.Errorf("could not update stream: %w", err)
		}

		return nil
	}); err != nil {
		return &eh.EventStoreError{
			Err:              err,
			Op:               eh.EventStoreOpSave,
			AggregateType:    aggregateType,
			AggregateID:      aggregateId,
			AggregateVersion: originalVersion,
			Events:           events,
		}
	}

	// Let the optional evt handler handle the events.
	if eventStore.eventHandlerAfterSave != nil {
		for _, event := range events {
			if err := eventStore.eventHandlerAfterSave.HandleEvent(ctx, event); err != nil {
				return &eh.EventHandlerError{
					Err:   err,
					Event: event,
				}
			}
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (eventStore *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	return eventStore.LoadFrom(ctx, id, 1)
}

// LoadFrom implements the LoadFrom method of the eventhorizon.EventStore interface.
func (eventStore *EventStore) LoadFrom(ctx context.Context, id uuid.UUID, version int) ([]eh.Event, error) {
	var events []eh.Event

	rows, err := eventStore.db.
		NewSelect().
		Model((*evt)(nil)).
		Where("? = ?", bun.Ident("aggregate_id"), id).
		Where("? >= ?", bun.Ident("version"), version).
		Order("version ASC").
		Rows(ctx)

	if err != nil {
		return nil, &eh.EventStoreError{
			Err:              err,
			Op:               eh.EventStoreOpLoad,
			AggregateID:      id,
			AggregateVersion: version,
		}
	}
	defer rows.Close()

	for rows.Next() {
		aggregateEvent := new(evt)
		if err := eventStore.db.ScanRow(ctx, rows, aggregateEvent); err != nil {
			return nil, &eh.EventStoreError{
				Err:              fmt.Errorf("could not scan row: %w", err),
				Op:               eh.EventStoreOpLoad,
				AggregateType:    aggregateEvent.AggregateType,
				AggregateID:      id,
				AggregateVersion: version,
			}
		}

		// Set concrete evt data
		if data, err := eh.CreateEventData(aggregateEvent.EventType); err == nil {
			// Manually decode the raw evt.
			if err := json.Unmarshal(aggregateEvent.RawData, data); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not unmarshal evt data: %w", err),
					Op:               eh.EventStoreOpLoad,
					AggregateType:    aggregateEvent.AggregateType,
					AggregateID:      id,
					AggregateVersion: aggregateEvent.Version,
					Events:           events,
				}
			}
			aggregateEvent.data = data
		}

		event := eh.NewEvent(
			aggregateEvent.EventType,
			aggregateEvent.data,
			aggregateEvent.Timestamp,
			eh.ForAggregate(
				aggregateEvent.AggregateType,
				aggregateEvent.AggregateID,
				aggregateEvent.Version,
			),
			eh.WithMetadata(aggregateEvent.Metadata),
		)

		events = append(events, event)
	}

	if len(events) == 0 {
		return nil, &eh.EventStoreError{
			Err:         eh.ErrAggregateNotFound,
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	return events, nil
}

// Close implements the Close method of the eventhorizon.EventStore interface.
func (eventStore *EventStore) Close() error {
	return eventStore.db.Close()
}

// newDBEvent maps an eventhorizon.Event to an evt
func (eventStore *EventStore) newDBEvent(event eh.Event) (*evt, error) {

	raw, err := json.Marshal(event.Data())

	if err != nil {
		return nil, &eh.EventStoreError{
			Err:              err,
			Op:               eh.EventStoreOpSave,
			AggregateID:      event.AggregateID(),
			AggregateVersion: event.Version(),
			AggregateType:    event.AggregateType(),
		}
	}

	return &evt{
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		RawData:       raw,
		Timestamp:     event.Timestamp(),
		Version:       event.Version(),
		Metadata:      event.Metadata(),
	}, nil
}

func (eventStore *EventStore) CreateTables(ctx context.Context) error {

	if _, err := eventStore.db.
		NewCreateTable().
		Model((*stream)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("could not create stream table: %w", err)
	}

	if _, err := eventStore.db.
		NewCreateTable().
		Model((*evt)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("could not create event store table: %w", err)
	}

	return nil
}
