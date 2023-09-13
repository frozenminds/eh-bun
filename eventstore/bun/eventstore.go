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
	domain "github.com/frozenminds/eh-bun/domain/event"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// EventStore is an eventhorizon.EventStore for Bun
type EventStore struct {
	db                    bun.DB
	eventHandlerAfterSave eh.EventHandler
	eventHandlerInTX      eh.EventHandler
	createTable           bool
	resetTable            bool
}

// NewEventStore creates a new EventStore with a bun.DB handle.
func NewEventStore(db bun.DB, options ...Option) (*EventStore, error) {
	return newEventStoreWithHandle(db, options...)
}

func newEventStoreWithHandle(db bun.DB, options ...Option) (*EventStore, error) {
	eventStore := &EventStore{
		db:          db,
		createTable: false,
		resetTable:  false,
	}

	for _, option := range options {
		if err := option(eventStore); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	if err := eventStore.db.DB.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping to DB: %w", err)
	}

	db.RegisterModel((*domain.EventStream)(nil))
	db.RegisterModel((*domain.EventStore)(nil))

	ctx := context.Background()

	if eventStore.createTable {
		err := eventStore.doCreateTable(ctx)
		if err != nil {
			return nil, err
		}
	}
	if eventStore.resetTable {
		err := eventStore.doResetTable(ctx)
		if err != nil {
			return nil, err
		}
	}

	return eventStore, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

func WithTableCreate() Option {
	return func(eventStore *EventStore) error {
		eventStore.createTable = true

		return nil
	}
}

func WithTableReset() Option {
	return func(eventStore *EventStore) error {
		eventStore.resetTable = true

		return nil
	}
}

// WithEventHandler adds an event handler that will be called after saving events.
// An example would be to add an event bus to publish events.
func WithEventHandler(eventHandler eh.EventHandler) Option {
	return func(eventStore *EventStore) error {
		if eventStore.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}

		if eventStore.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}

		eventStore.eventHandlerAfterSave = eventHandler

		return nil
	}
}

// WithEventHandlerInTX adds an event handler that will be called during saving of
// events. An example would be to add an outbox to further process events.
// For an outbox to be atomic it needs to use the same transaction as the save
// operation, which is passed down using the context.
func WithEventHandlerInTX(eventHandler eh.EventHandler) Option {
	return func(eventStore *EventStore) error {
		if eventStore.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}

		if eventStore.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}

		eventStore.eventHandlerInTX = eventHandler

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

	dbEvents := make([]domain.EventStore, lenEvents)
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
		strm := &domain.EventStream{
			ID:            lastEvent.AggregateID,
			Position:      lastEvent.Position,
			AggregateType: lastEvent.AggregateType,
			Version:       lastEvent.Version,
			UpdatedAt:     lastEvent.Timestamp,
		}

		insertQuery := tx.NewInsert().
			Model(strm)

		// MySQL has a slightly different syntax
		if eventStore.db.Dialect().Name() == dialect.MySQL {
			insertQuery.
				On("DUPLICATE KEY UPDATE").
				Set("position = VALUES(position)").
				Set("version = VALUES(version)").
				Set("updated_at = VALUES(updated_at)")
		} else {
			insertQuery.
				On("CONFLICT (id) DO UPDATE").
				Set("position = EXCLUDED.position").
				Set("version = EXCLUDED.version").
				Set("updated_at = EXCLUDED.updated_at")
		}

		if _, err := insertQuery.Exec(ctx); err != nil {
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
		Model((*domain.EventStore)(nil)).
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
		dbEvent := new(domain.EventStore)
		if err := eventStore.db.ScanRow(ctx, rows, dbEvent); err != nil {
			return nil, &eh.EventStoreError{
				Err:              fmt.Errorf("could not scan row: %w", err),
				Op:               eh.EventStoreOpLoad,
				AggregateType:    dbEvent.AggregateType,
				AggregateID:      id,
				AggregateVersion: version,
			}
		}

		// Set concrete event data
		if data, err := eh.CreateEventData(dbEvent.EventType); err == nil {
			// Manually decode the raw data.
			if err := json.Unmarshal(dbEvent.RawData, data); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not unmarshal evt data: %w", err),
					Op:               eh.EventStoreOpLoad,
					AggregateType:    dbEvent.AggregateType,
					AggregateID:      id,
					AggregateVersion: dbEvent.Version,
					Events:           events,
				}
			}
			dbEvent.Data = data
		}

		event := eh.NewEvent(
			dbEvent.EventType,
			dbEvent.Data,
			dbEvent.Timestamp,
			eh.ForAggregate(
				dbEvent.AggregateType,
				dbEvent.AggregateID,
				dbEvent.Version,
			),
			eh.WithMetadata(dbEvent.Metadata),
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

// newDBEvent maps an eventhorizon.Event to an event
func (eventStore *EventStore) newDBEvent(event eh.Event) (*domain.EventStore, error) {

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

	return &domain.EventStore{
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		RawData:       raw,
		Timestamp:     event.Timestamp(),
		Version:       event.Version(),
		Metadata:      event.Metadata(),
	}, nil
}

func (eventStore *EventStore) doCreateTable(ctx context.Context) error {

	if _, err := eventStore.db.
		NewCreateTable().
		Model((*domain.EventStream)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("could not create stream table: %w", err)
	}

	if _, err := eventStore.db.
		NewCreateTable().
		Model((*domain.EventStore)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("could not create event store table: %w", err)
	}

	return nil
}

func (eventStore *EventStore) doResetTable(ctx context.Context) error {
	if err := eventStore.db.
		ResetModel(ctx, (*domain.EventStream)(nil), (*domain.EventStore)(nil)); err != nil {
		return fmt.Errorf("could not reset model table: %w", err)
	}

	return nil
}
