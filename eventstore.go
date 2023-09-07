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

package ehbun

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

// AggregateEvent - is the event store model that is persisted to the DB
type AggregateEvent struct {
	bun.BaseModel `bun:"table:event_store,alias:es"`

	ID            uuid.UUID              `bun:"id,pk,type:uuid,notnull"`
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
			return nil, fmt.Errorf("error while applying option: %v", err)
		}
	}

	if err := eventStore.db.DB.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping to DB: %v", err)
	}

	ctx := context.Background()
	err := eventStore.CreateTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not create event store table: %v", err)
	}

	return eventStore, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

// Save implements the Save method of the eventhorizon.EventStore interface.
func (eventStore *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	lenEvents := len(events)

	if lenEvents == 0 {
		return &eh.EventStoreError{
			Err: eh.ErrMissingEvents,
			Op:  eh.EventStoreOpSave,
		}
	}

	aggregateEvents := make([]AggregateEvent, lenEvents)
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

		// Create the AggregateEvent record for the DB.
		aggregateEvent, err := eventStore.newDBEvent(ctx, event)
		if err != nil {
			return err
		}

		aggregateEvents[i] = *aggregateEvent
	}

	if err := eventStore.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {

		_, err := tx.
			NewInsert().
			Model(&aggregateEvents).
			Exec(ctx)

		return err
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

	// Let the optional AggregateEvent handler handle the events.
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
		Model((*AggregateEvent)(nil)).
		Where("? = ?", bun.Ident("aggregate_id"), id).
		Where("? >= ?", bun.Ident("version"), version).
		Order("version ASC").
		Rows(ctx)

	if err != nil {
		return events, &eh.EventStoreError{
			Err:              err,
			Op:               eh.EventStoreOpLoad,
			AggregateID:      id,
			AggregateVersion: version,
		}
	}
	defer rows.Close()

	for rows.Next() {
		aggregateEvent := new(AggregateEvent)
		if err := eventStore.db.ScanRow(ctx, rows, aggregateEvent); err != nil {
			return events, &eh.EventStoreError{
				Err:              fmt.Errorf("could not scan row: %v", err),
				Op:               eh.EventStoreOpLoad,
				AggregateType:    aggregateEvent.AggregateType,
				AggregateID:      id,
				AggregateVersion: version,
			}
		}

		// Set concrete event data
		if data, err := eh.CreateEventData(aggregateEvent.EventType); err == nil {
			// Manually decode the raw event.
			if err := json.Unmarshal(aggregateEvent.RawData, data); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not unmarshal event data: %v", err),
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

// newDBEvent maps an eventhorizon.Event to an AggregateEvent
func (eventStore *EventStore) newDBEvent(ctx context.Context, event eh.Event) (*AggregateEvent, error) {

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

	return &AggregateEvent{
		ID:            uuid.New(),
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
	_, err := eventStore.db.
		NewCreateTable().
		Model((*AggregateEvent)(nil)).
		IfNotExists().
		Exec(ctx)

	return err
}
