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
	"fmt"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// Repo can be an eventhorizon.ReadRepo, eventhorizon.WriteRepo or eventhorizon.ReadWriteRepo for Bun.
// Acts as a base/abstract for your custom entity implementation.
type Repo struct {
	db                    bun.DB
	model                 interface{}
	mapDbEntityToEhEntity func(m interface{}) eh.Entity
	mapEhEntityToDbEntity func(e interface{}) interface{}
	createTable           bool
	truncateTable         bool
}

// NewRepo creates a new Repo with a bun.DB handle.
func NewRepo(db bun.DB, model interface{}, mapDbEntityToEhEntity func(m interface{}) eh.Entity, mapEhEntityToDbEntity func(e interface{}) interface{}, options ...Option) (*Repo, error) {
	return newRepoWithHandle(db, model, mapDbEntityToEhEntity, mapEhEntityToDbEntity, options...)
}

func newRepoWithHandle(db bun.DB, model interface{}, mapDbEntityToEhEntity func(m interface{}) eh.Entity, mapEhEntityToDbEntity func(e interface{}) interface{}, options ...Option) (*Repo, error) {
	r := &Repo{
		db:                    db,
		model:                 model,
		mapDbEntityToEhEntity: mapDbEntityToEhEntity,
		mapEhEntityToDbEntity: mapEhEntityToDbEntity,
		createTable:           false,
		truncateTable:         false,
	}

	for _, option := range options {
		if err := option(r); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	if err := r.db.DB.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping to DB: %w", err)
	}

	db.RegisterModel(r.model)

	ctx := context.Background()

	if r.createTable {
		err := r.doCreateTable(ctx)
		if err != nil {
			return nil, err
		}
	}
	if r.truncateTable {
		err := r.doTruncateTable(ctx)
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

// Option is an option setter used to configure creation.
type Option func(*Repo) error

func WithTableCreate() Option {
	return func(r *Repo) error {
		r.createTable = true

		return nil
	}
}
func WithTableTruncate() Option {
	return func(r *Repo) error {
		r.truncateTable = true

		return nil
	}
}

func (r Repo) InnerRepo(ctx context.Context) eh.ReadRepo {
	return nil
}

func (r Repo) Find(ctx context.Context, uuid uuid.UUID) (eh.Entity, error) {

	model := r.model
	err := r.db.
		NewSelect().
		Model(model).
		Where("id = ?", uuid).
		Scan(ctx)

	if err != nil {
		return nil, &eh.RepoError{
			Err:      eh.ErrEntityNotFound,
			Op:       eh.RepoOpFind,
			EntityID: uuid,
		}
	}

	entity := r.mapDbEntityToEhEntity(model)

	return entity, nil
}

func (r Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	var entities []eh.Entity
	model := r.model

	rows, err := r.db.
		NewSelect().
		Model(model).
		Rows(ctx)

	if err != nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("could not find: %w", err),
			Op:  eh.RepoOpFindAll,
		}
	}
	defer rows.Close()

	for rows.Next() {
		model := r.model
		if err := r.db.ScanRow(ctx, rows, model); err != nil {
			return nil, &eh.RepoError{
				Err: fmt.Errorf("could not scan row: %w", err),
				Op:  eh.RepoOpFindAll,
			}
		}
		entities = append(entities, r.mapDbEntityToEhEntity(model))
	}

	return entities, nil
}

func (r Repo) Save(ctx context.Context, entity eh.Entity) error {
	id := entity.EntityID()
	if id == uuid.Nil {
		return &eh.RepoError{
			Err: fmt.Errorf("missing entity ID"),
			Op:  eh.RepoOpSave,
		}
	}

	dbEntity := r.mapEhEntityToDbEntity(entity)

	upsert := "CONFLICT (id) DO UPDATE"
	if r.db.Dialect().Name() == dialect.MySQL {
		upsert = "DUPLICATE KEY UPDATE"
	}

	_, err := r.db.
		NewInsert().
		Model(dbEntity).
		On(upsert).
		Exec(ctx)

	if err != nil {
		return &eh.RepoError{
			Err:      fmt.Errorf("could not save/update: %w", err),
			Op:       eh.RepoOpSave,
			EntityID: entity.EntityID(),
		}
	}

	return nil
}

func (r Repo) Remove(ctx context.Context, id uuid.UUID) error {
	model := r.model

	res, err := r.db.
		NewDelete().
		Model(model).
		Where("id = ?", id).
		Exec(ctx)

	if err != nil {
		return &eh.RepoError{
			Err:      fmt.Errorf("could not remove: %w", err),
			Op:       eh.RepoOpRemove,
			EntityID: id,
		}
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return &eh.RepoError{
			Err:      fmt.Errorf("could not remove: %w", err),
			Op:       eh.RepoOpRemove,
			EntityID: id,
		}
	}
	if rowsAffected != 1 {
		return &eh.RepoError{
			Err:      eh.ErrEntityNotFound,
			Op:       eh.RepoOpRemove,
			EntityID: id,
		}
	}

	return nil
}

func (r Repo) Close() error {
	return r.db.Close()
}

func (r Repo) doCreateTable(ctx context.Context) error {
	model := r.model

	if _, err := r.db.
		NewCreateTable().
		Model(model).
		IfNotExists().
		Exec(ctx); err != nil {
		return fmt.Errorf("could not create model table: %w", err)
	}

	return nil
}
func (r Repo) doTruncateTable(ctx context.Context) error {
	model := r.model

	if _, err := r.db.
		NewTruncateTable().
		Model(model).
		Exec(ctx); err != nil {
		return fmt.Errorf("could not truncate model table: %w", err)
	}

	return nil
}
