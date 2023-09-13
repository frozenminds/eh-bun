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
	ehbun "github.com/frozenminds/eh-bun"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/mocks"
	"github.com/looplab/eventhorizon/repo"
	"github.com/looplab/eventhorizon/uuid"
	"github.com/uptrace/bun"
	"testing"
	"time"
)

type model struct {
	bun.BaseModel `bun:"model,alias:m"`
	ID            uuid.UUID `json:"id"         bson:"_id"        bun:"id,type:uuid,pk"`
	Version       int       `json:"version"    bson:"version"    bun:"version"`
	Content       string    `json:"content"    bson:"content"    bun:"content"`
	CreatedAt     time.Time `json:"created_at" bson:"created_at" bun:"created_at"`
}

func (m model) EntityID() uuid.UUID {
	return m.ID
}

func TestReadRepoIntegration(t *testing.T) {
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

	dbModel := new(model)
	mapDbEntityToEhEntity := func(m interface{}) eh.Entity {
		model := m.(*model)
		return &mocks.Model{
			ID:        model.ID,
			Version:   model.Version,
			Content:   model.Content,
			CreatedAt: model.CreatedAt.UTC(),
		}
	}
	mapEhEntityToDbEntity := func(e interface{}) interface{} {
		entity := e.(*mocks.Model)
		return &model{
			ID:        entity.ID,
			Version:   entity.Version,
			Content:   entity.Content,
			CreatedAt: entity.CreatedAt.UTC(),
		}
	}

	r, err := NewRepo(
		*db,
		dbModel,
		mapDbEntityToEhEntity,
		mapEhEntityToDbEntity,
		WithTableReset(),
	)
	if err != nil {
		t.Fatal("could not create a new repository:", err)
	}
	if r == nil {
		t.Fatal("there should be a repository")
	}

	defer r.Close()

	if r.InnerRepo(context.Background()) != nil {
		t.Error("the inner repo should be nil")
	}

	repo.AcceptanceTest(t, r, context.Background())

	if err := r.Close(); err != nil {
		t.Error("there should be no error:", err)
	}
}
