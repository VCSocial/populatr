package app

import (
	"github.com/jmoiron/sqlx"
	"populatr/internal/domain"
	"populatr/internal/pg"
)

type app struct {
	genLimit       int
	infoSchemaRepo domain.InfoSchemaRepo
}

func NewApp(genLimit int, db *sqlx.DB) *app {
	return &app{
		genLimit:       genLimit,
		infoSchemaRepo: pg.NewInfoSchemaRepo(db),
	}
}
