package infa

import (
	"github.com/jmoiron/sqlx"
	"populatr/internal/datasource"
)

func NewDb(mgr datasource.ConnectionManager) (*sqlx.DB, error) {
	db, err := mgr.Connect()
	if err != nil {
		return db, err
	}

	db.DB.SetMaxIdleConns(16)
	db.DB.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(0)
	return db, nil
}
