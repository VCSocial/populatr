package info

import "database/sql"

type InfoRepo interface {
	FindAllTables(db *sql.DB) []Table
	InsertTestData(db *sql.DB, tables []Table)
}
