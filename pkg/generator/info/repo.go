package info

import "database/sql"

type InfoRepo interface {
	FindAllColumns(db *sql.DB) []Table
	InsertTestData(db *sql.DB, tables []Table)
}
