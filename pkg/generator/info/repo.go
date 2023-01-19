package info

import (
	"database/sql"
)

type InfoRepo interface {
	FindAllColumns(db *sql.DB, tblName string,
		fkColName sql.NullString) map[string]ColumnMetadata
	FindAllTables(db *sql.DB) []TableMetadata
	InsertAllTestData(db *sql.DB, tables []TableMetadata)
}
