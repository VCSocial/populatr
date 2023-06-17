package domain

import (
	"context"
	"database/sql"
)

type ColumnMetadata struct {
	TableName          sql.NullString `db:"table_name"`
	ColumnName         sql.NullString `db:"column_name"`
	DataType           sql.NullString `db:"data_type"`
	CharacterMaxLength sql.NullInt32  `db:"character_maximum_length"`
	NumericPrecision   sql.NullInt32  `db:"numeric_precision"`
	NumericScale       sql.NullInt32  `db:"numeric_scale"`
	DatetimePrecision  sql.NullInt32  `db:"datetime_precision"`
	IsNullable         sql.NullString `db:"is_nullable"`
	ConstraintType     sql.NullString `db:"constraint_type"`
	ForeignTable       sql.NullString `db:"foreign_table"`
	ForeignColumn      sql.NullString `db:"foreign_column"`
	OrdinalPosition    sql.NullString `db:"ordinal_position"`
}

type InfoSchemaRepo interface {
	FindAllMetadata(ctx context.Context) ([]ColumnMetadata, error)
	CreateTestData(ctx context.Context, tbls *[]string, ids *[][]string, vals *[][][]any) error
}
