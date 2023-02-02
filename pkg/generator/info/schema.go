package info

import "database/sql"

const (
	PrimaryKey = "PRIMARY KEY"
	ForeignKey = "FORIEGN KEY"
	BasicKey   = "BASIC KEY"
)

type Reference struct {
	TableName  string
	ColumnName string
	Valid      bool
}

type ColumnMetadata struct {
	Name                   string         `field:"column_name"`
	DataType               string         `field:"data_type"`
	CharacterMaximumLength sql.NullInt64  `field:"character_maximum_length"`
	NumericPercision       sql.NullInt64  `field:"numeric_precision"`
	NumericScale           sql.NullInt64  `field:"numeric_scale"`
	DateTimePrecision      sql.NullString `field:"datetime_precision"`
	IsNullable             string         `field:"is_nullable"`
	ConstraintType         string         `field:"constraint_type"`
	Reference              Reference
}

type TableMetadata struct {
	Name    string
	Columns map[string]ColumnMetadata
}
