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
	UdtName                string         `field:"udt_name"`
	CharacterMaximumLength sql.NullInt64  `field:"character_maximum_length"`
	NumericPercision       sql.NullInt64  `field:"numeric_precision"`
	DateTimePrecision      sql.NullString `field:"datetime_precision"`
	IsNullable             string         `field:"is_nullable"`
	ColumnDefault          sql.NullString `field:"column_default"`
	ConstraintType         string         `field:"constraint_type"`
	Reference              Reference
}

type TableMetadata struct {
	Name    string
	Columns map[string]ColumnMetadata
}
