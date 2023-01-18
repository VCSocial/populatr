package info

import "database/sql"

const (
	PrimaryKey = "PRIMARY KEY"
	ForeignKey = "FORIEGN KEY"
	BasicKey   = "BASIC KEY"
)

type Column struct {
	Name                   string         `field:"column_name"`
	DataType               string         `field:"data_type"`
	CharacterMaximumLength sql.NullInt64  `field:"character_maximum_length"`
	NumericPercision       sql.NullInt64  `field:"numeric_precision"`
	DateTimePrecision      sql.NullString `field:"datetime_precision"`
	UdtName                string         `field:"udt_name"`
	IsNullable             string         `field:"is_nullable"`
	IsGenerated            string         `field:"is_generated"`
	IsUpdatable            string         `field:"is_updatable"`
	ColumnDefault          sql.NullString `field:"column_default"`
	ConstraintType         string         `field:"constraint_type"`
	TableName              string         `field:"table_name"`
	References             *Column
}
