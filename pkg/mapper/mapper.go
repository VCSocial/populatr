package mapper

import (
	"database/sql"

	e "github.com/vcsocial/populatr/pkg/common/err"
	gen "github.com/vcsocial/populatr/pkg/generator"
)

const getAllColumns = `
SELECT
	table_name, column_name, data_type, ordinal_position, column_default,
	character_maximum_length, numeric_precision, datetime_precision, udt_name,
	is_nullable, is_generated, is_updatable
FROM information_schema.columns
WHERE table_schema != 'information_schema'
	AND table_schema != 'pg_catalog'
ORDER BY table_name`

func MapInfoSchemaColumns(db *sql.DB) map[string][]gen.InfoSchemaCol {
	rows, err := db.Query(getAllColumns)
	e.CheckPanic(err, "Query to retrieve columns in database failed")

	tblIscMap := make(map[string][]gen.InfoSchemaCol)
	for rows.Next() {
		var isc gen.InfoSchemaCol
		err = rows.Scan(&isc.TableName, &isc.ColumnName, &isc.DataType, &isc.OrdinalPosition, &isc.ColumnDefault,
			&isc.CharacterMaximumLength, &isc.NumericPercision, &isc.DateTimePrecision, &isc.UdtName,
			&isc.IsNullable, &isc.IsGenerated, &isc.IsUpdatable,
		)
		e.CheckPanic(err, "Could not map SQL result to InfoSchemaCol")

		tbl, ok := tblIscMap[isc.TableName]
		if ok {
			tbl = append(tbl, isc)
		} else {
			tbl = []gen.InfoSchemaCol{isc}
		}
		tblIscMap[isc.TableName] = tbl
	}
	return tblIscMap
}
