package repo

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/dialect"
	"github.com/vcsocial/populatr/pkg/generator/info"
	"github.com/vcsocial/populatr/pkg/generator/mapper"
)

func FindAllColumns(db *sql.DB, tblName string) map[string]info.ColumnMetadata {
	query, err := dialect.GetColumnsQuery()
	if err != nil {
		logging.Global.Error().
			Err(err).
			Msg("could not get column query for postgresql")
	}

	colRows, err := db.Query(query, tblName)
	if err != nil {
		logging.Global.Error().
			Err(err).
			Str("table_name", tblName).
			Msg("could not retrieve columns for table")
	}

	cols := make(map[string]info.ColumnMetadata)
	for colRows.Next() {
		var col info.ColumnMetadata
		err = colRows.Scan(&col.Name, &col.DataType,
			&col.CharacterMaximumLength, &col.NumericPercision,
			&col.NumericScale, &col.DateTimePrecision,
			&col.IsNullable, &col.ConstraintType)

		if err != nil {
			logging.Global.Error().
				Err(err).
				Str("table_name", tblName).
				Msg("could not map column")
		}
		cols[col.Name] = col
	}
	return cols
}

func FindAllTables(db *sql.DB) []info.TableMetadata {
	query, err := dialect.GetTableRelationQuery()
	if err != nil {
		logging.Global.Error().
			Err(err).
			Msg("could not get column query for postgresql")
	}

	tableRows, err := db.Query(query)
	if err != nil {
		logging.Global.Err(err).
			Msg("failed to retrieve table associations")
	}

	graph := newTableGraph()
	for tableRows.Next() {
		var parentTblName sql.NullString
		var parentColName sql.NullString
		var childTblName sql.NullString
		var childColName sql.NullString
		err = tableRows.Scan(&parentTblName, &parentColName, &childTblName,
			&childColName)
		if err != nil {
			logging.Global.Error().
				Err(err).
				Msg("could not map references")
		}

		if !childTblName.Valid && !parentTblName.Valid {
			logging.Global.Error().
				Msg("could not extract parent or child table")
			continue
		}

		if !graph.exists(childTblName.String) {
			childCols := FindAllColumns(db, childTblName.String)
			graph.addNode(childTblName.String, childCols)
		}

		if parentTblName.Valid {
			if !graph.exists(parentTblName.String) {
				parentCols := FindAllColumns(db, parentTblName.String)
				graph.addNode(parentTblName.String, parentCols)
			}
			if parentTblName.String != childTblName.String {
				graph.addEdge(parentTblName.String, childTblName.String)
			}
		}

		if parentTblName.Valid && parentColName.Valid {
			ref := info.Reference{
				TableName:  parentTblName.String,
				ColumnName: parentColName.String,
				Valid:      true,
			}
			graph.addRef(childTblName.String, childColName.String, ref)
		}
	}
	return graph.topologicalSort()
}

func insertTestData(db *sql.DB, d mapper.InsertableData) {
	logging.Global.Debug().
		Str("table_name", d.TableName).
		Msg("processing test data")

	escapedParams := []string{}
	for _, p := range d.Parameters {
		escapedParams = append(escapedParams, dialect.QuoteIdentifer(p))
	}

	paramsStr := strings.Join(escapedParams, ",")
	template, err := dialect.GetInsertQueryTemplate()
	if err != nil {
		logging.Global.Error().
			Err(err).
			Msg("could not get insert query template")
	}
	query := fmt.Sprintf(template, dialect.QuoteIdentifer(d.TableName),
		paramsStr, d.Placeholders)

	// TODO switch to bulk insert
	for _, colMap := range d.Values {
		vals := []any{}
		for _, param := range d.Parameters {
			v, ok := colMap[param]
			if ok {
				vals = append(vals, v)
			}
		}
		stmt, err := db.Prepare(query)
		if err != nil {
			logging.Global.Error().
				Err(err).
				Str("table_name", d.TableName).
				Msg("failed to prepare insert statement")
		}

		_, err = stmt.Exec(vals...)
		if err != nil {
			logging.Global.Error().
				Err(err).
				Str("table_name", d.TableName).
				Msg("failed to execute insert statement")
		}
	}
}

func InsertAllTestData(db *sql.DB, tables []info.TableMetadata) {
	data := mapper.MapAllTables(tables)
	for _, table := range tables {
		if d, ok := data[table.Name]; ok {
			insertTestData(db, d)
		}
	}
}
