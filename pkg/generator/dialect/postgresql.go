package dialect

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/info"
	"github.com/vcsocial/populatr/pkg/generator/mapper"
)

type PostgresqlRepo struct{}

func (pg PostgresqlRepo) FindAllColumns(db *sql.DB, tblName string,
	fkColName sql.NullString) map[string]info.ColumnMetadata {
	query, err := GetColumnsQuery(PG)
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
		err = colRows.Scan(&col.Name, &col.UdtName,
			&col.CharacterMaximumLength, &col.NumericPercision,
			&col.DateTimePrecision, &col.IsNullable,
			&col.ColumnDefault, &col.ConstraintType)

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

func (pg PostgresqlRepo) FindAllTables(db *sql.DB) []info.TableMetadata {
	query, err := GetTableRelationQuery(PG)
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

		if !graph.Exists(childTblName.String) {
			childCols := pg.FindAllColumns(db, childTblName.String,
				childColName)
			graph.AddNode(childTblName.String, childCols)
		}

		if parentTblName.Valid {
			if !graph.Exists(parentTblName.String) {
				parentCols := pg.FindAllColumns(db, parentTblName.String,
					sql.NullString{String: "", Valid: false})
				graph.AddNode(parentTblName.String, parentCols)
			}
			if parentTblName.String != childTblName.String {
				graph.AddEdge(parentTblName.String, childTblName.String)
			}
		}

		if parentTblName.Valid && parentColName.Valid {
			ref := info.Reference{
				TableName:  parentTblName.String,
				ColumnName: parentColName.String,
				Valid:      true,
			}
			graph.AddRef(childTblName.String, childColName.String, ref)
		}
	}
	return graph.topologicalSort()
}

func (pg PostgresqlRepo) insertTestData(db *sql.DB, d mapper.InsertableData) {
	logging.Global.Debug().
		Str("table_name", d.TableName).
		Msg("processing test data")

	escapedParams := []string{}
	for _, p := range d.Parameters {
		escapedParams = append(escapedParams, pq.QuoteIdentifier(p))
	}

	paramsStr := strings.Join(escapedParams, ",")
	query := fmt.Sprintf(insertQueryTemplate, pq.QuoteIdentifier(d.TableName),
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
			fmt.Println(query)
		}
	}
}

func (pg PostgresqlRepo) InsertAllTestData(db *sql.DB,
	tables []info.TableMetadata) {
	data := mapper.MapAllTables(tables)
	for _, table := range tables {
		if d, ok := data[table.Name]; ok {
			pg.insertTestData(db, d)
		}
	}
}
