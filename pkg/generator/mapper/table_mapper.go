package mapper

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/info"
)

const defaultRows = 10

type InsertableData struct {
	TableName    string
	Parameters   []string
	Placeholders string
	Values       []map[string]any
	Rows         int
}

func MapTable(table info.TableMetadata,
	processed map[string]InsertableData) InsertableData {
	parameters := []string{}
	placeholders := []string{}
	rows := []map[string]any{}

	for i := 0; i < defaultRows; i++ {
		values := make(map[string]any)
		j := 1
		for _, c := range table.Columns {
			if i == 0 {
				parameters = append(parameters, c.Name)
				placeholders = append(placeholders, fmt.Sprintf("$%d", j))
				j++
			}

			var val any
			if c.Reference.Valid {
				if c.Reference.TableName == table.Name &&
					c.IsNullable == "YES" {
					val = sql.NullString{String: "", Valid: false}
				} else {
					val = processed[c.Reference.TableName].
						Values[i][c.Reference.ColumnName]
					logging.Global.Debug().
						Str("table_name", table.Name).
						Str("parent_table_name", c.Reference.TableName).
						Msg("added referenced table")
				}

			} else {
				v, err := Convert(c)
				if err != nil {
					logging.Global.Error().
						Err(err).
						Str("table_name", table.Name).
						Str("column_name", c.Name).
						Msg("failed to convert")
					continue
				}
				val = v
			}
			values[c.Name] = val
		}
		rows = append(rows, values)
	}

	return InsertableData{
		TableName:    table.Name,
		Parameters:   parameters,
		Placeholders: "(" + strings.Join(placeholders, ",") + ")",
		Values:       rows,
		Rows:         defaultRows,
	}
}

func MapAllTables(tables []info.TableMetadata) map[string]InsertableData {
	processed := make(map[string]InsertableData)
	for _, table := range tables {
		processed[table.Name] = MapTable(table, processed)
	}
	return processed
}
