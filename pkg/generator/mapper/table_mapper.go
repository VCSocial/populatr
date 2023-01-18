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
	Query        string
	Dependencies []*InsertableData
	Valid        bool
}

type ValueData struct {
	Name  string
	Value any
}

func MapTable(table info.Table, visited map[string]InsertableData) {
	_, ok := visited[table.Name]
	logging.Global.Debug().
		Str("table_name", table.Name).
		Bool("already_processed", ok).
		Msg("mapping table")
	if ok {
		return
	}
	parameters := []string{}
	placeholders := []string{}
	rows := []map[string]any{}
	dependencies := []*InsertableData{}

	for _, parent := range table.Parents {
		if parent.Name == table.Name {
			continue
		}

		MapTable(*parent, visited)
		dep, ok := visited[parent.Name]
		if ok {
			dependencies = append(dependencies, &dep)
		}
	}

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
			if c.References != nil {
				if c.References.TableName == c.TableName && c.IsNullable == "YES" {
					val = sql.NullString{String: "", Valid: false}
				} else {
					val = visited[c.References.TableName].Values[i][c.References.Name]
					logging.Global.Debug().
						Str("table_name", c.TableName).
						Str("parent_table_name", c.References.TableName).
						Msg("added referenced table")
				}

			} else {
				v, err := Convert(c)
				if err != nil {
					continue
				}
				val = v
			}
			values[c.Name] = val
		}
		rows = append(rows, values)
	}
	data := InsertableData{
		TableName:    table.Name,
		Parameters:   parameters,
		Placeholders: "(" + strings.Join(placeholders, ",") + ")",
		Values:       rows,
		Rows:         defaultRows,
		Dependencies: dependencies,
		Valid:        true,
	}
	visited[table.Name] = data
}

func MapAllTables(tables []info.Table) map[string]InsertableData {
	visited := make(map[string]InsertableData)
	for _, table := range tables {
		MapTable(table, visited)
	}
	for _, v := range visited {
		v.Valid = false
		visited[v.TableName] = v
	}
	return visited
}
