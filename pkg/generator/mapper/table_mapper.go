package mapper

import (
	"database/sql"
	"fmt"
	"strings"

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
	if ok {
		fmt.Printf("Exiting early on %s\n", table.Name)
		return
	}
	parameters := []string{}
	placeholders := []string{}
	//rows := make(map[int][]any)
	rows := []map[string]any{}
	dependencies := []*InsertableData{}

	for _, parent := range table.Parents {
		if parent.Name == table.Name {
			continue
		}

		MapTable(*parent, visited)
		//fmt.Printf("child: %s, parent: %s \n", table.Name, parent.Name)
		dep, ok := visited[parent.Name]
		if ok {
			dependencies = append(dependencies, &dep)
		}
	}

	for i := 0; i < defaultRows; i++ {
		//values := []any{}
		values := make(map[string]any)
		j := 1
		for _, c := range table.Columns {
			if i == 0 {
				parameters = append(parameters, c.Name)
				placeholders = append(placeholders, fmt.Sprintf("$%d", j))
				j++
			}
			//if c.ConstraintType == info.PrimaryKey && c.ColumnDefault.Valid {
			//	continue
			//}

			var val any
			if c.References != nil {
				if c.References.TableName == c.TableName && c.IsNullable == "YES" {
					val = sql.NullString{String: "", Valid: false}
				} else {
					fmt.Printf("Adding to %s from %s\n", c.TableName, c.References.TableName)
					val = visited[c.References.TableName].Values[i][c.References.Name]
				}

			} else {
				v, err := Convert(c)
				if err != nil {
					continue
				}
				val = v
			}
			//values = append(values, val)
			values[c.Name] = val
		}
		//rows[i] = values
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
		fmt.Printf("Target: %s\n", table.Name)
		MapTable(table, visited)
	}
	for _, v := range visited {
		v.Valid = false
		visited[v.TableName] = v
	}
	return visited
}
