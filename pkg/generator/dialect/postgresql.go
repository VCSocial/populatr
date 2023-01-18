package dialect

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	e "github.com/vcsocial/populatr/pkg/common/err"
	info "github.com/vcsocial/populatr/pkg/generator/info"
	"github.com/vcsocial/populatr/pkg/generator/mapper"
)

const findAllTables = `
SELECT
	table_name
FROM information_schema.tables
WHERE table_schema != 'information_schema'
	AND table_schema != 'pg_catalog'
    AND table_type = 'BASE TABLE'
ORDER BY table_name
`

const findAllReferencedTableColumns = `
SELECT
    kcu.column_name as child_column,
    tc2.table_name as parent_table,
    kcu2.column_name as parent_column
FROM information_schema.table_constraints tc
JOIN information_schema.referential_constraints rc
	ON tc.constraint_name = rc.constraint_name
    AND tc.constraint_schema = rc.constraint_schema
    AND tc.constraint_catalog = rc.constraint_catalog
JOIN information_schema.table_constraints tc2
	ON rc.unique_constraint_name = tc2.constraint_name
    AND rc.constraint_schema = tc2.constraint_schema
    AND rc.constraint_catalog = tc2.constraint_catalog
JOIN information_schema.key_column_usage kcu
	ON tc.constraint_name = kcu.constraint_name
    AND tc.constraint_schema = kcu.constraint_schema
    AND tc.constraint_catalog = kcu.constraint_catalog
JOIN information_schema.key_column_usage kcu2
	ON tc2.constraint_name = kcu2.constraint_name
    AND tc2.constraint_schema = kcu2.constraint_schema
    AND tc2.constraint_catalog = kcu2.constraint_catalog
WHERE tc.table_name = $1
	AND tc.constraint_type = 'FOREIGN KEY'
    AND tc2.constraint_type = 'PRIMARY KEY'
`

const findAllTableParents = `
SELECT
    tc2.table_name as parentTable
FROM information_schema.table_constraints tc
JOIN information_schema.referential_constraints rc
	ON tc.constraint_name = rc.constraint_name
JOIN information_schema.table_constraints tc2
	ON rc.unique_constraint_name = tc2.constraint_name
WHERE tc.table_name = $1
	AND tc.constraint_type = 'FOREIGN KEY'
    AND tc2.constraint_type = 'PRIMARY KEY'
`

const findAllColumns = `
SELECT
	c.column_name as column_name,
    c.data_type as data_type,
    c.character_maximum_length as character_maximum_legnth,
    c.numeric_precision as numeric_precision,
	c.datetime_precision as datetime_precision,
    c.udt_name as udt_name,
    c.is_nullable as is_nullable,
    c.is_generated as is_generated,
    c.is_updatable as is_updatable,
	c.column_default as column_default,
	c.table_name as table_name,
    CASE
		WHEN kt.constraint_type IS NOT NULL
        	THEN kt.constraint_type
        ELSE 'BASIC KEY'
    END as constraint_type
FROM information_schema.columns c
LEFT JOIN (
  		SELECT
  			tc.constraint_type,
  			tc.table_name,
  			kcu.column_name
  		FROM information_schema.table_constraints tc
  		JOIN information_schema.key_column_usage kcu
  			ON tc.constraint_name = kcu.constraint_name
	) kt ON c.column_name = kt.column_name
		AND c.table_name = kt.table_name
WHERE c.table_name = $1;
`

const insertQueryTemplate = "INSERT INTO %s (%s) VALUES %s"

type PostgresqlRepo struct{}

func (pg PostgresqlRepo) FindAllColumns(db *sql.DB) []info.Table {
	tableRows, err := db.Query(findAllTables)
	e.CheckPanic(err, "failed to retrieve tables")

	nameTableMap := make(map[string]info.Table)
	for tableRows.Next() {
		var table info.Table
		err = tableRows.Scan(&table.Name)
		fmt.Printf(">> Found table %s\n", table.Name)
		e.CheckPanic(err, "unable to scan table name")

		colRows, err := db.Query(findAllColumns, table.Name)
		e.CheckPanic(err, "failed to retrieve columns of "+table.Name)

		table.Columns = make(map[string]info.Column)
		for colRows.Next() {
			var col info.Column
			err = colRows.Scan(&col.Name, &col.DataType,
				&col.CharacterMaximumLength, &col.NumericPercision,
				&col.DateTimePrecision, &col.UdtName, &col.IsNullable,
				&col.IsGenerated, &col.IsUpdatable, &col.ColumnDefault,
				&col.TableName, &col.ConstraintType,
			)
			table.Columns[col.Name] = col
		}
		nameTableMap[table.Name] = table
	}

	for name, table := range nameTableMap {
		//parentRows, err := db.Query(findAllTableParents, name)
		//e.CheckPanic(err, "failed to query table children")

		//for parentRows.Next() {
		//	var parentName string
		//	err = parentRows.Scan(&parentName)
		//	e.CheckPanic(err, "unable to scan child table name")

		//	parentTable := nameTableMap[parentName]
		//	table.Parents = append(table.Parents, &parentTable)
		//}

		referencedColumns, err := db.Query(findAllReferencedTableColumns, name)
		e.CheckPanic(err, "failed to query table parents")

		for referencedColumns.Next() {
			var childColName string
			var parentTblName string
			var parentColName string
			err = referencedColumns.Scan(&childColName, &parentTblName, &parentColName)
			e.CheckPanic(err, "unable to scan referenced columns")
			parent, ok := nameTableMap[parentTblName]
			if ok {
				table.Parents = append(table.Parents, &parent)
				parentCol, okParent := parent.Columns[parentColName]
				childCol, okChild := table.Columns[childColName]
				if okParent && okChild {
					childCol.References = &parentCol
					table.Columns[childColName] = childCol
				}
				nameTableMap[name] = table
			}
		}
	}

	var allTables []info.Table
	for _, table := range nameTableMap {
		updatedParents := []*info.Table{}
		for _, parent := range table.Parents {
			updated, ok := nameTableMap[parent.Name]
			if ok {
				updatedParents = append(updatedParents, &updated)
			}
		}
		table.Parents = updatedParents
		allTables = append(allTables, table)
	}

	return allTables
}

func InsertData(db *sql.DB, d mapper.InsertableData, visited map[string]mapper.InsertableData) {
	if visited[d.TableName].Valid {
		return
	}

	for _, dep := range d.Dependencies {
		InsertData(db, *dep, visited)
	}

	escapedParams := []string{}
	for _, p := range d.Parameters {
		escapedParams = append(escapedParams, pq.QuoteIdentifier(p))
	}

	paramsStr := strings.Join(escapedParams, ",")
	d.Query = fmt.Sprintf(insertQueryTemplate, pq.QuoteIdentifier(d.TableName), paramsStr, d.Placeholders)

	// TODO switch to bulk insert
	for _, colMap := range d.Values {
		vals := []any{}
		for _, param := range d.Parameters {
			v, ok := colMap[param]
			if ok {
				vals = append(vals, v)
			}
		}
		stmt, err := db.Prepare(d.Query)
		e.CheckPanic(err, "failed to prepare insert statement")

		_, err = stmt.Exec(vals...)
		if err != nil {
			fmt.Println(d.Query)
		}
		e.CheckPanic(err, "failed to execute insert statement on "+d.TableName)
	}
	d.Valid = true
	visited[d.TableName] = d
}

func (pg PostgresqlRepo) InsertTestData(db *sql.DB, tables []info.Table) {
	data := mapper.MapAllTables(tables)
	for _, d := range data {
		InsertData(db, d, data)
	}
}
