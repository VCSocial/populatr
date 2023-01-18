package main

import (
	"database/sql"
	"flag"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

var logger zerolog.Logger

func init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
}

func checkErrPanic(err error, msg string) {
	if err != nil {
		log.Fatal().Err(err).Msg(msg)
	}
}

func main() {
	hostPtr := flag.String("host", "localhost", "Host")
	dbPtr := flag.String("D", "", "Database")
	portPtr := flag.Int("P", 0, "Port")
	userPtr := flag.String("u", "", "Username")
	passPtr := flag.String("p", "", "Password (Do not use with production DBs!)")
	verbosePtr := flag.Bool("v", false, "Enable verbose output")
	sslModePtr := flag.Bool("s", false, "Enable sslmode")
	flag.Parse()

	dbConn := "host=%s port=%d user=%s password=%s dbname=%s"
	if !*sslModePtr {
		dbConn += " sslmode=disable"
	}
	dbConn = fmt.Sprintf(dbConn, *hostPtr, *portPtr, *userPtr, *passPtr, *dbPtr)

	if *verbosePtr {
		log.Debug().Str("%s", dbConn)
	}

	db, err := sql.Open("postgres", dbConn)
	checkErrPanic(err, "Could not connect to database")
	defer db.Close()

	rows, err := db.Query(getAllColumns)
	checkErrPanic(err, "Query to retrieve columns in database failed")

	tblIscMap := make(map[string][]gen.InfoSchemaCol)
	for rows.Next() {
		var isc gen.InfoSchemaCol
		err = rows.Scan(&isc.TableName, &isc.ColumnName, &isc.DataType, &isc.OrdinalPosition, &isc.ColumnDefault,
			&isc.CharacterMaximumLength, &isc.NumericPercision, &isc.DateTimePrecision, &isc.UdtName,
			&isc.IsNullable, &isc.IsGenerated, &isc.IsUpdatable,
		)
		checkErrPanic(err, "Could not map SQL result to InfoSchemaCol")

		tbl, ok := tblIscMap[isc.TableName]
		if ok {
			tbl = append(tbl, isc)
		} else {
			tbl = []gen.InfoSchemaCol{isc}
		}
		tblIscMap[isc.TableName] = tbl
	}

	registry := gen.EntityRegistry{}
	registry.Initialize()

	for tblName, tblIscs := range tblIscMap {
		ent := gen.Entity{}
		ent.GenerateRecords(tblIscs)

		query, values, err := ent.GetInsertQuery()
		checkErrPanic(err, "Could not generate data for "+tblName)
		log.Info().Msgf("%s", query)

		stmt, err := db.Prepare(query)
		checkErrPanic(err, "Could prepare statement")

		for _, v := range values {
			log.Debug().Msgf("Inserting values %+v", v)

			res, err := stmt.Exec(v...)
			if err != nil {
				log.Warn().Err(err).Msgf("Unexpected error while inserting data! Values: %+v", v)
				continue
			}

			rowNum, err := res.RowsAffected()
			checkErrPanic(err, "Could not extract rows affected!")

			log.Debug().Int64("rows", rowNum).Str("table_name", tblName)
		}
	}
}
