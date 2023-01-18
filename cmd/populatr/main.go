package main

import (
	"database/sql"
	"flag"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	e "github.com/vcsocial/populatr/pkg/common/err"
	logging "github.com/vcsocial/populatr/pkg/common/logging"
	gen "github.com/vcsocial/populatr/pkg/generator"
	"github.com/vcsocial/populatr/pkg/mapper"
)

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
		logging.Opts.Level = zerolog.DebugLevel
		log.Debug().Str("Connection", dbConn)
	}
	logging.InitLogger()

	db, err := sql.Open("postgres", dbConn)
	e.CheckPanic(err, "Could not connect to database")
	defer db.Close()

	tblIscMap := mapper.MapInfoSchemaColumns(db)

	for tblName, tblIscs := range tblIscMap {
		ent := gen.Entity{}
		ent.GenerateRecords(tblIscs)

		query, values, err := ent.GetInsertQuery()
		e.CheckPanic(err, "Could not generate data for "+tblName)
		logging.Global.Debug().Str("Table", tblName).Str("Query", query)

		stmt, err := db.Prepare(query)
		e.CheckPanic(err, "Could not prepare statement")

		for _, v := range values {
			logging.Global.Debug().Msgf("Inserting values %+v", v)

			res, err := stmt.Exec(v...)
			if err != nil {
				logging.Global.
					Warn().
					Err(err).
					Msgf("Unexpected error while inserting data! Values: %+v", v)
				continue
			}

			rowNum, err := res.RowsAffected()
			e.CheckPanic(err, "Could not extract rows affected!")

			logging.Global.
				Debug().
				Int64("rows", rowNum).
				Str("table_name", tblName)
		}
	}
}
