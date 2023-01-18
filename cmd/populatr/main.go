package main

import (
	"flag"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	logging "github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/dialect"
)

var (
	dbTypePtr  *string
	hostPtr    *string
	dbPtr      *string
	portPtr    *int
	userPtr    *string
	passPtr    *string
	verbosePtr *bool
	sslModePtr *bool
)

func init() {
	dbTypePtr = flag.String("type", dialect.PG, "Database Type")
	hostPtr = flag.String("host", "localhost", "Host")
	dbPtr = flag.String("D", "", "Database")
	portPtr = flag.Int("P", 0, "Port")
	userPtr = flag.String("u", "", "Username")
	passPtr = flag.String("p", "", "Password (Do not use with production DBs!)")
	verbosePtr = flag.Bool("v", false, "Enable verbose output")
	sslModePtr = flag.Bool("s", false, "Enable sslmode")
}

func main() {
	flag.Parse()

	dbConn := "host=%s port=%d user=%s password=%s dbname=%s"
	if !*sslModePtr {
		dbConn += " sslmode=disable"
	}

	dbConn = fmt.Sprintf(dbConn, *hostPtr, *portPtr, *userPtr, *passPtr, *dbPtr)

	if *verbosePtr {
		logging.Opts.Level = zerolog.DebugLevel
	}
	logging.InitLogger()
	logging.Global.Debug().Str("Connection", dbConn)

	db := dialect.Connect(dbConn, *dbTypePtr)
	defer db.Close()

	dao := dialect.GetDao(*dbTypePtr)
	tables := dao.FindAllColumns(db)
	dao.InsertTestData(db, tables)

}
