package main

import (
	"flag"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/vcsocial/populatr/pkg/common/logging"
	"github.com/vcsocial/populatr/pkg/generator/dialect"
	"github.com/vcsocial/populatr/pkg/generator/repo"
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
	dialect.Opts.Configure(*dbTypePtr, *userPtr, *passPtr, *hostPtr, *portPtr,
		*dbPtr, *sslModePtr)

	if *verbosePtr {
		logging.Opts.Level = zerolog.DebugLevel
	}
	logging.InitLogger()

	db, err := dialect.Connect(*dbTypePtr)
	if err != nil {
		logging.Global.Fatal().
			Err(err).
			Msg("could not connect to db")
	}
	defer db.Close()

	tables := repo.FindAllTables(db)
	repo.InsertAllTestData(db, tables)
}
