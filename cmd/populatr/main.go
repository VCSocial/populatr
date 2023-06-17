package main

import (
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"populatr/internal/app"
	"populatr/internal/datasource"
	"populatr/internal/infra"
	"populatr/internal/logger"
	"populatr/internal/pg"
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
	genLimit   *int
	lgr        zerolog.Logger
)

func init() {
	dbTypePtr = flag.String("type", datasource.DialectPG, "Database Type")
	hostPtr = flag.String("host", "localhost", "Host")
	dbPtr = flag.String("D", "pop", "Database")
	portPtr = flag.Int("P", 5432, "Port")
	userPtr = flag.String("u", "puser", "Username")
	passPtr = flag.String("p", "password", "Password (Do not use with production DBs!)")
	verbosePtr = flag.Bool("v", true, "Enable verbose output")
	sslModePtr = flag.Bool("s", false, "Enable sslmode")
	genLimit = flag.Int("n", 100, "Number of rows to generate")
}

func parseArgs() *datasource.Connection {
	flag.Parse()

	if *verbosePtr {
		logger.EnableVerbose()
	}
	lgr = logger.Get()
	return datasource.NewConnection(
		datasource.WithDialect(*dbTypePtr),
		datasource.WithHost(*hostPtr),
		datasource.WithUsername(*userPtr),
		datasource.WithPassword(*passPtr),
		datasource.WithPort(*portPtr),
		datasource.WithDb(*dbPtr),
		datasource.WithSsl(*sslModePtr),
	)
}

func main() {
	conn := parseArgs()
	mgr := pg.NewManager(*conn)
	db, err := infa.NewDb(mgr)
	if err != nil {
		lgr.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer func(db *sqlx.DB) {
		err := db.Close()
		if err != nil {
			lgr.Fatal().Err(err).Msg("failed properly close database connection")
		}
	}(db)

	cliApp := app.NewApp(*genLimit, db)
	err = cliApp.Generate()
	if err != nil {
		lgr.Fatal().Err(err).Msg("failed to generate test data")
	}
}
